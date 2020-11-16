/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.analitycs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.device.DeviceSearchQuery;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.EntityIdFactory;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "prologis alarm",
        configClazz = TbPrologisAlarmNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisAnalyticsAlarmNodeConfig",
        icon = "functions"
)
public class TbPrologisAlarmNode implements TbNode {

    private TbPrologisAlarmNodeConfiguration config;
    private Gson gson;
    private Asset building;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbPrologisAlarmNodeConfiguration.class);
        gson = new Gson();
        EntityId originatorId = EntityIdFactory.getByTypeAndUuid(config.getOriginatorType(), config.getOriginatorId());
        building = ctx.getAssetService().findAssetById(ctx.getTenantId(), new AssetId(originatorId.getId()));
        if (building == null) {
            throw new RuntimeException("Failed to find building: " + originatorId);
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        ListenableFuture<List<Device>> deviceFutures = ctx.getDeviceService()
                .findDevicesByQuery(ctx.getTenantId(), buildQuery(building.getId()));
        ListenableFuture<List<Device>> filteredDeviceFuture = Futures.transformAsync(deviceFutures, devices -> {
            if (devices != null && !devices.isEmpty()) {
                List<Device> filteredDevice = new ArrayList<>();
                List<ListenableFuture<Void>> futures = new ArrayList<>();
                for (Device device : devices) {
                    if (config.getDeviceLabel().equals(device.getLabel())) {
                        ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService()
                                .find(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, Collections.singletonList(config.getModel()));
                        futures.add(Futures.transform(attributesFuture, attributeKvEntries -> {
                            if (attributeKvEntries != null && !attributeKvEntries.isEmpty()) {
                                if (attributeKvEntries.get(0).getValueAsString().equals(config.getModel())) {
                                    filteredDevice.add(device);
                                }
                            } else {
                                log.warn("Failed to find model attribute for a device: {}", device.getName());
                            }
                            return null;
                        }, ctx.getDbCallbackExecutor()));
                    } else {
                        log.info("Label of device with name " + device.getName() + "is not equal to the configuration label");
                    }
                }
                return Futures.transform(Futures.allAsList(futures), v -> filteredDevice, ctx.getDbCallbackExecutor());
            } else {
                log.info("No devices found related to building {}", building.getName());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        ListenableFuture<List<TbMsg>> resFuture = Futures.transformAsync(filteredDeviceFuture, devices -> {
            if (devices != null && !devices.isEmpty()) {
                List<ListenableFuture<TbMsg>> tbMsgFutures = new ArrayList<>();
                for (Device device : devices) {
                    ListenableFuture<List<TsKvEntry>> tsFutures = ctx.getTimeseriesService()
                            .findAll(ctx.getTenantId(), device.getId(),
                                    Collections.singletonList(new BaseReadTsKvQuery(config.getKeyName(),
                                            getStartTs(config.getCountOfDays()), System.currentTimeMillis(), 1000, "DESC")));
                    tbMsgFutures.add(Futures.transform(tsFutures, tsKvEntries -> {
                        TbMsg tbMsg = TbMsg.newMsg(msg.getQueueName(), msg.getType(), device.getId(),
                                msg.getMetaData(), msg.getData());
                        if (tsKvEntries != null && !tsKvEntries.isEmpty()) {
                            if (config.getMaxThresholdValue() == null
                                    && config.getMinThresholdValue() == null) {
                                tbMsg.getMetaData().putValue("alarm", "true");
                                tbMsg.getMetaData().putValue("description", "Not found time-series on this interval");
                            } else {
                                tbMsg.getMetaData().putValue("alarm", "false");
                            }
                        } else {
                            if (tsKvEntries.stream().anyMatch(this::compareWithMinAndMaxThreshold)) {
                                tbMsg.getMetaData().putValue("alarm", "true");
                            } else {
                                tbMsg.getMetaData().putValue("alarm", "false");
                            }
                        }
                        return tbMsg;
                    }, ctx.getDbCallbackExecutor()));
                }
                return Futures.allAsList(tbMsgFutures);
            } else {
                log.info("No devices found with label=" + config.getDeviceLabel() + " and model=" + config.getModel());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(resFuture, tbMsgs -> {
            ctx.ack(msg);
            if (tbMsgs != null) {
                tbMsgs.forEach(ctx::tellSuccess);
            }
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {
    }

    private boolean compareWithMinAndMaxThreshold(KvEntry kvEntry) {
        Optional<String> jsonValueOptional = kvEntry.getJsonValue();
        if (!jsonValueOptional.isPresent()) {
            return false;
        }
        int value = gson.fromJson(jsonValueOptional.get(), JsonObject.class).get("VALUE").getAsInt();
        if (config.getMinThresholdValue() == null && config.getMaxThresholdValue() == null) {
            return false;
        }
        if (config.getMinThresholdValue() != null && config.getMaxThresholdValue() != null) {
            return value > config.getMaxThresholdValue()
                    || value < config.getMinThresholdValue();
        } else if (config.getMinThresholdValue() != null) {
            return value < config.getMinThresholdValue();
        } else {
            return value > config.getMaxThresholdValue();
        }
    }

    private DeviceSearchQuery buildQuery(EntityId originator) {
        DeviceSearchQuery query = new DeviceSearchQuery();
        query.setParameters(new RelationsSearchParameters(originator, EntitySearchDirection.FROM, 2, false));
        query.setRelationType("IS_IN_SPACE");
        query.setDeviceTypes(Collections.singletonList("DATA_DEVICE"));
        return query;
    }

    private long getStartTs(int countDaysAgo) {
        return LocalDateTime.now().minusDays(countDaysAgo).atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
    }
}