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
package org.thingsboard.rule.engine.node.action;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix dart",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbVixDartNode implements TbNode {

    private static final int ENTITIES_LIMIT = 10000;

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        log.info("Started fixing devices information...");
        List<Device> devices = getDevices(ctx);
        log.info("Found {} devices!", devices.size());
        if (!devices.isEmpty()) {
            List<ListenableFuture<List<Void>>> futures = new ArrayList<>();
            for (Device device : devices) {
                ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService()
                        .findByToAndTypeAsync(ctx.getTenantId(), device.getId(), "FromInstallationPointToDevice", RelationTypeGroup.COMMON);
                futures.add(Futures.transformAsync(relationsFuture, relations -> {
                    if (!CollectionUtils.isEmpty(relations)) {
                        EntityId installationPointId = relations.get(0).getFrom();

                        ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService()
                                .find(ctx.getTenantId(), installationPointId, DataConstants.SERVER_SCOPE, Collections.singletonList("topologyLocation"));
                        return Futures.transformAsync(attributesFuture, attributes -> {
                            if (!CollectionUtils.isEmpty(attributes)) {
                                String ipTopologyLocation = attributes.get(0).getValueAsString();
                                if (ipTopologyLocation.contains("vehicles")) {
                                    ListenableFuture<Asset> assetFuture = ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), new AssetId(installationPointId.getId()));
                                    return Futures.transformAsync(assetFuture, asset -> {
                                        if (asset != null) {
                                            String ipName = asset.getName();

                                            ListenableFuture<List<AttributeKvEntry>> deviceAttributesFuture = ctx.getAttributesService()
                                                    .find(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, Collections.singletonList("installationPointHistory"));
                                            return Futures.transformAsync(deviceAttributesFuture, deviceAttributes -> {
                                                if (!CollectionUtils.isEmpty(deviceAttributes)) {
                                                    String installationPointHistoryStr = deviceAttributes.get(0).getValueAsString();

                                                    String deviceTopologyLocation = ipTopologyLocation + "/installationPoints/" + ipName;

                                                    JsonArray jsonArray = new JsonParser().parse(installationPointHistoryStr).getAsJsonArray();

                                                    long currTs = System.currentTimeMillis();

                                                    jsonArray.add(getJsonObject(ipName, deviceTopologyLocation, currTs));

                                                    List<AttributeKvEntry> attributesToSave = new ArrayList<>();
                                                    attributesToSave.add(new BaseAttributeKvEntry(new StringDataEntry("topologyLocation", deviceTopologyLocation), currTs));
                                                    attributesToSave.add(new BaseAttributeKvEntry(new JsonDataEntry("installationPointHistory", jsonArray.toString()), currTs));

                                                    return ctx.getAttributesService().save(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, attributesToSave);
                                                }
                                                return Futures.immediateFuture(null);
                                            }, ctx.getDbCallbackExecutor());
                                        }
                                        return Futures.immediateFuture(null);
                                    }, ctx.getDbCallbackExecutor());
                                }
                            }
                            return Futures.immediateFuture(null);
                        }, ctx.getDbCallbackExecutor());
                    }
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor()));
            }

            DonAsynchron.withCallback(Futures.successfulAsList(futures), lists -> {
                ctx.tellSuccess(msg);
                log.info("Finished fixing devices information!");
            }, throwable -> ctx.tellFailure(msg, throwable));
        } else {
            log.warn("[{}] No Devices found!", ctx.getTenantId());
            ctx.tellFailure(msg, new RuntimeException("No Devices found!"));
        }
    }

    @Override
    public void destroy() {

    }

    private JsonObject getJsonObject(String ipName, String deviceTopologyLocation, long currTs) {
        JsonObject jo = new JsonObject();
        jo.addProperty("installationId", ipName);
        jo.addProperty("topologyLocation", deviceTopologyLocation);
        jo.addProperty("timeAdded", currTs);
        return jo;
    }

    private List<Device> getDevices(TbContext ctx) {
        return ctx.getDeviceService().findDevicesByTenantId(ctx.getTenantId(), new PageLink(ENTITIES_LIMIT)).getData();
    }
}
