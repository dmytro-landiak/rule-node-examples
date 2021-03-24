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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.TbRelationTypes;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix inactivity alarm",
        configClazz = TbVixInactivityAlarmNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeVixInactivityAlarmConfig")
public class TbVixInactivityAlarmNode implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String ACTIVITY_STATE = "active";
    private static final String LAST_ACTIVITY_TIME = "lastActivityTime";

    private static final int ENTITIES_LIMIT = 1000;
    private static final List<String> PERSISTENT_ATTRIBUTES = Arrays.asList(ACTIVITY_STATE, LAST_ACTIVITY_TIME);

    private TbVixInactivityAlarmNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbVixInactivityAlarmNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        log.info("Started fixing devices alarms...");

        AtomicInteger atomicInteger = new AtomicInteger(0);
        List<Device> devices = getDevices(ctx);
        long currTs = System.currentTimeMillis();

        log.info("Found {} devices!", devices.size());
        if (!devices.isEmpty()) {
            List<ListenableFuture<Device>> futures = new ArrayList<>();
            for (Device device : devices) {
                ListenableFuture<List<AttributeKvEntry>> future = ctx.getAttributesService()
                        .find(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, PERSISTENT_ATTRIBUTES);
                futures.add(Futures.transform(future, attributes -> {
                    if (!CollectionUtils.isEmpty(attributes)) {
                        Boolean active = null;
                        Long lastActivityTime = null;
                        for (AttributeKvEntry attribute : attributes) {
                            if (attribute.getKey().equals(ACTIVITY_STATE)) {
                                if (attribute.getBooleanValue().isPresent()) {
                                    active = attribute.getBooleanValue().get();
                                }
                            } else if (attribute.getKey().equals(LAST_ACTIVITY_TIME)) {
                                if (attribute.getLongValue().isPresent()) {
                                    lastActivityTime = attribute.getLongValue().get();
                                }
                            }
                        }

                        if (active != null && lastActivityTime != null) {
                            if (!active && (currTs > (lastActivityTime + config.getCriticalInactivityTimeoutMs()))) {
                                atomicInteger.incrementAndGet();
                                return device;
                            }
                        }
                    }
                    return null;
                }, ctx.getDbCallbackExecutor()));
            }

            DonAsynchron.withCallback(Futures.successfulAsList(futures), filteredDevices -> {
                ctx.ack(msg);
                filteredDevices.stream().filter(Objects::nonNull).forEach(device -> pushRuleEngineMessage(ctx, device));
                log.info("Finished fixing devices alarms for {}!", atomicInteger.get());
            }, throwable -> ctx.tellFailure(msg, throwable));
        } else {
            log.warn("[{}] No Devices found!", ctx.getTenantId());
            ctx.tellFailure(msg, new RuntimeException("No Devices found!"));
        }
    }

    private List<Device> getDevices(TbContext ctx) {
        List<Device> devices = new ArrayList<>();
        PageLink pageLink = new PageLink(ENTITIES_LIMIT);
        while (pageLink != null) {
            PageData<Device> page = ctx.getDeviceService().findDevicesByTenantId(ctx.getTenantId(), pageLink);
            pageLink = page.hasNext() ? pageLink.nextPageLink() : null;
            devices.addAll(page.getData());
        }
        return devices;
    }

    private void pushRuleEngineMessage(TbContext ctx, Device device) {
        try {
            TbMsg tbMsg = TbMsg.newMsg("CRITICAL_INACTIVITY_EVENT", device.getId(), new TbMsgMetaData(),
                    TbMsgDataType.JSON, mapper.writeValueAsString(mapper.createObjectNode()));
            ctx.enqueueForTellNext(tbMsg, "HighPriority", TbRelationTypes.SUCCESS,
                    () -> {},
                    throwable -> ctx.tellFailure(tbMsg, throwable));
        } catch (Exception e) {
            log.warn("[{}] Failed to push inactivity alarm!", device.getId(), e);
        }
    }

    @Override
    public void destroy() {

    }
}
