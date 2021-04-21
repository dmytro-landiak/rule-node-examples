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
package org.thingsboard.rule.engine.vix;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
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
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.queue.ServiceQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "vix critical inactivity event",
        configClazz = TbVixCriticalInactivityEventNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbAnalyticsNodeVixCriticalInactivityConfig")
public class TbVixCriticalInactivityEventNode implements TbNode {

    private static final String TB_MSG_CUSTOM_NODE_MSG = "TbMsgCustomNodeMsg";

    private static final String ACTIVITY_STATE = "active";
    private static final String LAST_ACTIVITY_TIME = "lastActivityTime";
    private static final String INACTIVITY_ALARM_TIME = "inactivityAlarmTime";
    private static final String CRITICAL_INACTIVITY_ALARM_TIME = "criticalInactivityAlarmTime";

    private static final String CRITICAL_INACTIVITY_EVENT = "CRITICAL_INACTIVITY_EVENT";

    private static final List<String> ATTRIBUTES = Arrays.asList(ACTIVITY_STATE, LAST_ACTIVITY_TIME,
            INACTIVITY_ALARM_TIME, CRITICAL_INACTIVITY_ALARM_TIME);

    private static final int ENTITIES_LIMIT = 1000;

    private TbVixCriticalInactivityEventNodeConfiguration config;
    private ConcurrentMap<DeviceId, DeviceData> devicesDataMap;
    private long lastScheduledTs;
    private long delay;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbVixCriticalInactivityEventNodeConfiguration.class);
        this.delay = TimeUnit.SECONDS.toMillis(config.getExecutionPeriodInSec());
        try {
            devicesDataMap = getDeviceDataMap(ctx);
        } catch (ExecutionException | InterruptedException e) {
            log.error("[{}] Failed to initialize devicesDataMap for critical inactivity processing!", ctx.getTenantId(), e);
            throw new TbNodeException(e);
        }
        scheduleTickMsg(ctx);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (msg.getOriginator().getEntityType() != EntityType.DEVICE && !msg.getType().equals(TB_MSG_CUSTOM_NODE_MSG)) {
            ctx.ack(msg);
            return;
        }

        switch (msg.getType()) {
            case DataConstants.INACTIVITY_EVENT:
            case DataConstants.ACTIVITY_EVENT: {
                updateDeviceDataMap(ctx, msg);
                break;
            }
            case DataConstants.ENTITY_CREATED: {
                JsonObject msgData = getMsgData(msg);
                DeviceId deviceId = new DeviceId(msg.getOriginator().getId());
                DonAsynchron.withCallback(getState(ctx, deviceId), state -> {
                            devicesDataMap.computeIfAbsent(deviceId,
                                    n -> new DeviceData(msgData.get("name").getAsString(),
                                            msgData.get("type").getAsString(), state));
                            log.info("[{}][{}] Added device to critical inactivity map processing!", ctx.getTenantId(), deviceId);
                            ctx.ack(msg);
                        },
                        throwable -> ctx.tellFailure(msg, throwable));
                break;
            }
            case DataConstants.ENTITY_DELETED: {
                devicesDataMap.remove(new DeviceId(msg.getOriginator().getId()));
                log.info("[{}][{}] Removed device from critical inactivity map processing!", ctx.getTenantId(), msg.getOriginator().getId());
                ctx.ack(msg);
                break;
            }
            case TB_MSG_CUSTOM_NODE_MSG:
                long ts = System.currentTimeMillis();
                List<TbMsg> msgs = new ArrayList<>();
                for (Map.Entry<DeviceId, DeviceData> entry : devicesDataMap.entrySet()) {
                    State state = entry.getValue().getDeviceState();
                    if (state != null && !state.isActive() && (ts > (state.getLastActivityTime() + state.getCriticalInactivityTimeout()))) {
                        if (state.getLastCriticalInactivityAlarmTime() == 0L || state.getLastCriticalInactivityAlarmTime() < state.getLastActivityTime()) {
                            state.setLastCriticalInactivityAlarmTime(ts);
                            save(ctx, entry.getKey(), CRITICAL_INACTIVITY_ALARM_TIME, ts);

                            TbMsgMetaData md = new TbMsgMetaData();
                            md.putValue("deviceName", entry.getValue().getDeviceName());
                            md.putValue("deviceType", entry.getValue().getDeviceType());

                            TbMsg newMsg = null;
                            try {
                                newMsg = TbMsg.newMsg(CRITICAL_INACTIVITY_EVENT, entry.getKey(), md, TbMsgDataType.JSON,
                                        JacksonUtil.OBJECT_MAPPER.writeValueAsString(state));
                            } catch (JsonProcessingException e) {
                                log.warn("[{}] Failed to push critical inactivity event: {}", entry.getKey(), state, e);
                            }
                            msgs.add(newMsg);
                        }
                    }
                }
                ctx.ack(msg);
                for (TbMsg tempMsg : msgs) {
                    if (tempMsg != null) {
                        ctx.enqueueForTellNext(tempMsg, TbRelationTypes.SUCCESS);
                    }
                }
                scheduleTickMsg(ctx);
                break;
            default:
                ctx.tellSuccess(msg);
                break;
        }
    }

    private void updateDeviceDataMap(TbContext ctx, TbMsg msg) {
        JsonObject msgData = getMsgData(msg);
        DonAsynchron.withCallback(getOrFetchDeviceData(ctx, msg.getOriginator()),
                deviceData -> {
                    State state = deviceData.getDeviceState();
                    state.setActive(msgData.has("active") ? msgData.get("active").getAsBoolean() : state.isActive());
                    state.setLastActivityTime(msgData.has("lastActivityTime")
                            ? msgData.get("lastActivityTime").getAsLong()
                            : state.getLastActivityTime());
                    state.setLastInactivityAlarmTime(msgData.has("lastInactivityAlarmTime")
                            ? msgData.get("lastInactivityAlarmTime").getAsLong()
                            : state.getLastInactivityAlarmTime());
                    devicesDataMap.put(new DeviceId(msg.getOriginator().getId()), deviceData);
                    ctx.tellSuccess(msg);
                },
                throwable -> ctx.tellFailure(msg, throwable));
    }

    private JsonObject getMsgData(TbMsg msg) {
        return new JsonParser().parse(msg.getData()).getAsJsonObject();
    }

    @Override
    public void destroy() {

    }

    private ListenableFuture<DeviceData> getOrFetchDeviceData(TbContext ctx, EntityId entityId) {
        DeviceId deviceId = new DeviceId(entityId.getId());
        DeviceData deviceData = devicesDataMap.get(deviceId);
        if (deviceData != null) {
            return Futures.immediateFuture(deviceData);
        } else {
            Device device = ctx.getDeviceService().findDeviceById(ctx.getTenantId(), deviceId);
            return Futures.transform(getState(ctx, deviceId),
                    state -> new DeviceData(device.getName(), device.getType(), state),
                    ctx.getDbCallbackExecutor());
        }
    }

    private void scheduleTickMsg(TbContext ctx) {
        long curTs = System.currentTimeMillis();
        if (lastScheduledTs == 0L) {
            lastScheduledTs = curTs;
        }
        lastScheduledTs = lastScheduledTs + delay;
        long curDelay = Math.max(0L, (lastScheduledTs - curTs));
        TbMsg tickMsg = ctx.newMsg(ServiceQueue.MAIN, TB_MSG_CUSTOM_NODE_MSG, ctx.getSelfId(), new TbMsgMetaData(), "");
        ctx.tellSelf(tickMsg, curDelay);
    }

    private void save(TbContext ctx, DeviceId deviceId, String key, long value) {
        ctx.getTelemetryService().saveAttrAndNotify(ctx.getTenantId(), deviceId,
                DataConstants.SERVER_SCOPE, key, value, new VixNodeCallback(ctx, null));
    }

    private ListenableFuture<State> getState(TbContext ctx, DeviceId deviceId) {
        ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService()
                .find(ctx.getTenantId(), deviceId, DataConstants.SERVER_SCOPE, ATTRIBUTES);
        return Futures.transform(attributesFuture, attributes -> {
            boolean active = getEntryValue(attributes, ACTIVITY_STATE, false);
            long lastActivityTime = getEntryValue(attributes, LAST_ACTIVITY_TIME, 0L);
            long inactivityAlarmTime = getEntryValue(attributes, INACTIVITY_ALARM_TIME, 0L);
            long criticalInactivityTimeout = TimeUnit.SECONDS.toMillis(config.getCriticalInactivityTimeoutInSec());
            long criticalInactivityAlarmTime = getEntryValue(attributes, CRITICAL_INACTIVITY_ALARM_TIME, 0L);
            return new State(active, lastActivityTime, inactivityAlarmTime, criticalInactivityTimeout, criticalInactivityAlarmTime);
        }, ctx.getDbCallbackExecutor());
    }

    private ConcurrentMap<DeviceId, DeviceData> getDeviceDataMap(TbContext ctx) throws ExecutionException, InterruptedException {
        ConcurrentMap<DeviceId, DeviceData> map = new ConcurrentHashMap<>();
        PageLink pageLink = new PageLink(ENTITIES_LIMIT);
        List<ListenableFuture<DeviceData>> futures = new ArrayList<>();
        while (pageLink != null) {
            PageData<Device> page = ctx.getDeviceService().findDevicesByTenantId(ctx.getTenantId(), pageLink);
            pageLink = page.hasNext() ? pageLink.nextPageLink() : null;
            for (Device device : page.getData()) {
                futures.add(Futures.transform(getState(ctx, device.getId()),
                        state -> map.computeIfAbsent(device.getId(), n -> new DeviceData(device.getName(), device.getType(), state)),
                        ctx.getDbCallbackExecutor()));
            }
        }
        if (!futures.isEmpty()) {
            Futures.allAsList(futures).get();
        }
        log.info("[{}] Found {} devices for critical inactivity processing!", ctx.getTenantId(), map.size());
        return map;
    }

    private long getEntryValue(List<? extends KvEntry> kvEntries, String attributeName, long defaultValue) {
        if (kvEntries != null) {
            for (KvEntry entry : kvEntries) {
                if (entry != null && !StringUtils.isEmpty(entry.getKey()) && entry.getKey().equals(attributeName)) {
                    return entry.getLongValue().orElse(defaultValue);
                }
            }
        }
        return defaultValue;
    }

    private boolean getEntryValue(List<? extends KvEntry> kvEntries, String attributeName, boolean defaultValue) {
        if (kvEntries != null) {
            for (KvEntry entry : kvEntries) {
                if (entry != null && !StringUtils.isEmpty(entry.getKey()) && entry.getKey().equals(attributeName)) {
                    return entry.getBooleanValue().orElse(defaultValue);
                }
            }
        }
        return defaultValue;
    }

    @Data
    @AllArgsConstructor
    private static class State {
        private boolean active;
        private long lastActivityTime;
        private long lastInactivityAlarmTime;
        private long criticalInactivityTimeout;
        private long lastCriticalInactivityAlarmTime;
    }

    @Data
    @AllArgsConstructor
    private static class DeviceData {
        private String deviceName;
        private String deviceType;
        private State deviceState;
    }
}