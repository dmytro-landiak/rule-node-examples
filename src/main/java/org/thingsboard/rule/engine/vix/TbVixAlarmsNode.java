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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
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
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix alarms",
        configClazz = TbVixAlarmsNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeVixAlarmsConfig")
public class TbVixAlarmsNode implements TbNode {

    private static final String ALARM_STATE_PREFIX = "alarmState";
    private static final String DEVICE_OUTPUT_PREFIX = "deviceOutput";
    private static final String TRIGGERED_METRIC_KEY = "triggeredMetric";
    private static final String EMPTY_STR = "";
    private static final String FETCH_ORDER = "DESC";
    private static final String STATE_KEY = "state";
    private static final long ONE_DAY = 24 * 3600 * 1000;

    private final ConcurrentHashMap<EntityId, ConcurrentMap<String, Deque<Integer>>> devicesAlarmStatesMap = new ConcurrentHashMap<>();

    private TbVixAlarmsNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbVixAlarmsNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        JsonElement jsonElement = new JsonParser().parse(msg.getData());
        if (jsonElement.isJsonObject()) {
            JsonObject jo = jsonElement.getAsJsonObject();
            List<String> alarmStateKeys = getAlarmStateKeys(jo);

            ConcurrentMap<String, Deque<Integer>> deviceAlarmStatesMap = devicesAlarmStatesMap.computeIfAbsent(msg.getOriginator(), id -> new ConcurrentHashMap<>());

            ListenableFuture<Map<String, AlarmContainer>> resultFuture = Futures.transform(
                    getFuture(ctx, msg, jo, deviceAlarmStatesMap, alarmStateKeys),
                    v -> getMap(deviceAlarmStatesMap, alarmStateKeys),
                    ctx.getDbCallbackExecutor());

            DonAsynchron.withCallback(resultFuture, resultMap -> {
                if (resultMap.isEmpty()) {
                    ctx.tellNext(msg, TbRelationTypes.SUCCESS);
                } else {
                    ctx.getPeContext().ack(msg);
                    for (Map.Entry<String, AlarmContainer> entry : resultMap.entrySet()) {
                        TbMsg tbMsg = TbMsg.newMsg(msg.getType(), msg.getOriginator(),
                                createMetaData(msg.getMetaData(), entry, jo), msg.getData());
                        ctx.enqueueForTellNext(tbMsg, TbRelationTypes.SUCCESS);
                    }
                }
            }, throwable -> ctx.tellFailure(msg, throwable));
        } else {
            log.error("Data is not a valid object {}", msg.getData());
            ctx.tellFailure(msg, new RuntimeException("Data is not a valid object " + msg.getData()));
        }
    }

    @Override
    public void destroy() {

    }

    private Map<String, AlarmContainer> getMap(ConcurrentMap<String, Deque<Integer>> deviceAlarmStatesMap, List<String> alarmStateKeys) {
        Map<String, AlarmContainer> checkMap = new HashMap<>();
        for (String alarmStateKey : alarmStateKeys) {
            Deque<Integer> values = deviceAlarmStatesMap.get(alarmStateKey);
            for (Integer value : values) {
                AlarmContainer alarmContainer = checkMap.computeIfAbsent(alarmStateKey, s ->
                        new AlarmContainer(new AtomicInteger(0), value, 0, new AtomicInteger(0)));
                if (alarmContainer.getPrevValueCounter().getAndIncrement() == 1) {
                    alarmContainer.setPrevValue(value);
                }
                if (alarmContainer.getValue() == 0) {
                    if (value == 0) {
                        alarmContainer.getCounter().incrementAndGet();
                    }
                } else {
                    if (value > 0 && value < 4) {
                        alarmContainer.getCounter().incrementAndGet();
                    }
                }
            }
        }
        return checkMap;
    }

    private ListenableFuture<Void> getFuture(TbContext ctx, TbMsg msg, JsonObject jo, ConcurrentMap<String, Deque<Integer>> deviceAlarmStatesMap,
                                             List<String> alarmStateKeys) {
        if (deviceAlarmStatesMap.isEmpty()) {
            return getHistory(ctx, msg, deviceAlarmStatesMap, alarmStateKeys);
        } else {
            List<String> keysToFetch = getListOfKeysToFetch(deviceAlarmStatesMap, alarmStateKeys, jo);
            if (!keysToFetch.isEmpty()) {
                return getHistory(ctx, msg, deviceAlarmStatesMap, keysToFetch);
            } else {
                return Futures.immediateFuture(null);
            }
        }
    }

    private List<String> getListOfKeysToFetch(ConcurrentMap<String, Deque<Integer>> deviceAlarmStatesMap, List<String> alarmStateKeys,
                                              JsonObject jo) {
        List<String> keysToFetch = new ArrayList<>();
        for (String alarmStateKey : alarmStateKeys) {
            Deque<Integer> values = deviceAlarmStatesMap.getOrDefault(alarmStateKey, null);
            if (values == null || values.size() < config.getEvaluatedDataPoints()) {
                keysToFetch.add(alarmStateKey);
                deviceAlarmStatesMap.put(alarmStateKey, new LinkedBlockingDeque<>());
            } else {
                values.removeLast();
                values.addFirst(jo.get(alarmStateKey).getAsInt());
            }
        }
        return keysToFetch;
    }

    private ListenableFuture<Void> getHistory(TbContext ctx, TbMsg msg, ConcurrentMap<String, Deque<Integer>> deviceAlarmStatesMap,
                                              List<String> keys) {
        ListenableFuture<List<TsKvEntry>> historicalDataFuture = ctx.getTimeseriesService().findAll(ctx.getTenantId(),
                msg.getOriginator(), getReadTsKvQueries(keys));
        return Futures.transform(historicalDataFuture, tsKvEntries -> {
            if (tsKvEntries != null && !tsKvEntries.isEmpty()) {
                for (TsKvEntry entry : tsKvEntries) {
                    Deque<Integer> values = deviceAlarmStatesMap.computeIfAbsent(entry.getKey(), k -> new LinkedBlockingDeque<>());
                    values.add(entry.getLongValue().orElse(0L).intValue());
                }
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private List<String> getAlarmStateKeys(JsonObject jo) {
        List<String> alarmStateKeys = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
            if (entry.getKey().startsWith(ALARM_STATE_PREFIX)) {
                alarmStateKeys.add(entry.getKey());
            }
        }
        return alarmStateKeys;
    }

    private TbMsgMetaData createMetaData(TbMsgMetaData metadata, Map.Entry<String, AlarmContainer> entry, JsonObject jo) {
        TbMsgMetaData newMetaData = metadata.copy();
        addTriggeredMetric(entry, jo, newMetaData);
        newMetaData.putValue("alarmType", entry.getKey());

        if (entry.getValue().getValue() != entry.getValue().getPrevValue()) {
            newMetaData.putValue("sendUpdate", EMPTY_STR);
        }

        if (entry.getValue().getCounter().get() >= config.getDataPointsToAlarm()) {
            switch (entry.getValue().getValue()) {
                case 1:
                    newMetaData.putValue(STATE_KEY, "Warning");
                    break;
                case 2:
                    newMetaData.putValue(STATE_KEY, "Critical");
                    break;
                case 3:
                    newMetaData.putValue(STATE_KEY, "Indeterminate");
                    break;
                default:
                    newMetaData.putValue(STATE_KEY, "Ok");
                    break;
            }
        }
        return newMetaData;
    }

    private void addTriggeredMetric(Map.Entry<String, AlarmContainer> entry, JsonObject jo, TbMsgMetaData newMetaData) {
        String triggeredMetricKey = DEVICE_OUTPUT_PREFIX + StringUtils.delete(entry.getKey(), ALARM_STATE_PREFIX);
        if (jo.has(triggeredMetricKey)) {
            newMetaData.putValue(TRIGGERED_METRIC_KEY, jo.get(triggeredMetricKey).getAsString());
        } else {
            newMetaData.putValue(TRIGGERED_METRIC_KEY, EMPTY_STR);
        }
    }

    private List<ReadTsKvQuery> getReadTsKvQueries(List<String> alarmStateKeys) {
        long endTs = System.currentTimeMillis();
        long startTs = endTs - ONE_DAY;
        List<ReadTsKvQuery> queries = new ArrayList<>();
        alarmStateKeys.forEach(key ->
                queries.add(new BaseReadTsKvQuery(key, startTs, endTs, validateLimit(config.getEvaluatedDataPoints()), FETCH_ORDER)));
        return queries;
    }

    private int validateLimit(int limit) {
        if (limit <= 0) {
            return 1;
        }
        return limit;
    }

    @Data
    @AllArgsConstructor
    private static class AlarmContainer {
        private AtomicInteger counter;
        private int value;
        private int prevValue;
        private AtomicInteger prevValueCounter;
    }
}
