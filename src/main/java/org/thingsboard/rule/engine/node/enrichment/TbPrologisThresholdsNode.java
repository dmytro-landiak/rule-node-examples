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
package org.thingsboard.rule.engine.node.enrichment;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonParser;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.IdBased;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;
import static org.thingsboard.server.common.data.DataConstants.SERVER_SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "prologis thresholds",
        configClazz = TbPrologisThresholdsNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisThresholdsNodeConfig")
public class TbPrologisThresholdsNode implements TbNode {

    private TbPrologisThresholdsNodeConfiguration config;

    private static final String MIN = "Min";
    private static final String MAX = "Max";
    private static final String ENABLE_PERIOD = "EnableAlarmUpdateNotificationPeriodically";
    private static final String INTERVAL = "SendAlarmUpdateNotificationInterval";
    private static final String TIME_UNIT = "TimeUnit";

    private static final String MAX_THRESHOLD_VALUE = "maxThresholdValue";
    private static final String MIN_THRESHOLD_VALUE = "minThresholdValue";
    private static final String ALARM_TYPE = "alarmType";
    private static final String CURRENT_VALUE = "currentValue";
    private static final String CURRENT_UOM = "currentUom";

    private static final String VALUE = "_VALUE";
    private static final String UOM = "_UOM";

    private static final String ALL_GROUP = "All";
    private static final String DATA_DEVICE_GROUP = "DATA_DEVICE";
    private static final String PRIORITY_ATTR = "priority";

    private static final Set<String> SUFFIXES = new HashSet<String>() {{
        add(MIN);
        add(MAX);
        add(ENABLE_PERIOD);
        add(INTERVAL);
        add(TIME_UNIT);
    }};

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisThresholdsNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        Map<String, KvEntry> kvEntryByName = getKvEntriesByKey(msg);
        Map<String, String> attrPrefixesByTelemetryKey = kvEntryByName.keySet()
                .stream()
                .filter(key -> key.contains(VALUE))
                .map(key -> key.replace(VALUE, ""))
                .collect(Collectors.toMap(key -> key, this::getAttrPrefix));

        DonAsynchron.withCallback(findAttributes(ctx, msg, new HashSet<>(attrPrefixesByTelemetryKey.values())), attrs -> {
            ctx.ack(msg);
            for (Map.Entry<String, String> entry : attrPrefixesByTelemetryKey.entrySet()) {
                TbMsgMetaData metaData = msg.getMetaData().copy();
                List<AttributeKvEntry> tempAttrs = attrs.get(entry.getValue());
                if (tempAttrs != null) {
                    for (AttributeKvEntry attr : tempAttrs) {
                        if (attr.getKey().contains("Max")) {
                            metaData.putValue(MAX_THRESHOLD_VALUE, attr.getValueAsString());
                        } else if (attr.getKey().contains("Min")) {
                            metaData.putValue(MIN_THRESHOLD_VALUE, attr.getValueAsString());
                        } else {
                            String suffix = attr.getKey().replace(entry.getValue(), "");
                            metaData.putValue(suffix.substring(0, 1).toLowerCase() + suffix.substring(1), attr.getValueAsString());
                        }
                    }
                }
                String alarmType = Arrays.stream(entry.getKey().split("_"))
                        .map(String::toLowerCase)
                        .map(key -> key.substring(0, 1).toUpperCase() + key.substring(1))
                        .collect(Collectors.joining(" "));
                metaData.putValue(ALARM_TYPE, alarmType);
                KvEntry kvEntryValue = kvEntryByName.get(entry.getKey() + VALUE);
                if (kvEntryValue != null) {
                    metaData.putValue(CURRENT_VALUE, kvEntryValue.getValueAsString());
                }
                KvEntry kvEntryUom = kvEntryByName.get(entry.getKey() + UOM);
                if (kvEntryUom != null) {
                    metaData.putValue(CURRENT_UOM, kvEntryUom.getValueAsString());
                }

                ctx.enqueueForTellNext(TbMsg.newMsg(msg.getType(), msg.getOriginator(), metaData, msg.getData()), SUCCESS);
            }
        }, throwable -> ctx.tellFailure(msg,throwable));
    }

    private ListenableFuture<Map<String, List<AttributeKvEntry>>> findAttributes(TbContext ctx, TbMsg msg, Set<String> attrPrefixes) {
        Map<String, List<String>> attrsNamesByPrefix = getAttributesNamesByPrefix(attrPrefixes);
        if (CollectionUtils.isEmpty(attrsNamesByPrefix)) {
            return Futures.immediateFuture(new HashMap<>());
        }
        return Futures.transformAsync(findAttributesOnEntityLevel(ctx, msg.getOriginator(), getAttrsNames(attrsNamesByPrefix)), deviceLevelAttrs -> {
            if (deviceLevelAttrs != null) {
                updateAttrKeys(deviceLevelAttrs, attrsNamesByPrefix);
            }
            if (!CollectionUtils.isEmpty(attrsNamesByPrefix)) {
                return Futures.transformAsync(findAttributesOnEntityGroupLevel(ctx, msg, getAttrsNames(attrsNamesByPrefix)), groupLevelAttrs -> {
                    if (groupLevelAttrs != null) {
                        updateAttrKeys(groupLevelAttrs, attrsNamesByPrefix);
                    }
                    if (!CollectionUtils.isEmpty(attrsNamesByPrefix)) {
                        return Futures.transform(findAttributesOnEntityLevel(ctx, ctx.getTenantId(), getAttrsNames(attrsNamesByPrefix)),
                                tenantLevelAttrs -> combineAttrs(deviceLevelAttrs, groupLevelAttrs, tenantLevelAttrs), ctx.getDbCallbackExecutor());
                    }
                    return Futures.immediateFuture(combineAttrs(deviceLevelAttrs, groupLevelAttrs));
                }, ctx.getDbCallbackExecutor());
            }
            return Futures.immediateFuture(deviceLevelAttrs);
        }, ctx.getDbCallbackExecutor());
    }

    private List<String> getAttrsNames(Map<String, List<String>> attrsNamesByPrefix) {
        return attrsNamesByPrefix.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @SafeVarargs
    private final Map<String, List<AttributeKvEntry>> combineAttrs(Map<String, List<AttributeKvEntry>>... attrsMaps) {
        return Arrays.stream(attrsMaps)
                .filter(map -> !CollectionUtils.isEmpty(map))
                .map(Map::values)
                .flatMap(Collection::stream)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(s -> removeSuffixesFromString(s.getKey())));
    }

    private ListenableFuture<Map<String, List<AttributeKvEntry>>> findAttributesOnEntityGroupLevel(TbContext ctx, TbMsg msg, List<String> keys) {
        ListenableFuture<List<EntityGroup>> allDeviceEntityGroupsFuture = ctx.getPeContext()
                .getEntityGroupService()
                .findEntityGroupsByType(ctx.getTenantId(), ctx.getTenantId(), EntityType.DEVICE);
        ListenableFuture<Set<EntityGroupId>> skipEntityGroupIdsFuture = Futures.transform(allDeviceEntityGroupsFuture, allDeviceEntityGroups -> {
            if (allDeviceEntityGroups != null) {
                return allDeviceEntityGroups
                        .stream()
                        .filter(entityGroup -> entityGroup.getName().equals(ALL_GROUP) || entityGroup.getName().equals(DATA_DEVICE_GROUP))
                        .map(IdBased::getId)
                        .collect(Collectors.toSet());
            }
            return new HashSet<>();
        }, ctx.getDbCallbackExecutor());

        ListenableFuture<List<EntityGroupId>> entityGroupIdsForEntityFuture = ctx.getPeContext().getEntityGroupService().findEntityGroupsForEntity(ctx.getTenantId(), msg.getOriginator());

        ListenableFuture<List<AttributeKvEntry>> attrsFuture = Futures.transformAsync(entityGroupIdsForEntityFuture, entityGroupIdsForEntity -> {
            if (CollectionUtils.isEmpty(entityGroupIdsForEntity)) {
                log.warn("Device[{}] doesn't belong to any group", msg.getOriginator());
                return Futures.immediateFuture(new ArrayList<>());
            }
            return Futures.transformAsync(skipEntityGroupIdsFuture, skipEntityGroupIds -> {
                if (CollectionUtils.isEmpty(skipEntityGroupIds)) {
                    return Futures.immediateFuture(new ArrayList<>());
                }
                List<EntityGroupId> entityGroupIds = entityGroupIdsForEntity
                        .stream()
                        .filter(entityGroupId -> !skipEntityGroupIds.contains(entityGroupId)).collect(Collectors.toList());
                if (entityGroupIds.size() == 1) {
                    return getAttributesAsync(ctx, entityGroupIds.get(0), keys);
                } else {
                    List<ListenableFuture<GroupAttrs>> results = new ArrayList<>();
                    for (EntityGroupId entityGroupId : entityGroupIds) {
                        results.add(Futures.transform(getAttributesAsync(ctx, entityGroupId, keys), attrs -> {
                            if (!CollectionUtils.isEmpty(attrs)) {
                                return new GroupAttrs(entityGroupId, attrs);
                            }
                            return null;
                        }, ctx.getDbCallbackExecutor()));
                    }
                    return Futures.transform(Futures.allAsList(results), groupAttrs -> {
                        if (!CollectionUtils.isEmpty(groupAttrs)) {
                            Map<Long, List<GroupAttrs>> attributesListsByPriority = new HashMap<>();
                            for (GroupAttrs attrs : groupAttrs) {
                                if (attrs != null) {
                                    Optional<Long> priorityOpt = attrs.getAttrs()
                                            .stream()
                                            .filter(attr -> attr.getKey().equals(PRIORITY_ATTR)
                                                    && attr.getLongValue().isPresent())
                                            .map(attr -> attr.getLongValue().get())
                                            .findFirst();
                                    priorityOpt.ifPresent(aLong -> attributesListsByPriority.computeIfAbsent(aLong, k -> new ArrayList<>()).add(attrs));
                                }
                            }
                            Optional<Long> minPriorityOpt = attributesListsByPriority
                                    .keySet()
                                    .stream()
                                    .min(Long::compareTo);
                            if (minPriorityOpt.isPresent()) {
                                List<GroupAttrs> groupAttrsList = attributesListsByPriority.get(minPriorityOpt.get());
                                if (!CollectionUtils.isEmpty(groupAttrsList)) {
                                    if (groupAttrsList.size() > 1) {
                                        msg.getMetaData().putValue("deviceGroupIds", groupAttrsList
                                                .stream()
                                                .map(GroupAttrs::getGroupId)
                                                .collect(Collectors.toList())
                                                .toString());
                                        return new ArrayList<>();
                                    }
                                    return groupAttrsList.get(0)
                                            .getAttrs();
                                }
                            }
                        } else {
                            log.warn("{}[id = {}] : Didn't find groups with attributes", msg.getOriginator().getEntityType(), msg.getOriginator());
                        }
                        return new ArrayList<>();
                    }, ctx.getDbCallbackExecutor());
                }
            }, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());

        return Futures.transform(attrsFuture, attrs -> {
            if (CollectionUtils.isEmpty(attrs)) {
                return new HashMap<>();
            }
            attrs.removeIf(attributeKvEntry -> attributeKvEntry.getKey().equals(PRIORITY_ATTR));
            return attrs.stream()
                    .collect(Collectors.groupingBy(s -> removeSuffixesFromString(s.getKey())));
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<AttributeKvEntry>> getAttributesAsync(TbContext ctx, EntityId entityId, List<String> keys) {
        return ctx.getAttributesService().find(ctx.getTenantId(), entityId, SERVER_SCOPE,
                Stream.concat(keys.stream(),
                        Stream.of(PRIORITY_ATTR)).collect(Collectors.toList())
        );
    }

    private void updateAttrKeys(Map<String, List<AttributeKvEntry>> foundedAttrs, Map<String, List<String>> allAttrsNamesByPrefix) {
        for (Map.Entry<String, List<AttributeKvEntry>> entry : foundedAttrs.entrySet()) {
            if (entry.getValue()
                    .stream()
                    .anyMatch(attr -> attr.getKey().contains(MIN) || attr.getKey().contains(MAX))) {
                allAttrsNamesByPrefix.get(entry.getKey())
                        .removeAll(Arrays.asList(entry.getKey() + MAX, entry.getKey() + MIN));
            }
            if (entry.getValue()
                    .stream()
                    .anyMatch(attr -> attr.getKey().contains(ENABLE_PERIOD))) {
                allAttrsNamesByPrefix.get(entry.getKey())
                        .removeAll(Arrays.asList(entry.getKey() + ENABLE_PERIOD, entry.getKey() + INTERVAL, entry.getKey() + TIME_UNIT));
            }
        }
        allAttrsNamesByPrefix.values().removeIf(CollectionUtils::isEmpty);
    }

    private ListenableFuture<Map<String, List<AttributeKvEntry>>> findAttributesOnEntityLevel(TbContext ctx, EntityId entityId, List<String> keys) {
        ListenableFuture<List<AttributeKvEntry>> attrsFuture = ctx.getAttributesService()
                .find(ctx.getTenantId(), entityId, DataConstants.SERVER_SCOPE, keys);
        return Futures.transform(attrsFuture, attrs -> {
            if (!CollectionUtils.isEmpty(attrs)) {
                return attrs.stream()
                        .collect(Collectors.groupingBy(s -> removeSuffixesFromString(s.getKey())));
            }
            return new HashMap<>();
        }, ctx.getDbCallbackExecutor());
    }

    private String removeSuffixesFromString(String arg) {
        for (String suffix : SUFFIXES) {
            arg = arg.replace(suffix, "");
        }
        return arg;
    }

    private Map<String, List<String>> getAttributesNamesByPrefix(Set<String> attrPrefixes) {
        Map<String, List<String>> res = new ConcurrentHashMap<>();
        for (String prefix : attrPrefixes) {
            res.put(prefix, getAttributesNamesForKey(prefix));
        }
        return res;
    }

    private List<String> getAttributesNamesForKey(String attrPrefix) {
        List<String> res = new ArrayList<>();
        for (String suffix : SUFFIXES) {
            res.add(attrPrefix + suffix);
        }
        return res;
    }

    private String getAttrPrefix(String telemetryKey) {
        String[] words = telemetryKey
                .toLowerCase()
                .split("_");
        if (words.length == 1) {
            return words[0];
        }
        StringBuilder stringBuilder = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            String tempWord = words[i];
            if (tempWord.length() == 1) {
                stringBuilder.append(tempWord.toUpperCase());
            } else {
                stringBuilder.append(tempWord.substring(0, 1).toUpperCase())
                        .append(tempWord.substring(1));
            }
        }
        return stringBuilder.toString();
    }

    private Map<String, KvEntry> getKvEntriesByKey(TbMsg msg) {
        return JsonConverter.convertToTelemetry(new JsonParser().parse(msg.getData()), msg.getTs())
                .values()
                .stream()
                .flatMap(Collection::stream)
                .filter(kvEntry -> config.getTelemetryKeys().contains(kvEntry.getKey().replace(VALUE, ""))
                        || config.getTelemetryKeys().contains(kvEntry.getKey().replace(UOM, "")))
                .collect(Collectors.toMap(KvEntry::getKey, kvEntry -> kvEntry));
    }

    @Override
    public void destroy() {

    }

    @Data
    private static class GroupAttrs {
        private final EntityGroupId groupId;
        private final List<AttributeKvEntry> attrs;
    }
}
