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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.device.DeviceSearchQuery;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis update projects attributes",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisUpdateProjectsAttributesNode implements TbNode {

    private static final String PROJECT = "PROJECT";
    private static final String DATA_DEVICE_TYPE = "DATA_DEVICE";
    private static final String IS_IN_SPACE = "IS_IN_SPACE";
    private static final String ACTIVE_ATTR = "active";
    private static final String DEVICES = "Devices";
    private final Set<String> exceptGroupNames = new HashSet<>(Arrays.asList("All", "DATA_DEVICE"));

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        Map<EntityId, ListenableFuture<List<EntityGroupState>>> entityGroupStatesMapByProject
                = getEntityGroupStatesMapByProject(ctx);
        List<ListenableFuture<List<Void>>> resultsFutures = new ArrayList<>();
        for (Map.Entry<EntityId, ListenableFuture<List<EntityGroupState>>> entry : entityGroupStatesMapByProject.entrySet()) {
            resultsFutures.add(Futures.transformAsync(entry.getValue(), entityGroupStates -> {
                ObjectNode allGroupsDataNode = OBJECT_MAPPER.createObjectNode();
                if (CollectionUtils.isEmpty(entityGroupStates)) {
                    return Futures.immediateFuture(null);
                }
                for (EntityGroupState entityGroupState : entityGroupStates) {
                    ObjectNode oneGroupDevices = OBJECT_MAPPER.createObjectNode();
                    int countOfActiveDevices = 0;
                    for (DeviceState deviceState : entityGroupState.getDeviceStates()) {
                        countOfActiveDevices += !deviceState.isActive() ? 1 : 0;
                    }
                    oneGroupDevices.put("inactiveDeviceCount", countOfActiveDevices);
                    oneGroupDevices.put("deviceCount", entityGroupState.getDeviceStates().size());
                    allGroupsDataNode.set(entityGroupState.getEntityGroup().getName(), oneGroupDevices);
                }
                KvEntry kvEntry = new JsonDataEntry("devicesByGroups", OBJECT_MAPPER.writeValueAsString(allGroupsDataNode));
                return ctx.getAttributesService()
                        .save(ctx.getTenantId(), entry.getKey(),
                                DataConstants.SERVER_SCOPE, Collections.singletonList(new BaseAttributeKvEntry(kvEntry, System.currentTimeMillis())));
            }, ctx.getDbCallbackExecutor()));
        }
        DonAsynchron.withCallback(
                Futures.transform(Futures.allAsList(resultsFutures), v -> null, ctx.getDbCallbackExecutor()),
                list -> ctx.tellSuccess(msg),
                throwable -> ctx.tellFailure(msg, throwable));
    }

    private Map<EntityId, ListenableFuture<List<EntityGroupState>>> getEntityGroupStatesMapByProject(TbContext ctx) {
        Map<EntityId, ListenableFuture<List<EntityGroupState>>> entityGroupStatesMapByProject = new HashMap<>();
        for (Map.Entry<AssetId, ListenableFuture<List<DeviceState>>> entry : getDeviceStatesMapByProject(ctx).entrySet()) {
            entityGroupStatesMapByProject.put(entry.getKey(), Futures.transformAsync(entry.getValue(), devices ->
                    Futures.transformAsync(getMapOfEntityGroupsAndDeviceIds(ctx), mapOfEntityGroupsAndDevicesIds -> {
                        List<ListenableFuture<EntityGroupState>> entityGroupStatesForCurrentProject = new ArrayList<>();
                        for (Map.Entry<EntityGroup, ListenableFuture<Set<EntityId>>> entityGroupDevicesEntry : mapOfEntityGroupsAndDevicesIds.entrySet()) {
                            entityGroupStatesForCurrentProject.add(Futures.transform(entityGroupDevicesEntry.getValue(), entitySet -> {
                                EntityGroupState entityGroupState = new EntityGroupState();
                                if (!CollectionUtils.isEmpty(entitySet)) {
                                    entityGroupState.setDeviceStates(devices
                                            .stream()
                                            .filter(deviceState -> entitySet.contains(deviceState.getDevice().getId()))
                                            .collect(Collectors.toList()));
                                } else {
                                    entityGroupState.setDeviceStates(new ArrayList<>());
                                }
                                entityGroupState.setEntityGroup(entityGroupDevicesEntry.getKey());
                                return entityGroupState;
                            }, ctx.getDbCallbackExecutor()));
                        }
                        return Futures.allAsList(entityGroupStatesForCurrentProject);
                    }, ctx.getDbCallbackExecutor()), ctx.getDbCallbackExecutor()));
        }
        return entityGroupStatesMapByProject;
    }

    private ListenableFuture<Map<EntityGroup, ListenableFuture<Set<EntityId>>>> getMapOfEntityGroupsAndDeviceIds(TbContext ctx) {
        return Futures.transformAsync(getDeviceGroups(ctx), deviceGroups -> {
            if (CollectionUtils.isEmpty(deviceGroups)) {
                log.error("Didn't find entity groups for {} except {}", EntityType.DEVICE, exceptGroupNames);
                return Futures.immediateFuture(new HashMap<>());
            }
            Map<EntityGroup, ListenableFuture<Set<EntityId>>> entityGroupsEntityIds = new HashMap<>();
            for (EntityGroup deviceGroup : deviceGroups) {
                entityGroupsEntityIds.put(deviceGroup, getEntityIdsInEntityGroup(ctx, deviceGroup.getId()));
            }
            return Futures.immediateFuture(entityGroupsEntityIds);
        }, ctx.getDbCallbackExecutor());
    }

    private void findAllEntitiesForGroup(TbContext ctx, SettableFuture<Set<EntityId>> settableFuture,
                                         EntityGroupId entityGroupId, Set<EntityId> entityIds, AtomicInteger number) {
        ListenableFuture<List<EntityId>> future = ctx.getPeContext()
                .getEntityGroupService()
                .findAllEntityIds(ctx.getTenantId(), entityGroupId, new PageLink(1000, number.get()));
        Futures.addCallback(future, new FutureCallback<List<EntityId>>() {
            @Override
            public void onSuccess(@Nullable List<EntityId> entityIdsForPack) {
                if (!CollectionUtils.isEmpty(entityIdsForPack)) {
                    entityIds.addAll(entityIdsForPack);
                    if (entityIdsForPack.size() < 1000) {
                        settableFuture.set(entityIds);
                    } else {
                        number.incrementAndGet();
                        findAllEntitiesForGroup(ctx, settableFuture, entityGroupId, entityIds, number);
                    }
                } else {
                    settableFuture.set(entityIds);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[{}] Error occurred during device fetch for group!", entityGroupId, t);
            }
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Set<EntityId>> getEntityIdsInEntityGroup(TbContext ctx, EntityGroupId entityGroupId) {
        SettableFuture<Set<EntityId>> settableFuture = SettableFuture.create();
        findAllEntitiesForGroup(ctx, settableFuture, entityGroupId, new HashSet<>(), new AtomicInteger());
        return settableFuture;
    }

    private ListenableFuture<List<EntityGroup>> getDeviceGroups(TbContext ctx) {
        ListenableFuture<List<EntityGroup>> deviceGroupsFuture = ctx.getPeContext()
                .getEntityGroupService()
                .findEntityGroupsByType(ctx.getTenantId(), ctx.getTenantId(), EntityType.DEVICE);
        return Futures.transform(deviceGroupsFuture, deviceGroups -> {
            if (!CollectionUtils.isEmpty(deviceGroups)) {
                return deviceGroups
                        .stream()
                        .filter(entityGroup -> !exceptGroupNames.contains(entityGroup.getName())
                                && !entityGroup.getName().endsWith(DEVICES))
                        .collect(Collectors.toList());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private Map<AssetId, ListenableFuture<List<DeviceState>>> getDeviceStatesMapByProject(TbContext ctx) {
        List<AssetId> projectsIds = getProjectsIds(ctx);
        if (CollectionUtils.isEmpty(projectsIds)) {
            log.warn("Didn't find assets with type = {}", PROJECT);
            return new HashMap<>();
        }
        Map<AssetId, ListenableFuture<List<DeviceState>>> deviceStatesMapByProject = new HashMap<>();
        for (AssetId projectId : projectsIds) {
            deviceStatesMapByProject.put(projectId, getDeviceStatesByProject(ctx, projectId));
        }
        return deviceStatesMapByProject;
    }

    private ListenableFuture<List<DeviceState>> getDeviceStatesByProject(TbContext ctx, AssetId projectId) {
        ListenableFuture<List<Device>> devicesFuture = ctx.getDeviceService()
                .findDevicesByQuery(ctx.getTenantId(), getDeviceSearchQuery(projectId));
        return Futures.transformAsync(devicesFuture, devices -> {
            if (CollectionUtils.isEmpty(devices)) {
                log.error("Didn't find devices for Project [{}]", projectId);
                return Futures.immediateFuture(new ArrayList<>());
            }
            List<ListenableFuture<DeviceState>> deviceStates = new ArrayList<>();
            for (Device device : devices) {
                deviceStates.add(Futures.transform(ctx.getAttributesService()
                        .find(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, Collections.singletonList(ACTIVE_ATTR)), kvEntries -> {
                    if (!CollectionUtils.isEmpty(kvEntries)) {
                        return new DeviceState(device, kvEntries.get(0).getBooleanValue().orElse(false));
                    }
                    return new DeviceState(device, false);
                }, ctx.getDbCallbackExecutor()));
            }
            return Futures.allAsList(deviceStates);
        }, ctx.getDbCallbackExecutor());
    }

    private DeviceSearchQuery getDeviceSearchQuery(EntityId rootId) {
        DeviceSearchQuery deviceSearchQuery = new DeviceSearchQuery();
        deviceSearchQuery.setDeviceTypes(Collections.singletonList(DATA_DEVICE_TYPE));
        deviceSearchQuery.setParameters(new RelationsSearchParameters(rootId, EntitySearchDirection.FROM, 10, false));
        deviceSearchQuery.setRelationType(IS_IN_SPACE);
        return deviceSearchQuery;
    }

    private List<AssetId> getProjectsIds(TbContext ctx) {
        return ctx.getAssetService()
                .findAssetsByTenantIdAndType(ctx.getTenantId(), PROJECT, new PageLink(1000))
                .getData()
                .stream()
                .map(Asset::getId)
                .collect(Collectors.toList());
    }

    @Data
    private static class EntityGroupState {
        private EntityGroup entityGroup;
        private List<DeviceState> deviceStates;
    }

    @Data
    @AllArgsConstructor
    private static class DeviceState {
        private Device device;
        private boolean active;
    }

    @Override
    public void destroy() {

    }
}
