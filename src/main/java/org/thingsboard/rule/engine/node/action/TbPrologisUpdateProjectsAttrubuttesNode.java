package org.thingsboard.rule.engine.node.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.*;
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

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis update projects attributes",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisUpdateProjectsAttrubuttesNode implements TbNode {

    private static final String PROJECT = "PROJECT";
    private static final String DATA_DEVICE_TYPE = "DATA_DEVICE";
    private static final String IS_IN_SPACE = "IS_IN_SPACE";
    private static final String ACTIVE_ATTR = "active";
    private final Set<String> exceptGroupNames = new HashSet<>(Arrays.asList("All", "DATA_DEVICE"));

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        Map<EntityId, ListenableFuture<List<EntityGroupDevicesAndActives>>> projectIdEntityGroupDevicesAndActiveByGroupsMap
                = getEntityGroupDevicesAndActivesProjectsIds(ctx);
        List<ListenableFuture<List<Void>>> resultsFutures = new ArrayList<>();
        for (Map.Entry<EntityId, ListenableFuture<List<EntityGroupDevicesAndActives>>> projectIdEntityGroupDevicesAndActives : projectIdEntityGroupDevicesAndActiveByGroupsMap.entrySet()) {
            resultsFutures.add(Futures.transformAsync(projectIdEntityGroupDevicesAndActives.getValue(), entityGroupDevicesAndActivesList -> {
                ObjectNode allGroupsDataNode = OBJECT_MAPPER.createObjectNode();
                if (CollectionUtils.isEmpty(entityGroupDevicesAndActivesList)) {
                    return Futures.immediateFuture(null);
                }
                for (EntityGroupDevicesAndActives entityGroupDevicesAndActives : entityGroupDevicesAndActivesList) {
                    ObjectNode oneGroupDevices = OBJECT_MAPPER.createObjectNode();
                    ArrayNode deviceNames = OBJECT_MAPPER.createArrayNode();
                    int countOfActiveDevices = 0;
                    for (DeviceAndActive deviceAndActive : entityGroupDevicesAndActives.getDeviceAndActives()) {
                        deviceNames.add(deviceAndActive.getDevice().getName());
                        countOfActiveDevices += !deviceAndActive.isActive() ? 1 : 0;
                    }
                    oneGroupDevices.put("inactiveDeviceCount", countOfActiveDevices);
                    oneGroupDevices.put("deviceCount", deviceNames.size());
                    allGroupsDataNode.set(entityGroupDevicesAndActives.getEntityGroup().getName(), oneGroupDevices);
                }
                KvEntry kvEntry = new JsonDataEntry("devicesByGroups", OBJECT_MAPPER.writeValueAsString(allGroupsDataNode));
                return ctx.getAttributesService()
                        .save(ctx.getTenantId(), projectIdEntityGroupDevicesAndActives.getKey(),
                                DataConstants.SERVER_SCOPE, Collections.singletonList(new BaseAttributeKvEntry(kvEntry, System.currentTimeMillis())));
            }, ctx.getDbCallbackExecutor()));
        }
        DonAsynchron.withCallback(Futures.transform(Futures.allAsList(resultsFutures), voidsList -> {
            if (CollectionUtils.isEmpty(voidsList)) {
                return null;
            }
            return voidsList.stream().flatMap(Collection::stream).collect(Collectors.toList());
        },ctx.getDbCallbackExecutor()), list -> ctx.tellSuccess(msg), throwable -> ctx.tellFailure(msg, throwable));
    }

    private Map<EntityId, ListenableFuture<List<EntityGroupDevicesAndActives>>> getEntityGroupDevicesAndActivesProjectsIds(TbContext ctx) {
        Map<EntityId, ListenableFuture<List<EntityGroupDevicesAndActives>>> projectIdDevicesByGroups = new HashMap<>();
        for (Map.Entry<AssetId, ListenableFuture<List<DeviceAndActive>>> projectIdDevices : getDevicesAndActivesByProjectId(ctx).entrySet()) {
            projectIdDevicesByGroups.put(projectIdDevices.getKey(), Futures.transformAsync(projectIdDevices.getValue(), devices -> Futures.transformAsync(getMapOfEntityGroupsAndDevicesIds(ctx), mapOfEntityGroupsAndDevicesIds -> {
                List<ListenableFuture<EntityGroupDevicesAndActives>> entityGroupDevicesForCurrentProject = new ArrayList<>();
                for (Map.Entry<EntityGroup, ListenableFuture<Set<EntityId>>> entityGroupDevicesIds : mapOfEntityGroupsAndDevicesIds.entrySet()) {
                    entityGroupDevicesForCurrentProject.add(Futures.transform(entityGroupDevicesIds.getValue(), entitySet -> {
                        EntityGroupDevicesAndActives entityGroupDevicesAndActives = new EntityGroupDevicesAndActives();
                        if (!CollectionUtils.isEmpty(entitySet)) {
                            entityGroupDevicesAndActives.setDeviceAndActives(devices
                                    .stream()
                                    .filter(deviceAndActive -> entitySet.contains(deviceAndActive.getDevice().getId()))
                                    .collect(Collectors.toList()));
                        } else {
                            entityGroupDevicesAndActives.setDeviceAndActives(new ArrayList<>());
                        }
                        entityGroupDevicesAndActives.setEntityGroup(entityGroupDevicesIds.getKey());
                        return entityGroupDevicesAndActives;
                    }, ctx.getDbCallbackExecutor()));
                }
                return Futures.allAsList(entityGroupDevicesForCurrentProject);
            }, ctx.getDbCallbackExecutor()), ctx.getDbCallbackExecutor()));
        }
        return projectIdDevicesByGroups;
    }

    private ListenableFuture<Map<EntityGroup, ListenableFuture<Set<EntityId>>>> getMapOfEntityGroupsAndDevicesIds(TbContext ctx) {
        return Futures.transformAsync(getDeviceGroups(ctx), deviceGroups -> {
            if (CollectionUtils.isEmpty(deviceGroups)) {
                log.error("Didn't find entity groups for {} except {}", EntityType.DEVICE, exceptGroupNames);
                return Futures.immediateFuture(new HashMap<>());
            }
            Map<EntityGroup, ListenableFuture<Set<EntityId>>> entityGroupsEntitiesIds = new HashMap<>();
            for (EntityGroup deviceGroup : deviceGroups) {
                entityGroupsEntitiesIds.put(deviceGroup, getEntityIdsInEntityGroup(ctx, deviceGroup.getId()));
            }
            return Futures.immediateFuture(entityGroupsEntitiesIds);
        },ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Set<EntityId>> getEntityIdsInEntityGroup(TbContext ctx, EntityGroupId entityGroupId) {
        List<ListenableFuture<List<EntityId>>> entitiesIdsListFutures = new ArrayList<>();
        AtomicInteger number = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean(false);
        while (!stop.get()) {
            entitiesIdsListFutures.add(Futures.transform(ctx
                    .getPeContext()
                    .getEntityGroupService()
                    .findAllEntityIds(ctx.getTenantId(), entityGroupId, new PageLink(1000, number.get())), entitiesIds -> {
                if (CollectionUtils.isEmpty(entitiesIds) || entitiesIds.size() < 1000) {
                    stop.set(true);
                }
                number.getAndIncrement();
                return CollectionUtils.isEmpty(entitiesIds) ? new ArrayList<>() : entitiesIds;
            },ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(entitiesIdsListFutures), entitiesIdsList -> {
            Set<EntityId> entitiesIds = new HashSet<>();
            if (CollectionUtils.isEmpty(entitiesIdsList)) {
                log.error("Didn't find entities in entity group id={}", entityGroupId);
                return null;
            }
            for (List<EntityId> entityIdList: entitiesIdsList) {
                entitiesIds.addAll(entityIdList);
            }
            return entitiesIds;
        },ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<EntityGroup>> getDeviceGroups(TbContext ctx) {
        ListenableFuture<List<EntityGroup>> deviceGroupsFuture = ctx.getPeContext()
                .getEntityGroupService()
                .findEntityGroupsByType(ctx.getTenantId(), ctx.getTenantId(), EntityType.DEVICE);
        return Futures.transform(deviceGroupsFuture, deviceGroups -> {
            if (!CollectionUtils.isEmpty(deviceGroups)) {
                return deviceGroups
                        .stream()
                        .filter(entityGroup -> !exceptGroupNames.contains(entityGroup.getName()))
                        .collect(Collectors.toList());
            }
            return null;
        },ctx.getDbCallbackExecutor());
    }

    private Map<AssetId, ListenableFuture<List<DeviceAndActive>>> getDevicesAndActivesByProjectId(TbContext ctx) {
        List<AssetId> projectsIds = getProjectsIds(ctx);
        if (CollectionUtils.isEmpty(projectsIds)) {
            log.warn("Didn't find assets with type = {}", PROJECT);
            return new HashMap<>();
        }
        Map<AssetId, ListenableFuture<List<DeviceAndActive>>> devicesByProjectId = new HashMap<>();
        for (AssetId projectId : projectsIds) {
            devicesByProjectId.put(projectId, getDevicesAndActiveByProject(ctx, projectId));
        }
        return devicesByProjectId;
    }

    private ListenableFuture<List<DeviceAndActive>> getDevicesAndActiveByProject(TbContext ctx, AssetId projectId) {
        ListenableFuture<List<Device>> devicesFuture = ctx.getDeviceService()
                .findDevicesByQuery(ctx.getTenantId(), getDeviceSearchQuery(projectId));
        return Futures.transformAsync(devicesFuture, devices -> {
            if (CollectionUtils.isEmpty(devices)) {
                log.error("Didn't find devices for Project[{}]", projectId);
                return Futures.immediateFuture(new ArrayList<>());
            }
            List<ListenableFuture<DeviceAndActive>> deviceAndActiveList = new ArrayList<>();
            for (Device device : devices) {
                deviceAndActiveList.add(Futures.transform(ctx.getAttributesService()
                        .find(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, Collections.singletonList(ACTIVE_ATTR)), kvEntries -> {
                    if (!CollectionUtils.isEmpty(kvEntries)) {
                        return new DeviceAndActive(device, kvEntries.get(0).getBooleanValue().get());
                    }
                    return new DeviceAndActive(device, false);
                }, ctx.getDbCallbackExecutor()));
            }

            return Futures.allAsList(deviceAndActiveList);
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
        return  ctx.getAssetService()
                .findAssetsByTenantIdAndType(ctx.getTenantId(), PROJECT, new PageLink(1000))
                .getData()
                .stream()
                .map(Asset::getId)
                .collect(Collectors.toList());
    }

    @Data
    private static class EntityGroupDevicesAndActives {
        private EntityGroup entityGroup;
        private List<DeviceAndActive> deviceAndActives;
    }

    @Data
    @AllArgsConstructor
    private static class DeviceAndActive {
        private Device device;
        private boolean active;
    }

    @Override
    public void destroy() {

    }
}
