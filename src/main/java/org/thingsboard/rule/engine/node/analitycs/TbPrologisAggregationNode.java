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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonObject;
import lombok.Data;
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
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.device.DeviceSearchQuery;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.DataType;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.page.SortOrder;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "prologis aggregation",
        configClazz = TbPrologisAggregationNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisAnalyticsAggregationNodeConfig",
        icon = "functions"
)
public class TbPrologisAggregationNode implements TbNode {

    private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final List<String> keys = new ArrayList<>(Arrays.asList("TEMPERATURE_VALUE", "HUMIDITY_VALUE", "AIRPRESSURE_VALUE",
            "LIGHT_VALUE", "VIBRATION_ENERGY_LEVEL_VALUE", "NOISE_VALUE", "PRESENCE_MOVE_COUNT_VALUE"));

    private static final String TB_MSG_CUSTOM_NODE_MSG = "TbMsgCustomNodeMsg";
    private static final String DOCK_PROJECTS = "DockProjects";
    private static final String DATA_DEVICE_TYPE = "DATA_DEVICE";
    private static final String IS_IN_SPACE = "IS_IN_SPACE";
    private static final String COLUMN_NAME_ATTR = "columnName";
    private static final String KEY_ENDING = "AvgHeatMap";
    private static final String SERVICE_ID = "tb-node-0";

    private static final int ENTITIES_LIMIT = 100;
    private static final int TELEMETRY_LIMIT = 1000;
    private static final long ONE_HOUR_MS = 3600 * 1000;

    private ScheduledExecutorService scheduledExecutor;
    private TbPrologisAggregationNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisAggregationNodeConfiguration.class);
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

        long currTsMs = System.currentTimeMillis();
        long configTsMs = getTimeOfExecution();

        if (currTsMs <= configTsMs) {
            scheduledExecutor.scheduleAtFixedRate(() -> runTask(ctx),
                    configTsMs - currTsMs, ONE_HOUR_MS, TimeUnit.MILLISECONDS);
        } else {
            scheduledExecutor.scheduleAtFixedRate(() -> runTask(ctx),
                    getInitDelayMs(currTsMs, configTsMs), ONE_HOUR_MS, TimeUnit.MILLISECONDS);
        }
    }

    private long getInitDelayMs(long currTsMs, long configTsMs) {
        long diff = getMillisForTime(currTsMs) - getMillisForTime(configTsMs);
        if (diff < 0) {
            return Math.abs(diff);
        }
        return ONE_HOUR_MS - diff;
    }

    private long getMillisForTime(long unixTimestampInMillis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimestampInMillis), ZoneId.systemDefault());
        return (ldt.getMinute() * 60 + ldt.getSecond()) * 1000;
    }

    private long getTimeOfExecution() {
        try {
            return defaultDateFormat.parse(this.config.getTimeOfExecution()).getTime();
        } catch (ParseException e) {
            log.error("Failed to parse date: {}", this.config.getTimeOfExecution(), e);
        }
        return System.currentTimeMillis();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (msg.getType().equals(TB_MSG_CUSTOM_NODE_MSG)) {
            ctx.enqueueForTellNext(msg, TbRelationTypes.SUCCESS,
                    () -> log.info("Successfully put message into queue!"),
                    throwable -> log.error("Failed to put message into queue!", throwable));
        } else {
            ctx.tellSuccess(msg);
        }
    }

    private void runTask(TbContext ctx) {
        if (!ctx.getServiceId().equals(SERVICE_ID)) {
            return;
        }
        LocalDateTime currentDateTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        long tsAtStartOfDay = currentDateTime.toLocalDate().atStartOfDay().atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
        long startTs = (currentDateTime.getHour() - 1) * 3600 * 1000 + tsAtStartOfDay;
        long endTs = currentDateTime.getHour() * 3600 * 1000 + tsAtStartOfDay;
        log.info("[{}][{}] Started calculating averages...", startTs, endTs);

        ListenableFuture<Void> resultFuture = Futures.transformAsync(getProjects(ctx), projects -> {
            List<ListenableFuture<List<Void>>> resultFutures = new ArrayList<>();
            if (!CollectionUtils.isEmpty(projects)) {
                for (Asset project : projects) {
                    resultFutures.add(Futures.transformAsync(getRelatedDevices(ctx, project.getId()), devices -> {
                        if (!CollectionUtils.isEmpty(devices)) {
                            log.info("[{}] Found {} devices for project!", project.getName(), devices.size());

                            return Futures.transformAsync(getTargetDevices(ctx, devices), targetDevices -> {
                                if (!CollectionUtils.isEmpty(targetDevices)) {
                                    log.info("[{}] Found {} target devices for project!", project.getName(), targetDevices.size());

                                    List<ListenableFuture<Void>> futures = new ArrayList<>();
                                    for (String key : keys) {
                                        log.info("[{}] Started calculation for key {}!", project.getName(), key);
                                        futures.add(Futures.transform(getDeviceAvgs(ctx, targetDevices, key, startTs, endTs), deviceAvgs -> {
                                            if (!CollectionUtils.isEmpty(deviceAvgs)) {
                                                log.info("[{}][{}] Found {} device averages for project!", project.getName(), key, deviceAvgs.size());
                                                JsonObject jo = new JsonObject();
                                                for (DeviceAvg deviceAvg : deviceAvgs) {
                                                    jo.addProperty(deviceAvg.getDeviceId().toString(), deviceAvg.getValue());
                                                }
                                                saveTelemetry(ctx, project, endTs, key, jo);
                                            } else {
                                                log.info("[{}][{}] Did not find device averages for project!", project.getName(), key);
                                            }
                                            return null;
                                        }, ctx.getDbCallbackExecutor()));
                                    }
                                    return Futures.allAsList(futures);
                                }
                                return Futures.immediateFuture(null);
                            }, ctx.getDbCallbackExecutor());
                        }
                        log.warn("[{}] Did not find any device for project!", project.getName());
                        return Futures.immediateFuture(null);
                    }, ctx.getDbCallbackExecutor()));
                }
            }
            return Futures.transform(Futures.allAsList(resultFutures), v -> null, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());

        DonAsynchron.withCallback(resultFuture,
                aVoid -> log.info("Finished calculating averages..."),
                throwable -> log.error("Failed to calculate averages...", throwable));
    }

    private ListenableFuture<Set<Device>> getTargetDevices(TbContext ctx, List<Device> devices) {
        ListenableFuture<List<Device>> targetDevicesByAttrFuture = getTargetDevicesByAttr(ctx, devices);
        ListenableFuture<List<Device>> targetDevicesByRelationsFuture = getTargetDevicesByRelations(ctx, devices);

        return Futures.transform(Futures.allAsList(targetDevicesByAttrFuture, targetDevicesByRelationsFuture), targetDevicesLists -> {
            if (!CollectionUtils.isEmpty(targetDevicesLists)) {
                return targetDevicesLists.stream().flatMap(List::stream).collect(Collectors.toSet());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private void saveTelemetry(TbContext ctx, Asset project, long ts, String key, JsonObject jo) {
        log.info("[{}] Trying to save {}!", project.getName(), key);
        ctx.getTelemetryService().saveAndNotify(
                ctx.getTenantId(),
                project.getId(),
                Collections.singletonList(new BasicTsKvEntry(ts, new JsonDataEntry(getKey(key), jo.toString()))),
                new TelemetryNodeCallback(project.getName(), key));
    }

    private String getKey(String key) {
        return key.toLowerCase().replace("_value", KEY_ENDING);
    }

    private ListenableFuture<List<DeviceAvg>> getDeviceAvgs(TbContext ctx, Set<Device> targetDevices, String key, long startTs, long endTs) {
        List<ListenableFuture<DeviceAvg>> devicesAvgFuture = new ArrayList<>();
        for (Device targetDevice : targetDevices) {
            devicesAvgFuture.add(Futures.transform(getAvg(ctx, targetDevice, key, startTs, endTs), tsKvEntries -> {
                if (!CollectionUtils.isEmpty(tsKvEntries)) {
                    return getDeviceAvg(targetDevice, tsKvEntries);
                } else {
                    log.info("[{}][{}] Did not find calculated average!", targetDevice.getName(), key);
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(devicesAvgFuture), deviceAvgs -> {
            if (!CollectionUtils.isEmpty(deviceAvgs)) {
                return deviceAvgs.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private DeviceAvg getDeviceAvg(Device targetDevice, List<TsKvEntry> tsKvEntries) {
        TsKvEntry entry = tsKvEntries.get(0);
        if (entry.getDataType().equals(DataType.DOUBLE) && entry.getDoubleValue().isPresent()) {
            return new DeviceAvg(targetDevice.getId(), entry.getDoubleValue().get());
        } else if (entry.getDataType().equals(DataType.LONG) && entry.getLongValue().isPresent()) {
            return new DeviceAvg(targetDevice.getId(), entry.getLongValue().get());
        } else {
            log.warn("[{}][{}] Unknown data type result for average!", targetDevice.getName(), entry.getKey());
        }
        return null;
    }

    private ListenableFuture<List<TsKvEntry>> getAvg(TbContext ctx, Device device, String key, long startTs, long endTs) {
        return ctx.getTimeseriesService().findAll(
                ctx.getTenantId(),
                device.getId(),
                Collections.singletonList(
                        new BaseReadTsKvQuery(key, startTs, endTs, ONE_HOUR_MS, TELEMETRY_LIMIT, Aggregation.AVG, SortOrder.Direction.DESC.name())));
    }

    private ListenableFuture<List<Device>> getTargetDevicesByAttr(TbContext ctx, List<Device> devices) {
        List<ListenableFuture<Device>> targetDevicesFuture = new ArrayList<>();
        for (Device device : devices) {
            targetDevicesFuture.add(Futures.transform(getAttributes(ctx, device.getId()), attributeKvEntries -> {
                if (!CollectionUtils.isEmpty(attributeKvEntries)) {
                    return device;
                } else {
                    log.info("[{}] {} attribute is not found for device!", device.getName(), COLUMN_NAME_ATTR);
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(targetDevicesFuture), targetDevices -> {
            if (!CollectionUtils.isEmpty(targetDevices)) {
                return targetDevices.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<Device>> getTargetDevicesByRelations(TbContext ctx, List<Device> devices) {
        List<ListenableFuture<Device>> targetDevicesByRelationsFuture = new ArrayList<>();
        for (Device device : devices) {
            targetDevicesByRelationsFuture.add(Futures.transform(findRelationsByQuery(ctx, device.getId()), entityRelations -> {
                if (!CollectionUtils.isEmpty(entityRelations)) {
                    return device;
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(targetDevicesByRelationsFuture), targetDevices -> {
            if (!CollectionUtils.isEmpty(targetDevices)) {
                return targetDevices.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<EntityRelation>> findRelationsByQuery(TbContext ctx, DeviceId deviceId) {
        return ctx.getRelationService().findByQuery(ctx.getTenantId(), getEntityRelationsQuery(deviceId));
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId) {
        RelationsSearchParameters relationsSearchParameters = new RelationsSearchParameters(originatorId,
                EntitySearchDirection.TO, 1, false);
        List<EntityTypeFilter> entityTypeFilters = new ArrayList<>(Arrays.asList(
                new EntityTypeFilter("buildingToVibrationSensor", Collections.singletonList(EntityType.ASSET)),
                new EntityTypeFilter("buildingToPresenceSensor", Collections.singletonList(EntityType.ASSET)),
                new EntityTypeFilter("buildingToNoiseSensor", Collections.singletonList(EntityType.ASSET))
        ));
        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(entityTypeFilters);
        return entityRelationsQuery;
    }

    private ListenableFuture<List<AttributeKvEntry>> getAttributes(TbContext ctx, DeviceId deviceId) {
        return ctx.getAttributesService().find(ctx.getTenantId(), deviceId, DataConstants.SERVER_SCOPE, Collections.singletonList(COLUMN_NAME_ATTR));
    }

    private ListenableFuture<List<Device>> getRelatedDevices(TbContext ctx, AssetId projectId) {
        return ctx.getDeviceService().findDevicesByQuery(ctx.getTenantId(), getDeviceSearchQuery(projectId));
    }

    private DeviceSearchQuery getDeviceSearchQuery(AssetId projectId) {
        DeviceSearchQuery deviceSearchQuery = new DeviceSearchQuery();
        deviceSearchQuery.setParameters(new RelationsSearchParameters(projectId, EntitySearchDirection.FROM, 10, false));
        deviceSearchQuery.setDeviceTypes(Collections.singletonList(DATA_DEVICE_TYPE));
        deviceSearchQuery.setRelationType(IS_IN_SPACE);
        return deviceSearchQuery;
    }

    private ListenableFuture<List<Asset>> getProjects(TbContext ctx) {
        return Futures.transformAsync(getDockProjectsGroup(ctx), optionalEntityGroup -> {
            if (optionalEntityGroup != null && optionalEntityGroup.isPresent()) {
                ListenableFuture<List<EntityId>> entitiesFuture = ctx.getPeContext().getEntityGroupService()
                        .findAllEntityIds(ctx.getTenantId(), optionalEntityGroup.get().getId(), new PageLink(ENTITIES_LIMIT));
                return Futures.transformAsync(entitiesFuture, entityIds -> {
                    if (!CollectionUtils.isEmpty(entityIds)) {
                        log.info("Found {} projects in group!", entityIds.size());
                        return ctx.getAssetService().findAssetsByTenantIdAndIdsAsync(ctx.getTenantId(), getAssetIds(entityIds));
                    }
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Optional<EntityGroup>> getDockProjectsGroup(TbContext ctx) {
        return ctx.getPeContext().getEntityGroupService()
                .findEntityGroupByTypeAndName(ctx.getTenantId(), ctx.getTenantId(), EntityType.ASSET, DOCK_PROJECTS);
    }

    private List<AssetId> getAssetIds(List<EntityId> entityIds) {
        return entityIds.stream().map(entityId -> new AssetId(entityId.getId())).collect(Collectors.toList());
    }

    @Override
    public void destroy() {
        if (this.scheduledExecutor != null) {
            this.scheduledExecutor.shutdown();
        }
    }

    @Data
    private static class DeviceAvg {
        private final DeviceId deviceId;
        private final double value;
    }

    @Data
    private static class TelemetryNodeCallback implements FutureCallback<Void> {
        private final String building;
        private final String key;

        @Override
        public void onSuccess(@Nullable Void result) {
            log.info("[{}][{}] Saved avg values for building!", building, key);
        }

        @Override
        public void onFailure(Throwable throwable) {
            log.error("[{}][{}] Failed to save avg values for building!", building, key, throwable);
        }
    }
}
