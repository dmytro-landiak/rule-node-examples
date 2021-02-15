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
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.asset.AssetSearchQuery;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis calculate dew point",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisCalculateDewPointNode implements TbNode {

    private static final String DOCK_PROJECTS = "DockProjects";
    private static final String IS_IN_SPACE = "IS_IN_SPACE";
    private static final String IS_IN_ZONE = "IS_IN_ZONE";
    private static final String ZONE = "ZONE";
    private static final String LEVELER = "leveler";
    private static final String EQUIPMENT = "EQUIPMENT";
    private static final String TEMPERATURE = "TEMPERATURE_VALUE";
    private static final String HUMIDITY = "HUMIDITY_VALUE";
    private static final String DOCK_DOOR = "dock door";
    private static final String OBSERVES_EQUIPMENT = "OBSERVES_EQUIPMENT";
    private static final String OBSERVES_STRUCTURE = "OBSERVES_STRUCTURE";
    private static final String ENVIRONMENT = "environment";
    private static final String STRUCTURE = "STRUCTURE";
    private static final String LEVELER_TEMPERATURE_KEY = "levelerTemperatureValue";
    private static final String DEW_POINT_KEY = "dewPointValue";
    private static final String MODEL = "MODEL";
    private static final List<String> DOCK_LABELS = Arrays.asList("dock", "dok");

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ListenableFuture<Optional<EntityGroup>> dockProjectsGroupFuture = ctx.getPeContext()
                .getEntityGroupService()
                .findEntityGroupByTypeAndName(ctx.getTenantId(), ctx.getTenantId(), EntityType.ASSET, DOCK_PROJECTS);
        ListenableFuture<Integer> resultsFuture = Futures.transformAsync(dockProjectsGroupFuture, dockProjectsGroup -> {
            if (dockProjectsGroup != null && dockProjectsGroup.isPresent()) {
                List<Asset> projects = ctx.getAssetService()
                        .findAssetsByEntityGroupId(dockProjectsGroup.get().getId(), new PageLink(1000))
                        .getData();
                List<ListenableFuture<List<DockInfo>>> dockInfosFutures = new ArrayList<>();
                for (Asset project : projects) {
                    dockInfosFutures.add(getDockInfos(ctx, project));
                }
                return Futures.transformAsync(Futures.allAsList(dockInfosFutures), dockInfosList -> {
                    if (!CollectionUtils.isEmpty(dockInfosList)) {
                        List<DockInfo> filteredDockInfos = dockInfosList.stream()
                                .filter(dockInfos -> !CollectionUtils.isEmpty(dockInfos))
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                        return saveTimeseries(ctx, filteredDockInfos, msg.getTs());
                    }
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor());
            }
            log.error("Did not find {} entity group", DOCK_PROJECTS);
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(resultsFuture, r -> ctx.tellSuccess(msg), throwable -> ctx.tellFailure(msg, throwable));
    }

    private ListenableFuture<Integer> saveTimeseries(TbContext ctx, List<DockInfo> dockInfos, long ts) {
        ts = ts != 0 ? ts : System.currentTimeMillis();
        List<ListenableFuture<Integer>> futures = new ArrayList<>();
        for (DockInfo dockInfo : dockInfos) {
            List<TsKvEntry> tsKvEntries = new ArrayList<>();
            if (dockInfo.getDewPoint() != null) {
                tsKvEntries.add(new BasicTsKvEntry(ts, new DoubleDataEntry(DEW_POINT_KEY, dockInfo.getDewPoint())));
            }
            if (dockInfo.getLevelerTemperature() != null) {
                tsKvEntries.add(new BasicTsKvEntry(ts, new DoubleDataEntry(LEVELER_TEMPERATURE_KEY, dockInfo.getLevelerTemperature())));
            }
            if (!CollectionUtils.isEmpty(tsKvEntries)) {
                futures.add(ctx.getTimeseriesService()
                        .save(ctx.getTenantId(), dockInfo.getDockId(), tsKvEntries, 0L));
            }
        }
        return Futures.transform(Futures.allAsList(futures), i -> null, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<DockInfo>> getDockInfos(TbContext ctx, Asset project) {
        return Futures.transformAsync(ctx.getAssetService()
                        .findAssetsByQuery(ctx.getTenantId(), getAssetSearchQuery(project.getId(), IS_IN_SPACE, 10, Collections.singletonList(ZONE))),
                zones -> {
                    if (!CollectionUtils.isEmpty(zones)) {
                        List<Asset> docks = zones.stream()
                                .filter(zone -> zone.getLabel() != null
                                        && DOCK_LABELS.stream()
                                        .anyMatch(dock -> zone.getLabel().toLowerCase().contains(dock)))
                                .collect(Collectors.toList());
                        List<ListenableFuture<DockInfo>> dockInfoFutures = new ArrayList<>();
                        for (Asset dock : docks) {
                            ListenableFuture<Double> dewPointFuture = calculateDewPoint(ctx, dock);
                            ListenableFuture<Double> levelerTemperatureFuture = getLevelerTemperature(ctx, dock);
                            dockInfoFutures.add(Futures.transformAsync(dewPointFuture,
                                    dewPoint -> Futures.transform(levelerTemperatureFuture,
                                            levelerTemperature -> new DockInfo(dock.getId(), dewPoint, levelerTemperature),
                                            ctx.getDbCallbackExecutor()),
                                    ctx.getDbCallbackExecutor()));
                        }
                        return Futures.allAsList(dockInfoFutures);
                    }
                    log.warn("Project[{}]: Didn't find any zone!", project.getName());
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Double> getLevelerTemperature(TbContext ctx, Asset dock) {
        return Futures.transformAsync(ctx.getAssetService()
                .findAssetsByQuery(ctx.getTenantId(),
                        getAssetSearchQuery(dock.getId(), IS_IN_ZONE, 1, Collections.singletonList(EQUIPMENT))), equipments -> {
            if (!CollectionUtils.isEmpty(equipments)) {
                Optional<Asset> leveler = equipments.stream()
                        .filter(asset -> asset.getLabel() != null && asset.getLabel().toLowerCase().contains(LEVELER))
                        .findFirst();
                if (leveler.isPresent()) {
                    return Futures.transformAsync(getEnvironmentalSensorId(ctx, leveler.get(), OBSERVES_EQUIPMENT), environmentalSensors -> {
                        if (!CollectionUtils.isEmpty(environmentalSensors)) {
                            return Futures.transform(ctx.getTimeseriesService()
                                    .findLatest(ctx.getTenantId(),
                                            environmentalSensors.get(0), Collections.singletonList(TEMPERATURE)), kvEntries -> {
                                if (!CollectionUtils.isEmpty(kvEntries) && kvEntries.stream().allMatch(tsKvEntry -> tsKvEntry.getValue() != null)) {
                                    return Double.parseDouble(kvEntries.get(0).getValueAsString());
                                }
                                log.warn("Device[id = {}]: Can't get temperature from [{}]", environmentalSensors.get(0), kvEntries);
                                return null;
                            }, ctx.getDbCallbackExecutor());
                        }
                        log.warn("Leveler[{}]: Didn't find any environmental devices", leveler.get().getName());
                        return Futures.immediateFuture(null);
                    }, ctx.getDbCallbackExecutor());
                }
                log.warn("Dock[{}]: Didn't find any leveler", dock.getId());
            } else {
                log.warn("Dock[{}]: Didn't find any EQUIPMENTS", dock.getId());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Double> calculateDewPoint(TbContext ctx, Asset dock) {
        return Futures.transformAsync(ctx.getAssetService()
                .findAssetsByQuery(ctx.getTenantId(),
                        getAssetSearchQuery(dock.getId(), IS_IN_ZONE, 1, Collections.singletonList(STRUCTURE))), structures -> {
            if (!CollectionUtils.isEmpty(structures)) {
                Optional<Asset> dockDoorOpt = structures.stream()
                        .filter(asset -> asset.getLabel() != null && asset.getLabel().toLowerCase().contains(DOCK_DOOR))
                        .findFirst();
                if (dockDoorOpt.isPresent()) {
                    return Futures.transformAsync(getEnvironmentalSensorId(ctx, dockDoorOpt.get(), OBSERVES_STRUCTURE), environmentalSensors -> {
                        if (!CollectionUtils.isEmpty(environmentalSensors)) {
                            return calculateDewPoint(ctx, environmentalSensors.get(0));
                        }
                        return Futures.immediateFuture(null);
                    }, ctx.getDbCallbackExecutor());
                }
                log.warn("Dock[{}]: Didn't find any dock door", dock.getId());
            } else {
                log.warn("Dock[{}]: Didn't find any STRUCTURES", dock.getId());
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<DeviceId>> getEnvironmentalSensorId(TbContext ctx, Asset asset, String relationType) {
        List<DeviceId> deviceIds = ctx.getRelationService()
                .findByFromAndType(ctx.getTenantId(), asset.getId(), relationType, RelationTypeGroup.COMMON)
                .stream()
                .filter(relation -> relation.getTo().getEntityType().equals(EntityType.DEVICE))
                .map(relation -> new DeviceId(relation.getTo().getId()))
                .collect(Collectors.toList());
        List<ListenableFuture<DeviceId>> environmentDeviceIdFutures = new ArrayList<>();
        for (DeviceId deviceId : deviceIds) {
            environmentDeviceIdFutures.add(Futures.transform(ctx.getAttributesService()
                    .find(ctx.getTenantId(), deviceId, DataConstants.SERVER_SCOPE, MODEL), modelAttrOpt -> {
                if (modelAttrOpt != null && modelAttrOpt.isPresent()) {
                    if (modelAttrOpt.get().getValueAsString().toLowerCase().contains(ENVIRONMENT)) {
                        return deviceId;
                    }
                } else {
                    log.warn("Device[id = {}]: Didn't find MODEL attribute", deviceId);
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(environmentDeviceIdFutures), environmentDeviceIds -> {
            if (!CollectionUtils.isEmpty(environmentDeviceIds)) {
                return environmentDeviceIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Double> calculateDewPoint(TbContext ctx, DeviceId environmentalSensorId) {
        return Futures.transform(ctx.getTimeseriesService()
                .findLatest(ctx.getTenantId(),
                        environmentalSensorId, Arrays.asList(TEMPERATURE, HUMIDITY)), tsKvEntries -> {
            if (!CollectionUtils.isEmpty(tsKvEntries)
                    && tsKvEntries.size() == 2
                    && tsKvEntries.stream().allMatch(tsKvEntry -> tsKvEntry.getValue() != null)) {
                Optional<String> temperatureStr = tsKvEntries.stream()
                        .filter(tsKvEntry -> tsKvEntry.getKey().equals(TEMPERATURE))
                        .map(KvEntry::getValueAsString)
                        .findFirst();
                Optional<String> humidityStr = tsKvEntries.stream()
                        .filter(tsKvEntry -> tsKvEntry.getKey().equals(HUMIDITY))
                        .map(KvEntry::getValueAsString)
                        .findFirst();
                if (temperatureStr.isPresent() && humidityStr.isPresent()) {
                    double humidity = Double.parseDouble(humidityStr.get());
                    double temperature = Double.parseDouble(temperatureStr.get());
                    return 243.04 * (Math.log(humidity / 100) + ((17.625 * temperature) / (243.04 + temperature))) / (17.625 - Math.log10(humidity / 100) - ((17.625 * temperature) / (243.04 + temperature)));
                }
            }
            log.warn("Device[id = {}]: Can't calculate dew point for values [{}]", environmentalSensorId, tsKvEntries);
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private AssetSearchQuery getAssetSearchQuery(EntityId entityId, String relationType, int maxLevel, List<String> assetTypes) {
        AssetSearchQuery query = new AssetSearchQuery();
        query.setRelationType(relationType);
        query.setAssetTypes(assetTypes);
        query.setParameters(new RelationsSearchParameters(entityId, EntitySearchDirection.FROM, maxLevel, false));
        return query;
    }

    @Override
    public void destroy() {

    }

    @Data
    @AllArgsConstructor
    private static class DockInfo {
        private AssetId dockId;
        private Double dewPoint;
        private Double levelerTemperature;
    }
}
