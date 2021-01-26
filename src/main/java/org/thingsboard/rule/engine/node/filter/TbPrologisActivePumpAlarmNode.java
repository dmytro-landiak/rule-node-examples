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
package org.thingsboard.rule.engine.node.filter;

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
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "prologis active pump ",
        configClazz = TbPrologisActivePumpAlarmNodeConfiguration.class,
        relationTypes = {"True", "False"},
        nodeDescription = "Filter by sum of chosen telemetry and period",
        nodeDetails = "Get sum of values for all doors in the zone (by originator) for chosen telemetry in specified period and filter by value(name = countOfMovement)." +
                "If the sum is greater than countOfMovement or equals to it -> the message goes through True chain. Otherwise, through False chain.",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisFilterActivePumpConfig")
public class TbPrologisActivePumpAlarmNode implements TbNode {

    private TbPrologisActivePumpAlarmNodeConfiguration config;

    private static final String OBSERVES_ZONE = "OBSERVES_ZONE";
    private static final String DOOR = "Door";

    private long interval;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisActivePumpAlarmNodeConfiguration.class);
        this.interval = config.getPeriodTimeUnit().toMillis(config.getPeriodValue());
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        EntityRelationsQuery toObservesZoneRelationsQuery
                = getEntityRelationsQuery(msg.getOriginator(), EntitySearchDirection.TO, EntityType.ASSET);
        ListenableFuture<List<EntityRelation>> toObservesZoneRelationsFuture
                = ctx.getRelationService().findByQuery(ctx.getTenantId(), toObservesZoneRelationsQuery);
        ListenableFuture<Long> sumOfTimeseriesFuture = Futures.transformAsync(toObservesZoneRelationsFuture, toObservesZoneRelations -> {
            if (!CollectionUtils.isEmpty(toObservesZoneRelations)) {
                return getTimeseriesForDevices(ctx, getDoorDevices(ctx, toObservesZoneRelations.get(0).getFrom()));
            } else {
                log.error("Didn't find relations with type = {} for device with id = {}", OBSERVES_ZONE, msg.getOriginator());
                return Futures.immediateFuture(null);
            }
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(sumOfTimeseriesFuture, sumOfTimeseries -> {
            if (sumOfTimeseries != null) {
                msg.getMetaData().putValue("movementCount", String.valueOf(sumOfTimeseries));
                if (sumOfTimeseries > config.getCountOfMovement()) {
                    ctx.tellNext(msg, "True");
                } else {
                    ctx.tellNext(msg, "False");
                }
            } else {
                ctx.ack(msg);
            }
        }, e -> ctx.tellFailure(msg, e));
    }

    private ListenableFuture<Long> getTimeseriesForDevices(TbContext ctx, ListenableFuture<List<Device>> doorDevicesFuture) {
        return Futures.transformAsync(doorDevicesFuture, doorDevices -> {
            if (CollectionUtils.isEmpty(doorDevices)) {
                log.error("Did't find {} devices", DOOR);
                return Futures.immediateFuture(null);
            }
            long endTs = System.currentTimeMillis();
            List<ListenableFuture<List<TsKvEntry>>> timeSeriesFutures = new ArrayList<>();
            for (Device door : doorDevices) {
                timeSeriesFutures.add(ctx.getTimeseriesService()
                        .findAll(ctx.getTenantId(), door.getId(),
                                Collections.singletonList(new BaseReadTsKvQuery(config.getTelemetryKey(),
                                        endTs - interval, endTs, interval, 1000, Aggregation.SUM))));
            }
            return Futures.transform(Futures.allAsList(timeSeriesFutures), listOfTsKvEntries -> {
                if (!CollectionUtils.isEmpty(listOfTsKvEntries)) {
                    long result = 0L;
                    for (List<TsKvEntry> tempList : listOfTsKvEntries) {
                        if (!CollectionUtils.isEmpty(tempList)) {
                            Optional<Long> longValue = tempList.get(0).getLongValue();
                            if (longValue.isPresent()) {
                                result += longValue.get();
                            }
                        }
                    }
                    return result;
                }
                log.info("Didn't find timeseries by key = {}", config.getTelemetryKey());
                return 0L;
            }, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<Device>> getDoorDevices(TbContext ctx, EntityId zoneId) {
        EntityRelationsQuery fromObservesZoneRelationsQuery = getEntityRelationsQuery(zoneId, EntitySearchDirection.FROM, EntityType.DEVICE);
        ListenableFuture<List<EntityRelation>> fromObservesZoneRelationsFuture
                = ctx.getRelationService().findByQuery(ctx.getTenantId(), fromObservesZoneRelationsQuery);
        return Futures.transformAsync(fromObservesZoneRelationsFuture, fromObservesZoneRelations -> {
            if (!CollectionUtils.isEmpty(fromObservesZoneRelations)) {
                List<DeviceId> deviceIds = fromObservesZoneRelations.stream()
                        .map(EntityRelation::getTo)
                        .map(entityId -> new DeviceId(entityId.getId()))
                        .collect(Collectors.toList());
                ListenableFuture<List<Device>> devicesFuture = ctx.getDeviceService()
                        .findDevicesByTenantIdAndIdsAsync(ctx.getTenantId(), deviceIds);
                return Futures.transform(devicesFuture, devices -> {
                    if (!CollectionUtils.isEmpty(devices)) {
                        return devices.stream()
                                .filter(device -> device.getLabel() != null
                                        && device.getLabel().toLowerCase().contains(DOOR.toLowerCase()))
                                .collect(Collectors.toList());
                    }
                    return null;
                }, ctx.getDbCallbackExecutor());
            } else {
                log.error("Didn't find devices for Zone[id = {}] by relation = {}", zoneId, OBSERVES_ZONE);
                return Futures.immediateFuture(null);
            }
        }, ctx.getDbCallbackExecutor());
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId,
                                                         EntitySearchDirection searchDirection, EntityType entityType) {
        RelationsSearchParameters relationsSearchParameters
                = new RelationsSearchParameters(originatorId, searchDirection, 1, true);
        EntityTypeFilter entityTypeFilter = new EntityTypeFilter(OBSERVES_ZONE, Collections.singletonList(entityType));
        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(Collections.singletonList(entityTypeFilter));
        return entityRelationsQuery;
    }

    @Override
    public void destroy() {

    }
}
