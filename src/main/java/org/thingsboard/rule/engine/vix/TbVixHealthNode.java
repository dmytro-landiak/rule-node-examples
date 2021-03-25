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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.TbRelationTypes;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "vix health",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig",
        icon = "functions")
public class TbVixHealthNode implements TbNode {

    private static final int ENTITIES_LIMIT = 7000;
    private static final LongBinaryOperator longBinaryOperator = Long::sum;

    private static final String EMPTY_KEY = "empty";
    private static final String ACTIVE_TELEMETRY_KEY = "active";
    private static final String NOT_ACTIVE_KEY = "notActive";
    private static final String TOTAL_KEY = "total";
    private static final String OPERATIONAL_KEY = "operational";
    private static final String OUTAGES_KEY = "outages";
    private static final String OFFLINE = "offline";
    private static final String CRITICAL = "critical";
    private static final String WARNING = "warning";
    private static final String GOOD = "good";
    private static final String ACTIVE_ATR = ACTIVE_TELEMETRY_KEY;
    private static final List<String> KEYS = new ArrayList<>(Arrays.asList(OFFLINE, CRITICAL, WARNING, GOOD, ACTIVE_ATR));

    private static final String IP_TYPE = "installationPoint";

    private static final String FROM_IP_TO_DEVICE_TYPE = "FromInstallationPointToDevice";
    private static final String FROM_LINE_TO_STATION_TYPE = "FromLineToStation";
    private static final String FROM_FLEET_TO_VEHICLE_TYPE = "FromFleetToVehicle";
    private static final String FROM_DEPOT_TO_AREA_TYPE = "FromDepotToArea";
    private static final String FROM_TM_TO_FLEET_TYPE = "FromTransitModeToFleet";
    private static final String FROM_TM_TO_LINE_TYPE = "FromTransitModeToLine";
    private static final String FROM_TM_TO_DEPOT_TYPE = "FromTransitModeToDepot";
    private static final String FROM_STATION_TO_IP_TYPE = "FromStationToInstallationPoint";
    private static final String FROM_VEHICLE_TO_IP_TYPE = "FromVehicleToInstallationPoint";
    private static final String FROM_AREA_TO_IP_TYPE = "FromAreaToInstallationPoint";
    private static final String FROM_CUSTOMER_TO_TM_TYPE = "FromCustomerToTransitMode";

    private final ConcurrentMap<EntityId, EntityContainer> entitiesMap = new ConcurrentHashMap<>();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        List<Asset> installationPoints = ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), IP_TYPE, new PageLink(ENTITIES_LIMIT)).getData();
        if (!installationPoints.isEmpty()) {
            List<ListenableFuture<Void>> ipFutures = new ArrayList<>();
            for (Asset installationPoint : installationPoints) {
                AtomicLong offlineCount = new AtomicLong(0);
                AtomicLong criticalCount = new AtomicLong(0);
                AtomicLong warningCount = new AtomicLong(0);
                AtomicLong goodCount = new AtomicLong(0);
                AtomicLong activeCount = new AtomicLong(0);
                AtomicLong notActiveCount = new AtomicLong(0);
                AtomicLong emptyCount = new AtomicLong(0);

                ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService()
                        .findByFromAndTypeAsync(ctx.getTenantId(), installationPoint.getId(), FROM_IP_TO_DEVICE_TYPE, RelationTypeGroup.COMMON);
                ipFutures.add(Futures.transformAsync(relationsFuture, relations -> {
                    ListenableFuture<Void> voidFuture;
                    if (relations != null && !relations.isEmpty()) {
                        voidFuture = processCountForDevices(ctx, offlineCount, criticalCount, warningCount,
                                goodCount, activeCount, notActiveCount, relations);
                    } else {
                        emptyCount.incrementAndGet();
                        voidFuture = Futures.immediateFuture(null);
                    }
                    entitiesMap.put(installationPoint.getId(),
                            new EntityContainer(
                                    VixEntity.IP,
                                    new HealthContainer(
                                            offlineCount,
                                            criticalCount,
                                            warningCount,
                                            goodCount,
                                            activeCount,
                                            notActiveCount,
                                            emptyCount),
                                    null));
                    return voidFuture;
                }, ctx.getDbCallbackExecutor()));
            }

            ListenableFuture<Void> entitiesResultFuture = Futures.transformAsync(Futures.allAsList(ipFutures), l -> {
                List<ListenableFuture<Void>> nextLevelEntities = new ArrayList<>();
                for (Asset installationPoint : installationPoints) {
                    ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService()
                            .findByQuery(ctx.getTenantId(), buildQuery(installationPoint.getId()));
                    nextLevelEntities.add(Futures.transform(relationsFuture, relations -> {
                        if (relations != null && !relations.isEmpty()) {
                            processCountForEntities(relations, installationPoint);
                        }
                        return null;
                    }, ctx.getDbCallbackExecutor()));
                }
                return Futures.transform(Futures.successfulAsList(nextLevelEntities), input -> null, ctx.getDbCallbackExecutor());
            }, ctx.getDbCallbackExecutor());

            DonAsynchron.withCallback(entitiesResultFuture, r -> {
                final AtomicLong outagesSum = new AtomicLong(0);
                final AtomicLong operationalSum = new AtomicLong(0);
                final AtomicLong totalSum = new AtomicLong(0);
                final AtomicLong activeSum = new AtomicLong(0);
                final AtomicLong notActiveSum = new AtomicLong(0);
                final AtomicLong emptySum = new AtomicLong(0);

                final HealthContainer healthContainerSum = new HealthContainer(
                        new AtomicLong(0),
                        new AtomicLong(0),
                        new AtomicLong(0),
                        new AtomicLong(0),
                        new AtomicLong(0),
                        new AtomicLong(0),
                        new AtomicLong(0));

                entitiesMap.forEach((entityId, entityContainer) -> {
                    saveAndNotify(ctx, msg, entityId, entityContainer.getHealthContainer());
                    saveAndNotify(ctx, msg, entityId, OUTAGES_KEY, getOutages(entityContainer.getHealthContainer()));
                    saveAndNotify(ctx, msg, entityId, OPERATIONAL_KEY, getOperational(entityContainer.getHealthContainer()));
                    saveAndNotify(ctx, msg, entityId, TOTAL_KEY, getTotal(entityContainer.getHealthContainer()));
                    saveAndNotify(ctx, msg, entityId, ACTIVE_TELEMETRY_KEY, entityContainer.getHealthContainer().getActive().get());
                    saveAndNotify(ctx, msg, entityId, NOT_ACTIVE_KEY, entityContainer.getHealthContainer().getNotActive().get());
                    saveAndNotify(ctx, msg, entityId, EMPTY_KEY, entityContainer.getHealthContainer().getEmpty().get());

                    if (entityContainer.getEntity() == VixEntity.CUSTOMER) {
                        outagesSum.addAndGet(getOutages(entityContainer.getHealthContainer()));
                        operationalSum.addAndGet(getOperational(entityContainer.getHealthContainer()));
                        totalSum.addAndGet(getTotal(entityContainer.getHealthContainer()));
                        activeSum.addAndGet(entityContainer.getHealthContainer().getActive().get());
                        notActiveSum.addAndGet(entityContainer.getHealthContainer().getNotActive().get());
                        emptySum.addAndGet(entityContainer.getHealthContainer().getEmpty().get());
                        accumulateAndGet(healthContainerSum, entityContainer.getHealthContainer());
                    }
                });

                saveAndNotify(ctx, msg, ctx.getTenantId(), OUTAGES_KEY, outagesSum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), OPERATIONAL_KEY, operationalSum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), TOTAL_KEY, totalSum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), ACTIVE_TELEMETRY_KEY, activeSum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), NOT_ACTIVE_KEY, notActiveSum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), EMPTY_KEY, emptySum.get());
                saveAndNotify(ctx, msg, ctx.getTenantId(), healthContainerSum);

                entitiesMap.clear();
            }, throwable -> log.error("Error during health counting!", throwable));

            ctx.tellNext(msg, TbRelationTypes.SUCCESS);
        } else {
            ctx.tellFailure(msg, new RuntimeException("No IPs found!"));
        }
    }

    @Override
    public void destroy() {

    }

    private void processCountForEntities(List<EntityRelation> relations, Asset installationPoint) {
        for (EntityRelation relation : relations) {
            EntityContainer nextLevelContainer = entitiesMap.computeIfAbsent(relation.getFrom(),
                    id -> new EntityContainer(
                            getEntityType(relation.getType()),
                            new HealthContainer(
                                    new AtomicLong(0),
                                    new AtomicLong(0),
                                    new AtomicLong(0),
                                    new AtomicLong(0),
                                    new AtomicLong(0),
                                    new AtomicLong(0),
                                    new AtomicLong(0)),
                            new CopyOnWriteArrayList<>()));

            EntityContainer ipContainer = entitiesMap.getOrDefault(installationPoint.getId(), null);
            if (ipContainer != null && !nextLevelContainer.getUsedEntityIds().contains(installationPoint.getId())) {
                nextLevelContainer.getUsedEntityIds().add(installationPoint.getId());
                accumulateAndGet(nextLevelContainer.getHealthContainer(), ipContainer.getHealthContainer());
            }
        }
    }

    private EntityRelationsQuery buildQuery(EntityId originator) {
        EntityRelationsQuery query = new EntityRelationsQuery();
        query.setFilters(constructEntityTypeFilters());
        query.setParameters(new RelationsSearchParameters(originator, EntitySearchDirection.TO, 5, false));
        return query;
    }

    private List<EntityTypeFilter> constructEntityTypeFilters() {
        List<EntityTypeFilter> entityTypeFilters = new ArrayList<>();
        entityTypeFilters.add(createTypeFilter(FROM_LINE_TO_STATION_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_FLEET_TO_VEHICLE_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_FLEET_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_LINE_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_STATION_TO_IP_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_VEHICLE_TO_IP_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_DEPOT_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_DEPOT_TO_AREA_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_AREA_TO_IP_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_CUSTOMER_TO_TM_TYPE, Collections.singletonList(EntityType.CUSTOMER)));
        return entityTypeFilters;
    }

    private EntityTypeFilter createTypeFilter(String relationType, List<EntityType> entityTypes) {
        return new EntityTypeFilter(relationType, entityTypes);
    }

    private ListenableFuture<Void> processCountForDevices(TbContext ctx, AtomicLong offlineCount, AtomicLong criticalCount,
                                                          AtomicLong warningCount, AtomicLong goodCount, AtomicLong activeCount,
                                                          AtomicLong notActiveCount, List<EntityRelation> relations) {
        List<ListenableFuture<Void>> futures = new ArrayList<>();
        for (EntityRelation relation : relations) {
            ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService()
                    .find(ctx.getTenantId(), relation.getTo(), DataConstants.SERVER_SCOPE, KEYS);
            futures.add(Futures.transform(attributesFuture, attributes -> {
                if (attributes != null && !attributes.isEmpty()) {
                    processCount(attributes, offlineCount, criticalCount, warningCount, goodCount, activeCount, notActiveCount);
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(futures), input -> null, ctx.getDbCallbackExecutor());
    }

    private void processCount(List<AttributeKvEntry> attributes, AtomicLong offlineCount, AtomicLong criticalCount,
                              AtomicLong warningCount, AtomicLong goodCount, AtomicLong activeCount, AtomicLong notActiveCount) {
        boolean offlineState = false;
        boolean criticalState = false;
        boolean warningState = false;
        boolean activeState = false;

        for (AttributeKvEntry attribute : attributes) {
            String key = attribute.getKey();
            if (key.equals(ACTIVE_ATR)) {
                activeState = attribute.getBooleanValue().orElse(false);
            } else {
                long value = attribute.getLongValue().orElse(0L);

                if (key.equals(OFFLINE) && value > 0) {
                    offlineState = true;
                } else if (key.equals(CRITICAL) && value > 0) {
                    criticalState = true;
                } else if (key.equals(WARNING) && value > 0) {
                    warningState = true;
                }
            }
        }
        if (offlineState) {
            offlineCount.incrementAndGet();
        } else if (criticalState) {
            criticalCount.incrementAndGet();
        } else if (warningState) {
            warningCount.incrementAndGet();
        } else {
            goodCount.incrementAndGet();
        }

        if (activeState) {
            activeCount.incrementAndGet();
        } else {
            notActiveCount.incrementAndGet();
        }
    }

    private List<AttributeKvEntry> constructAttrKvEntries(HealthContainer container) {
        List<AttributeKvEntry> attrKvEntries = new ArrayList<>();
        long ts = System.currentTimeMillis();
        attrKvEntries.add(createAttrKvEntry(OFFLINE, container.getOffline(), ts));
        attrKvEntries.add(createAttrKvEntry(CRITICAL, container.getCritical(), ts));
        attrKvEntries.add(createAttrKvEntry(WARNING, container.getWarning(), ts));
        attrKvEntries.add(createAttrKvEntry(GOOD, container.getGood(), ts));
        return attrKvEntries;
    }

    private BaseAttributeKvEntry createAttrKvEntry(String key, AtomicLong value, long ts) {
        return new BaseAttributeKvEntry(new LongDataEntry(key, value.get()), ts);
    }

    private long getOutages(HealthContainer healthContainer) {
        return healthContainer.getOffline().get() + healthContainer.getCritical().get();
    }

    private long getOperational(HealthContainer healthContainer) {
        return healthContainer.getGood().get() + healthContainer.getWarning().get();
    }

    private long getTotal(HealthContainer healthContainer) {
        return healthContainer.getGood().get() + healthContainer.getWarning().get()
                + healthContainer.getOffline().get() + healthContainer.getCritical().get();
    }

    private VixEntity getEntityType(String relationType) {
        if (relationType.equals(FROM_TM_TO_FLEET_TYPE) || relationType.equals(FROM_TM_TO_LINE_TYPE) || relationType.equals(FROM_TM_TO_DEPOT_TYPE)) {
            return VixEntity.TM;
        } else if (relationType.equals(FROM_CUSTOMER_TO_TM_TYPE)) {
            return VixEntity.CUSTOMER;
        }
        return VixEntity.OTHER;
    }

    private void accumulateAndGet(HealthContainer targetContainer, HealthContainer sourceContainer) {
        targetContainer.getOffline().accumulateAndGet(sourceContainer.getOffline().get(), longBinaryOperator);
        targetContainer.getCritical().accumulateAndGet(sourceContainer.getCritical().get(), longBinaryOperator);
        targetContainer.getWarning().accumulateAndGet(sourceContainer.getWarning().get(), longBinaryOperator);
        targetContainer.getGood().accumulateAndGet(sourceContainer.getGood().get(), longBinaryOperator);
        targetContainer.getActive().accumulateAndGet(sourceContainer.getActive().get(), longBinaryOperator);
        targetContainer.getNotActive().accumulateAndGet(sourceContainer.getNotActive().get(), longBinaryOperator);
        targetContainer.getEmpty().accumulateAndGet(sourceContainer.getEmpty().get(), longBinaryOperator);
    }

    private void saveAndNotify(TbContext ctx, TbMsg msg, EntityId entityId, String key, long value) {
        ctx.getTelemetryService().saveAndNotify(
                ctx.getTenantId(),
                entityId,
                Collections.singletonList(
                        new BasicTsKvEntry(System.currentTimeMillis(),
                                new LongDataEntry(key, value))),
                new VixNodeCallback(ctx, msg));
    }

    private void saveAndNotify(TbContext ctx, TbMsg msg, EntityId entityId, HealthContainer healthContainer) {
        ctx.getTelemetryService().saveAndNotify(
                ctx.getTenantId(),
                entityId,
                DataConstants.SERVER_SCOPE,
                constructAttrKvEntries(healthContainer),
                new VixNodeCallback(ctx, msg));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class EntityContainer {
        private VixEntity entity;
        private HealthContainer healthContainer;
        private List<EntityId> usedEntityIds;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class HealthContainer {
        private AtomicLong offline;
        private AtomicLong critical;
        private AtomicLong warning;
        private AtomicLong good;
        private AtomicLong active;
        private AtomicLong notActive;
        private AtomicLong empty;
    }
}
