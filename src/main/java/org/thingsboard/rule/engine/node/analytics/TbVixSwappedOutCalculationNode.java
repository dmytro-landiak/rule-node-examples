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
package org.thingsboard.rule.engine.node.analytics;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.thingsboard.rule.engine.node.conf.VixNodeCallback;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "vix swapped out calculation",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig",
        icon = "functions")
public class TbVixSwappedOutCalculationNode implements TbNode {

    private static final int ENTITIES_LIMIT = 100;
    private static final String TYPE = "swappedOutDevice";
    private static final String FROM_SO_TO_DEVICE_RELATION = "FromSwappedOutToDevice";
    private static final String FROM_TM_TO_SO_RELATION = "FromTransitModeToSwappedOut";
    private static final String FROM_CUSTOMER_TO_TM_TYPE = "FromCustomerToTransitMode";
    private static final String DISCONNECTED = "disconnected";

    private final ConcurrentMap<EntityId, AtomicInteger> entitiesMap = new ConcurrentHashMap<>();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        List<Asset> swappedOutList = ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), TYPE, new PageLink(ENTITIES_LIMIT)).getData();
        if (!CollectionUtils.isEmpty(swappedOutList)) {
            List<ListenableFuture<Void>> futures = new ArrayList<>();
            AtomicInteger totalSwappedOutCount = new AtomicInteger(0);
            for (Asset swappedOut : swappedOutList) {
                ListenableFuture<List<EntityRelation>> fromSoRelationsFuture = ctx.getRelationService().findByFromAndTypeAsync(ctx.getTenantId(),
                        swappedOut.getId(), FROM_SO_TO_DEVICE_RELATION, RelationTypeGroup.COMMON);
                ListenableFuture<Integer> countFuture = Futures.transform(fromSoRelationsFuture, relations -> {
                    int number = 0;
                    if (!CollectionUtils.isEmpty(relations)) {
                        number = relations.size();
                    }
                    totalSwappedOutCount.addAndGet(number);
                    saveAndNotify(ctx, msg, swappedOut.getId(), number);
                    return number;
                }, ctx.getDbCallbackExecutor());

                futures.add(Futures.transformAsync(countFuture, count -> {
                    ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService()
                            .findByQuery(ctx.getTenantId(), buildQuery(swappedOut.getId()));
                    return Futures.transform(relationsFuture, relations -> {
                        if (!CollectionUtils.isEmpty(relations)) {
                            for (EntityRelation relation : relations) {
                                if (relation.getType().equals(FROM_TM_TO_SO_RELATION)) {
                                    saveAndNotify(ctx, msg, relation.getFrom(), count);
                                } else {
                                    AtomicInteger atomicInteger = entitiesMap.computeIfAbsent(relation.getFrom(), id -> new AtomicInteger(0));
                                    atomicInteger.addAndGet(count);
                                }
                            }
                        }
                        return null;
                    }, ctx.getDbCallbackExecutor());
                }, ctx.getDbCallbackExecutor()));
            }
            DonAsynchron.withCallback(Futures.allAsList(futures),
                    voids -> {
                        for (ConcurrentMap.Entry<EntityId, AtomicInteger> entry : entitiesMap.entrySet()) {
                            saveAndNotify(ctx, msg, entry.getKey(), entry.getValue().get());
                        }
                        entitiesMap.clear();
                        saveAndNotify(ctx, msg, ctx.getTenantId(), totalSwappedOutCount.get());
                        ctx.tellSuccess(msg);
                    },
                    throwable -> ctx.tellFailure(msg, throwable));
        } else {
            ctx.tellFailure(msg, new RuntimeException("No Swapped Out found!"));
        }
    }

    @Override
    public void destroy() {

    }

    private void saveAndNotify(TbContext ctx, TbMsg msg, EntityId entityId, long value) {
        ctx.getTelemetryService().saveAndNotify(
                ctx.getTenantId(),
                entityId,
                DataConstants.SERVER_SCOPE,
                constructAttrKvEntries(value),
                new VixNodeCallback(ctx, msg));
    }

    private List<AttributeKvEntry> constructAttrKvEntries(long value) {
        return Collections.singletonList(createAttrKvEntry(value));
    }

    private BaseAttributeKvEntry createAttrKvEntry(long value) {
        return new BaseAttributeKvEntry(new LongDataEntry(DISCONNECTED, value), System.currentTimeMillis());
    }

    private EntityRelationsQuery buildQuery(EntityId originator) {
        EntityRelationsQuery query = new EntityRelationsQuery();
        query.setFilters(constructEntityTypeFilters());
        query.setParameters(new RelationsSearchParameters(originator, EntitySearchDirection.TO, 2, false));
        return query;
    }

    private List<EntityTypeFilter> constructEntityTypeFilters() {
        List<EntityTypeFilter> entityTypeFilters = new ArrayList<>();
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_SO_RELATION, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_CUSTOMER_TO_TM_TYPE, Collections.singletonList(EntityType.CUSTOMER)));
        return entityTypeFilters;
    }

    private EntityTypeFilter createTypeFilter(String relationType, List<EntityType> entityTypes) {
        return new EntityTypeFilter(relationType, entityTypes);
    }
}
