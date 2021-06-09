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
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.device.DeviceSearchQuery;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis create relations",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisCreateRelationsNodeConfig")
public class TbPrologisCreateRelationsNode implements TbNode {

    private static final String DOCK_PROJECTS_GROUP_NAME = "DockProjects";

    private static final List<String> KEYS = Arrays.asList("LIGHT_VALUE", "VIBRATION_ENERGY_LEVEL_VALUE", "PRESENCE_MOVE_COUNT_VALUE", "NOISE_VALUE");

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ListenableFuture<List<Boolean>> resFuture = Futures.transformAsync(ctx.getPeContext()
                .getEntityGroupService()
                .findEntityGroupByTypeAndName(ctx.getTenantId(), ctx.getTenantId(), EntityType.ASSET, DOCK_PROJECTS_GROUP_NAME), entityGroupOpt -> {
            if (entityGroupOpt != null && entityGroupOpt.isPresent()) {
                List<Asset> assets = ctx.getAssetService()
                        .findAssetsByEntityGroupId(entityGroupOpt.get().getId(), new PageLink(1000))
                        .getData();
                List<ListenableFuture<List<Boolean>>> futures = new ArrayList<>();
                for (Asset asset : assets) {
                    futures.add(createRelations(ctx, asset));
                }
                return Futures.transform(Futures.allAsList(futures), booleansList -> {
                    if (booleansList == null) {
                        return null;
                    }
                    return booleansList.stream().flatMap(Collection::stream).collect(Collectors.toList());
                }, ctx.getDbCallbackExecutor());
            } else {
                log.warn("Did not find {} entity group", DOCK_PROJECTS_GROUP_NAME);
                return Futures.immediateFuture(null);
            }
        }, ctx.getDbCallbackExecutor());

        DonAsynchron.withCallback(resFuture, list -> {
                    if (list == null) {
                        ctx.tellFailure(msg, new TbNodeException("Failed to create relations"));
                    } else {
                        ctx.tellSuccess(msg);
                    }
                },
                throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }

    private ListenableFuture<List<Boolean>> createRelations(TbContext ctx, Asset asset) {
        return Futures.transformAsync(ctx.getDeviceService()
                .findDevicesByQuery(ctx.getTenantId(), getDeviceSearchQuery(asset.getId())), devices -> {
            List<ListenableFuture<List<Boolean>>> createRelationsFuturesList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(devices)) {
                for (Device device : devices) {
                    createRelationsFuturesList.add(Futures.transformAsync(getTelemetryKeys(ctx, device.getId()), keys -> {
                        List<ListenableFuture<Boolean>> createRelationFutures = new ArrayList<>();
                        for (String key : KEYS.stream().filter(keys::contains).collect(Collectors.toList())) {
                            EntityRelation relation = getEntityRelation(asset.getId(), device.getId(), key);
                            createRelationFutures.add(Futures.transform(ctx.getRelationService().
                                    saveRelationAsync(ctx.getTenantId(), relation), b -> {
                                if (b == null || !b) {
                                    log.warn("Failed to create relation {}", relation);
                                }
                                return b;
                            }, ctx.getDbCallbackExecutor()));
                        }
                        return Futures.allAsList(createRelationFutures);
                    }, ctx.getDbCallbackExecutor()));

                }
            }
            return Futures.transform(Futures.allAsList(createRelationsFuturesList), res -> {
                if (CollectionUtils.isEmpty(res)) {
                    return new ArrayList<>();
                }
                return res.stream().flatMap(Collection::stream).collect(Collectors.toList());
            }, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Set<String>> getTelemetryKeys(TbContext ctx, EntityId entityId) {
        return Futures.transform(ctx.getTimeseriesService().findAllLatest(ctx.getTenantId(), entityId), kvEntryList -> {
            if (CollectionUtils.isEmpty(kvEntryList)) {
                return new HashSet<>();
            }
            return kvEntryList.stream().map(KvEntry::getKey).collect(Collectors.toSet());
        }, ctx.getDbCallbackExecutor());
    }

    private EntityRelation getEntityRelation(EntityId fromId, EntityId toId, String key) {
        EntityRelation relation = new EntityRelation();
        relation.setFrom(fromId);
        relation.setTo(toId);
        relation.setTypeGroup(RelationTypeGroup.COMMON);
        relation.setType("buildingTo" + getRelationTypePart(key) + "Sensor");
        return relation;
    }

    private DeviceSearchQuery getDeviceSearchQuery(EntityId entityId) {
        DeviceSearchQuery query = new DeviceSearchQuery();
        query.setRelationType("IS_IN_SPACE");
        query.setParameters(new RelationsSearchParameters(entityId, EntitySearchDirection.FROM, 10, false));
        query.setDeviceTypes(Collections.singletonList("DATA_DEVICE"));
        return query;
    }

    private String getRelationTypePart(String key) {
        switch (key) {
            case "VIBRATION_ENERGY_LEVEL_VALUE":
                return "Vibration";
            case "PRESENCE_MOVE_COUNT_VALUE":
                return "Presence";
            case "NOISE_VALUE":
                return "Noise";
            default:
                return "Light";
        }
    }
}
