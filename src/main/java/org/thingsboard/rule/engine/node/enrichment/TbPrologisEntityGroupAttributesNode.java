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
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.group.EntityGroup;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.IdBased;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.thingsboard.common.util.DonAsynchron.withCallback;
import static org.thingsboard.server.common.data.DataConstants.SERVER_SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "prologis attributes",
        configClazz = TbPrologisEntityGroupAttributesNodeConfiguration.class,
        nodeDescription = "Add Entity Group Attributes into Message Metadata",
        nodeDetails = "Found related groups for originator, skipped All and DATA_DEVICE groups. If more than one group left - no attributes are fetched." +
                "Server scope attributes are added into Message metadata. To access those attributes in other nodes this template can be used metadata.temperature",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisEnrichmentAttributesNodeConfig")
public class TbPrologisEntityGroupAttributesNode implements TbNode {

    private static final String ALL_GROUP = "All";
    private static final String DATA_DEVICE_GROUP = "DATA_DEVICE";

    private TbPrologisEntityGroupAttributesNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisEntityGroupAttributesNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
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

        ListenableFuture<List<AttributeKvEntry>> attributesFuture = Futures.transformAsync(entityGroupIdsForEntityFuture, entityGroupIdsForEntity -> {
            if (CollectionUtils.isEmpty(entityGroupIdsForEntity)) {
                log.warn("Device[{}] doesn't belong to any group", msg.getOriginator());
                return Futures.immediateFuture(null);
            }
            return Futures.transformAsync(skipEntityGroupIdsFuture, skipEntityGroupIds -> {
                List<EntityGroupId> entityGroupIds = entityGroupIdsForEntity
                        .stream()
                        .filter(entityGroupId -> !skipEntityGroupIds.contains(entityGroupId)).collect(Collectors.toList());
                if (entityGroupIds.size() == 1) {
                    return getAttributesAsync(ctx, entityGroupIds.get(0));
                }
                log.warn("Device[{}] belongs to {} groups", msg.getOriginator(), entityGroupIds.size());
                return Futures.immediateFuture(null);
            }, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());

        withCallback(attributesFuture,
                attributeKvEntries -> {
                    if (!CollectionUtils.isEmpty(attributeKvEntries)) {
                        putAttributes(msg, attributeKvEntries);
                    }
                    ctx.tellSuccess(msg);
                },
                throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }

    private ListenableFuture<List<AttributeKvEntry>> getAttributesAsync(TbContext ctx, EntityId entityId) {
        return ctx.getAttributesService().find(ctx.getTenantId(), entityId, SERVER_SCOPE, config.getAttrMapping().keySet());
    }

    private void putAttributes(TbMsg msg, List<AttributeKvEntry> attributes) {
        attributes.forEach(attr -> {
            String attrName = config.getAttrMapping().get(attr.getKey());
            msg.getMetaData().putValue(attrName, attr.getValueAsString());
        });
    }
}
