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
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "users emails",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        configDirective = "tbNodeEmptyConfig"
)
public class TbPrologisUserEmailsNode implements TbNode {

    private EmptyNodeConfiguration config;
    private Customer prologis;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
        ctx.getCustomerService().findCustomerByTenantIdAndTitle(ctx.getTenantId(), "Prologis").ifPresent(customer -> prologis = customer);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        EntityRelationsQuery entityRelationsQuery = getEntityRelationsQuery(msg.getOriginator());
        ListenableFuture<List<EntityRelation>> future = ctx.getRelationService().findByQuery(ctx.getTenantId(), entityRelationsQuery);
        ListenableFuture<Void> resFuture = Futures.transformAsync(future, entityRelations -> {
            if (!CollectionUtils.isEmpty(entityRelations)) {
                if (prologis != null) {
                    List<UserId> entityIds = entityRelations
                            .stream()
                            .map(EntityRelation::getTo)
                            .map(entityId -> new UserId(entityId.getId()))
                            .collect(Collectors.toList());
                    return Futures.transform(ctx.getUserService().findUsersByTenantIdAndIdsAsync(ctx.getTenantId(), entityIds), users -> {
                        if (!CollectionUtils.isEmpty(users)) {
                            List<String> emails = users.stream().map(User::getEmail).collect(Collectors.toList());
                            msg.getMetaData().putValue("emails", String.join(",", emails));
                        }
                        return null;
                    }, ctx.getDbCallbackExecutor());
                }
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(resFuture, v -> ctx.tellSuccess(msg), throwable -> ctx.tellFailure(msg, throwable));
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId) {
        RelationsSearchParameters relationsSearchParameters = new RelationsSearchParameters(originatorId,
                EntitySearchDirection.TO, 4, true);
        EntityTypeFilter entityTypeFilter = new EntityTypeFilter("FromCustomerToTech", Collections.singletonList(EntityType.CUSTOMER));

        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(Collections.singletonList(entityTypeFilter));
        return entityRelationsQuery;
    }

    @Override
    public void destroy() {
    }

}