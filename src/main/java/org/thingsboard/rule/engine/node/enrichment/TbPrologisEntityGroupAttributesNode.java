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

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.id.EntityGroupId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.thingsboard.common.util.DonAsynchron.withCallback;
import static org.thingsboard.rule.engine.api.TbRelationTypes.FAILURE;
import static org.thingsboard.server.common.data.DataConstants.SERVER_SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "prologis attributes",
        configClazz = TbPrologisEntityGroupAttributesNodeConfiguration.class,
        nodeDescription = "Add Entity Group Attributes into Message Metadata",
        nodeDetails = "Server scope attributes are added into Message metadata. To access those attributes in other nodes this template can be used metadata.temperature",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisEnrichmentAttributesNodeConfig")
public class TbPrologisEntityGroupAttributesNode implements TbNode {

    private TbPrologisEntityGroupAttributesNodeConfiguration config;
    private EntityGroupId entityGroupId;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisEntityGroupAttributesNodeConfiguration.class);
        entityGroupId = EntityGroupId.fromString(config.getEntityGroupId());
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (entityGroupId == null || entityGroupId.isNullUid()) {
            ctx.tellNext(msg, FAILURE);
            return;
        }
        withCallback(getAttributesAsync(ctx, entityGroupId),
                attributeKvEntries -> putAttributesAndTell(ctx, msg, attributeKvEntries),
                throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }

    private ListenableFuture<List<AttributeKvEntry>> getAttributesAsync(TbContext ctx, EntityId entityId) {
        return ctx.getAttributesService().find(ctx.getTenantId(), entityId, SERVER_SCOPE, config.getAttrMapping().keySet());
    }

    private void putAttributesAndTell(TbContext ctx, TbMsg msg, List<AttributeKvEntry> attributes) {
        attributes.forEach(attr -> {
            String attrName = config.getAttrMapping().get(attr.getKey());
            msg.getMetaData().putValue(attrName, attr.getValueAsString());
        });
        ctx.tellSuccess(msg);
    }
}
