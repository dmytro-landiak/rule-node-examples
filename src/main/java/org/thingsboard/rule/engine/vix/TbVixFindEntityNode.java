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

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "vix find entity",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig",
        icon = "find_replace")

public class TbVixFindEntityNode implements TbNode {

    private static final String INSTALLATION_POINT_ID = "installationPointId";

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        String installationPointId = msg.getMetaData().getValue(INSTALLATION_POINT_ID);
        if (StringUtils.isEmpty(installationPointId)) {
            ctx.tellFailure(msg, new RuntimeException("InstallationPoint id is not present in the metadata!"));
        } else {
            ListenableFuture<Asset> assetFuture = ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), AssetId.fromString(installationPointId));
            DonAsynchron.withCallback(assetFuture, asset -> {
                if (asset == null) {
                    ctx.tellFailure(msg, new RuntimeException("Failed to find entity by id: " + installationPointId));
                } else {
                    TbMsg tbMsg = ctx.transformMsg(msg, msg.getType(), asset.getId(), msg.getMetaData(), msg.getData());
                    ctx.tellSuccess(tbMsg);
                }
            }, throwable -> ctx.tellFailure(msg, throwable));
        }
    }

    @Override
    public void destroy() {

    }
}
