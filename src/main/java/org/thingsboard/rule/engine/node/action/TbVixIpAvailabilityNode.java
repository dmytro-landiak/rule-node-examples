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
import com.google.gson.JsonObject;
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
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix ip availability",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbVixIpAvailabilityNode implements TbNode {

    private static final String IP_TYPE = "installationPoint";
    private static final String ACTUAL_AVAILABILITY_EVENTS = "actualAvailabilityEvents";
    private static final int ENTITIES_LIMIT = 10000;

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        log.info("Started populating actualAvailabilityEvents for all IPs...");
        List<Asset> installationPoints = getInstallationPoints(ctx);
        if (!installationPoints.isEmpty()) {
            List<ListenableFuture<List<Void>>> futures = new ArrayList<>();
            long currTs = System.currentTimeMillis();
            String jo = getJsonObject(String.valueOf(currTs));
            for (Asset installationPoint : installationPoints) {
                ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService().find(
                        ctx.getTenantId(),
                        installationPoint.getId(),
                        DataConstants.SERVER_SCOPE,
                        Collections.singletonList(ACTUAL_AVAILABILITY_EVENTS));

                futures.add(Futures.transformAsync(attributesFuture, attributes -> {
                    if (CollectionUtils.isEmpty(attributes)) {
                        return ctx.getAttributesService().save(
                                ctx.getTenantId(),
                                installationPoint.getId(),
                                DataConstants.SERVER_SCOPE,
                                Collections.singletonList(
                                        new BaseAttributeKvEntry(
                                                new JsonDataEntry(ACTUAL_AVAILABILITY_EVENTS, jo),
                                                currTs)));
                    }
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor()));
            }

            DonAsynchron.withCallback(Futures.allAsList(futures),
                    lists -> ctx.tellSuccess(msg),
                    throwable -> ctx.tellFailure(msg, throwable));
        } else {
            log.warn("[{}] No IPs found!", ctx.getTenantId());
            ctx.tellFailure(msg, new RuntimeException("No IPs found!"));
        }
        log.info("Finished populating actualAvailabilityEvents for all IPs...");
    }

    @Override
    public void destroy() {

    }

    private List<Asset> getInstallationPoints(TbContext ctx) {
        return ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), IP_TYPE, new PageLink(ENTITIES_LIMIT)).getData();
    }

    private String getJsonObject(String key) {
        JsonObject jo = new JsonObject();
        jo.addProperty(key, "available");
        return jo.toString();
    }
}
