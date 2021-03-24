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
package org.thingsboard.rule.engine.node.transform;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "vix change originator",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig",
        icon = "find_replace")
public class TbVixChangeOriginatorNode implements TbNode {

    private static final String VEHICLE_TYPE = "vehicle";
    private static final String NUMBER_ATTR = "number";
    private static final String ZEROS_PREFIX = "00";
    private static final int ENTITIES_LIMIT = 10000;

    private final ConcurrentMap<String, EntityId> vehiclesMap = new ConcurrentHashMap<>();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
        initVehiclesMap(ctx);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (msg.getType().equals(DataConstants.ATTRIBUTES_UPDATED)) {
            JsonObject jsonObject = new JsonParser().parse(msg.getData()).getAsJsonObject();
            AssetId assetId = new AssetId(msg.getOriginator().getId());
            ListenableFuture<Asset> assetFuture = ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), assetId);
            DonAsynchron.withCallback(assetFuture, asset -> {
                if (asset.getType().equals(VEHICLE_TYPE)) {
                    vehiclesMap.put(jsonObject.get(NUMBER_ATTR).getAsString(), assetId);
                    log.info("[{}] Vehicles map contains: {}", ctx.getTenantId(), vehiclesMap.size());
                }
                ctx.ack(msg);
            }, throwable -> ctx.tellFailure(msg, throwable));
        } else {
            JsonElement jsonElement = new JsonParser().parse(msg.getData());
            if (jsonElement.isJsonObject()) {
                JsonObject jo = jsonElement.getAsJsonObject();
                String vehicleNumber;
                try {
                    vehicleNumber = ZEROS_PREFIX.concat(jo.get("tripRawData").getAsJsonObject().get("vehicle").getAsJsonObject().get("label").getAsString());
                } catch (Exception e) {
                    log.error("Failed to find required fields in the object {}", jo);
                    ctx.tellFailure(msg, new RuntimeException("Failed to find required fields in the object " + jo));
                    return;
                }
                EntityId vehicleId = vehiclesMap.getOrDefault(vehicleNumber, null);
                if (vehicleId != null) {
                    TbMsg tbMsg = ctx.transformMsg(msg, msg.getType(), vehicleId, msg.getMetaData(), msg.getData());
                    ctx.tellSuccess(tbMsg);
                } else {
                    log.error("[{}] Failed to find vehicle with number {}", ctx.getTenantId(), vehicleNumber);
                    ctx.ack(msg);
                }
            } else {
                log.error("Data is not a valid object {}", msg.getData());
                ctx.tellFailure(msg, new RuntimeException("Data is not a valid object " + msg.getData()));
            }
        }
    }

    @Override
    public void destroy() {

    }

    private void initVehiclesMap(TbContext ctx) {
        List<Asset> vehicles = ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), VEHICLE_TYPE, new PageLink(ENTITIES_LIMIT)).getData();
        if (CollectionUtils.isEmpty(vehicles)) {
            log.warn("[{}] No vehicles found!", ctx.getTenantId());
        } else {
            log.info("Found [{}] vehicles for tenant: {}!", vehicles.size(), ctx.getTenantId());
            CountDownLatch countDownLatch = new CountDownLatch(vehicles.size());
            List<ListenableFuture<Void>> futures = new ArrayList<>();
            for (Asset vehicle : vehicles) {
                ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService().find(ctx.getTenantId(), vehicle.getId(),
                        DataConstants.SERVER_SCOPE, Collections.singletonList(NUMBER_ATTR));
                futures.add(Futures.transform(attributesFuture, list -> {
                    if (!CollectionUtils.isEmpty(list)) {
                        vehiclesMap.put(list.get(0).getValueAsString(), vehicle.getId());
                    } else {
                        log.warn("[{}][{}] Number attribute is not found!", ctx.getTenantId(), vehicle.getName());
                    }
                    countDownLatch.countDown();
                    return null;
                }, ctx.getDbCallbackExecutor()));
            }
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
                Futures.allAsList(futures).get();
                log.info("[{}] Vehicles map contains: {}", ctx.getTenantId(), vehiclesMap.size());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to find all vehicles and their numbers", e);
            }
        }
    }
}
