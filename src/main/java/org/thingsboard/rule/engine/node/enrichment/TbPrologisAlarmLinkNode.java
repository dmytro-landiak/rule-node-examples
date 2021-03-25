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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DashboardId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.IdBased;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;
import reactor.netty.http.client.HttpClient;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "prologis alarm link",
        configClazz = TbPrologisAlarmLinkNodeConfiguration.class,
        nodeDescription = "Generates link to alarm",
        nodeDetails = "Generate a link of alarm and set it to message metadata in \"alarmLink\" field.",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisEnrichmentAlarmLinkNodeConfig")
public class TbPrologisAlarmLinkNode implements TbNode {

    private static final String DASHBOARD_NAME = "Pump Rooms";
    private static final String STATE = "/?state=";
    private static final String ID_BUILDINGS = "buildings";
    private static final String ID_BUILDING_INDICATORS = "building_indicators_and_card_with_move_count";
    private static final String CREATE_SHORT_URL = "https://d3l24qqaaa9x7b.cloudfront.net/create";
    private static final String LONG_URL = "long_url";
    private static final String SHORT = "short";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TbPrologisAlarmLinkNodeConfiguration config;
    private DashboardId dashboardId;
    private WebClient webClient;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbPrologisAlarmLinkNodeConfiguration.class);
        Optional<DashboardId> dashboardIdOptional = ctx.getDashboardService()
                .findDashboardsByTenantId(ctx.getTenantId(), new PageLink(1000))
                .getData()
                .stream()
                .filter(dashboardInfo -> dashboardInfo.getName().equals(DASHBOARD_NAME))
                .map(IdBased::getId)
                .findFirst();
        webClient = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create().followRedirect(true)))
                .build();
        if (dashboardIdOptional.isPresent()) {
            dashboardId = dashboardIdOptional.get();
        } else {
            log.error("Didn't find dashboard with name = {}", DASHBOARD_NAME);
            throw new RuntimeException("Didn't find dashboard with name = " + DASHBOARD_NAME);
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        DonAsynchron.withCallback(getArrayNode(ctx, msg), arrayNode -> {
            if (arrayNode != null) {
                try {
                    byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(arrayNode);
                    String encodeToString = Base64.getEncoder().encodeToString(bytes);
                    String alarmLink = this.config.getLinkPrefix() +
                            dashboardId.getId() +
                            STATE +
                            encodeToString;
                    webClient.post()
                            .uri(CREATE_SHORT_URL)
                            .contentType(MediaType.APPLICATION_JSON)
                            .accept(MediaType.APPLICATION_JSON)
                            .bodyValue(new HashMap<String, String>(){{
                                put(LONG_URL, alarmLink);
                            }})
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnError(throwable -> {
                                log.warn("Failed to generate short url from long url[{}], Exception : {}", alarmLink, throwable);
                                ctx.tellSuccess(msg);
                            })
                            .doOnSuccess(body -> {
                                msg.getMetaData().putValue("alarmLink", config.getLinkPrefix().replace("dashboards", SHORT) + body);
                                ctx.tellSuccess(msg);
                            })
                            .subscribe();
                    return;
                } catch (JsonProcessingException e) {
                    log.warn("Didn't parse json to bytes!", e);
                }
            }
            ctx.tellSuccess(msg);
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    private ListenableFuture<ArrayNode> getArrayNode(TbContext ctx, TbMsg msg) {
        ObjectNode firstObject = OBJECT_MAPPER.createObjectNode();
        firstObject.put("id", ID_BUILDINGS);
        firstObject.set("params", OBJECT_MAPPER.createObjectNode());

        ObjectNode secondObject = OBJECT_MAPPER.createObjectNode();
        secondObject.put("id", ID_BUILDING_INDICATORS);

        return Futures.transform(getParams(ctx, msg), params -> {
            if (params == null) {
                return null;
            }
            secondObject.set("params", params);
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            arrayNode.add(firstObject);
            arrayNode.add(secondObject);
            return arrayNode;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<ObjectNode> getParams(TbContext ctx, TbMsg msg) {
        String alarmId;
        try {
            JsonNode msgDataNode = OBJECT_MAPPER.readTree(msg.getData());
            if (msgDataNode.has("id") && msgDataNode.get("id").has("id")) {
                alarmId = msgDataNode.get("id").get("id").asText();
            } else {
                log.warn("Didn't find alarm id in msg = {}", msg);
                return Futures.immediateFuture(null);
            }
        } catch (JsonProcessingException e) {
            log.warn("Didn't parse data msg = {}- to json node", msg);
            return Futures.immediateFuture(null);
        }
        return Futures.transform(getBuilding(ctx, msg.getOriginator()), building -> {
            if (building != null) {
                ObjectNode entityId = OBJECT_MAPPER.createObjectNode();
                entityId.put("entityType", "ASSET");
                entityId.put("id", building.getId().toString());

                ObjectNode params = OBJECT_MAPPER.createObjectNode();
                params.set("entityId", entityId);
                params.put("entityName", building.getName());
                params.put("entityLabel", building.getName());
                params.put("alarmId", alarmId);
                return params;
            } else {
                log.warn("Didn't find building for device with id = {}", msg.getOriginator());
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Asset> getBuilding(TbContext ctx, EntityId originator) {
        ListenableFuture<List<EntityRelation>> entityRelationsFuture = ctx.getRelationService()
                .findByQuery(ctx.getTenantId(), getEntityRelationsQuery(originator));
        return Futures.transform(entityRelationsFuture, entityRelations -> {
            if (!CollectionUtils.isEmpty(entityRelations)) {
                return ctx.getAssetService().findAssetById(ctx.getTenantId(), new AssetId(entityRelations.get(0).getFrom().getId()));
            }
            return null;
        }, ctx.getDbCallbackExecutor());
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId) {
        RelationsSearchParameters relationsSearchParameters = new RelationsSearchParameters(originatorId,
                EntitySearchDirection.TO, 5, false);
        EntityTypeFilter entityTypeFilter = new EntityTypeFilter("IS_IN_PROJECT", Collections.singletonList(EntityType.ASSET));
        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(Collections.singletonList(entityTypeFilter));
        return entityRelationsQuery;
    }

    @Override
    public void destroy() {

    }
}
