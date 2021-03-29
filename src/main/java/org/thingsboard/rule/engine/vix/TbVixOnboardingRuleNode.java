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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationTypeGroup;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgDataType;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.session.SessionMsgType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.thingsboard.server.common.msg.session.SessionMsgType.POST_ATTRIBUTES_REQUEST;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix onboarding",
        configClazz = TbVixOnboardingRuleNodeConfiguration.class,
        relationTypes = {"Success", "Ip Creation Event", "Service", "Swap Notification", "Clear Alarms Event", "Post attributes", "Attributes Updated"},
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeVixOnboardingConfig"
)
public class TbVixOnboardingRuleNode implements TbNode {

    private static final String TRANSIT_TYPE = "transitType";
    private static final String TRANSIT_MODE = "transitMode";
    private static final String NUMBER = "number";
    private static final String INSTALLATION_ID = "installationId";
    private static final String ESN_KEY = "esn";
    private static final String TL_KEY = "topologyLocation";
    private static final String INSTALLATION_POINT_ID = "installationPointId";
    private static final String IP_TL_PART = "/installationPoints/";

    private static final String FROM_TM_TO_FLEET_TYPE = "FromTransitModeToFleet";
    private static final String FROM_TM_TO_LINE_TYPE = "FromTransitModeToLine";
    private static final String FROM_TM_TO_DEPOT_TYPE = "FromTransitModeToDepot";
    private static final String FROM_IP_TO_DEVICE_RELATION = "FromInstallationPointToDevice";
    private static final String FROM_SO_TO_DEVICE_RELATION = "FromSwappedOutToDevice";

    private static final String SERVICE_STATE_KEY = "serviceState";
    private static final String IN_SERVICE = "inService";
    private static final String OUT_OF_SERVICE = "outOfService";
    private static final String SERVICE_MSG = "Service";

    private final Gson gson = new Gson();
    private final ConcurrentMap<String, TransitModeInfo> transitModeInfoByNumber = new ConcurrentHashMap<>();

    private TbVixOnboardingRuleNodeConfiguration config;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TransitModeInfo {
        private EntityId transitModeId;
        private String transitType;
    }

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbVixOnboardingRuleNodeConfiguration.class);
        initTransitModeInfoByEntityIdMap(ctx);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (DataConstants.ENTITY_DELETED.equals(msg.getType())) {
            JsonObject json = new JsonParser().parse(msg.getData()).getAsJsonObject();
            if (json.has("id")) {
                EntityId entityId = JacksonUtil.fromString(json.get("id").toString(), EntityId.class);
                if (transitModeInfoByNumber.values().removeIf(m -> m.getTransitModeId().equals(entityId))) {
                    log.info("[{}][{}] Removed TM from map!", ctx.getTenantId(), entityId);
                }
            }
            ctx.ack(msg);
            return;
        }
        if (EntityType.DEVICE.equals(msg.getOriginator().getEntityType())
                && msg.getType().equals(SessionMsgType.POST_ATTRIBUTES_REQUEST.name())) {
            DonAsynchron.withCallback(processOnBoarding(ctx, msg), b -> {
                ctx.tellNext(msg, "Post attributes");
            }, throwable -> ctx.tellFailure(msg, throwable));
        } else if (EntityType.ASSET.equals(msg.getOriginator().getEntityType())
                && DataConstants.ATTRIBUTES_UPDATED.equals(msg.getType())) {
            Asset asset = ctx.getAssetService().findAssetById(ctx.getTenantId(), AssetId.fromString(msg.getOriginator().getId().toString()));
            if (asset.getType().equals(TRANSIT_MODE)) {
                JsonObject json = new JsonParser().parse(msg.getData()).getAsJsonObject();
                if (json.has(NUMBER)) {
                    TransitModeInfo tmInfo = transitModeInfoByNumber.computeIfAbsent(json.get(NUMBER).getAsString(), number -> new TransitModeInfo());
                    tmInfo.setTransitModeId(msg.getOriginator());
                    tmInfo.setTransitType(json.has(TRANSIT_TYPE) ? json.get(TRANSIT_TYPE).getAsString() : null);
                } else if (json.has(TRANSIT_TYPE)) {
                    Optional<TransitModeInfo> tmInfoOpt = transitModeInfoByNumber.values()
                            .stream()
                            .filter(m -> m.getTransitModeId()
                                    .equals(msg.getOriginator()))
                            .findFirst();
                    tmInfoOpt.ifPresent(transitModeInfo -> transitModeInfo.setTransitType(json.get(TRANSIT_TYPE).getAsString()));
                }
                log.info("[{}] Added new TM with a number {}", ctx.getTenantId(), json);
            }
            ctx.tellSuccess(msg);
        } else {
            ctx.ack(msg);
        }
    }

    private ListenableFuture<Boolean> processOnBoarding(TbContext ctx, TbMsg msg) throws TbNodeException {
        JsonObject json = new JsonParser().parse(msg.getData()).getAsJsonObject();
        if (json.has(INSTALLATION_ID)) {
            ListenableFuture<Device> deviceFuture = ctx.getDeviceService().findDeviceByIdAsync(ctx.getTenantId(), new DeviceId(msg.getOriginator().getId()));
            return Futures.transformAsync(deviceFuture, device -> {
                if (device == null) {
                    log.error("Failed to find device {}", msg.getOriginator());
                    return Futures.immediateFuture(false);
                }
                String installationId = json.get(INSTALLATION_ID).getAsString();
                Asset installationPoint = findAssetByName(ctx, installationId);
                String tmNumber = installationId.substring(16, 19);
                if (installationPoint == null) {
                    log.warn("[{}][{}][{}] Installation point is not present for a device!", ctx.getTenantId(), installationId, device.getName());
                    if (transitModeInfoByNumber.containsKey(tmNumber)) {
                        pushIpCreateEventToRuleEngine(ctx, device, installationId, transitModeInfoByNumber.get(tmNumber).getTransitType());
                    } else {
                        log.error("[{}] Failed to find TM by number {}!", ctx.getTenantId(), tmNumber);
                    }
                    return Futures.immediateFuture(false);
                } else {
                    return Futures.transformAsync(findPresentRelationsByIp(ctx, installationPoint), fromIpToDeviceRelations -> {
                        if (!CollectionUtils.isEmpty(fromIpToDeviceRelations)) {
                            EntityRelation presentFromIpToDeviceRelation = fromIpToDeviceRelations.get(0);
                            if (presentFromIpToDeviceRelation.getTo().getId().equals(device.getId().getId())) {
                                if (json.has(ESN_KEY)) {
                                    return Futures.immediateFuture(true);
                                } else {
                                    return processOnboarding(ctx, device, installationPoint, presentFromIpToDeviceRelation, installationId, false);
                                }
                            } else {
                                return processOnboarding(ctx, device, installationPoint, presentFromIpToDeviceRelation, installationId, true);
                            }
                        } else {
                            return onBoardDevice(ctx, device, installationPoint, null);
                        }
                    }, ctx.getDbCallbackExecutor());
                }
            }, ctx.getDbCallbackExecutor());
        } else {
            log.error("[{}] Installation id attribute is absent in the payload {}!", msg.getOriginator(), json);
            throw new TbNodeException("Installation id attribute is absent in the payload!");
        }
    }

    private void pushIpCreateEventToRuleEngine(TbContext ctx, Device device, String installationId, String transitType) {
        try {
            ObjectNode entityNode = JacksonUtil.OBJECT_MAPPER.createObjectNode();
            entityNode.put("installationId", installationId);
            TbMsgMetaData metaData = createDeviceMetaData(device);
            metaData.putValue("customerName", config.getCustomerName());
            metaData.putValue(TRANSIT_TYPE, transitType);
            TbMsg msg = TbMsg.newMsg(DataConstants.IP_CREATION, device.getId(),
                    metaData, JacksonUtil.OBJECT_MAPPER.writeValueAsString(entityNode));
            ctx.enqueueForTellNext(msg, "Ip Creation Event");
        } catch (Exception e) {
            log.warn("[{}] Failed to push installation point create notification to rule engine: {}", device.getId(), DataConstants.IP_CREATION, e);
        }
    }

    @Override
    public void destroy() {
    }

    private Asset findAssetByName(TbContext ctx, String name) {
        return ctx.getAssetService().findAssetByTenantIdAndName(ctx.getTenantId(), name);
    }

    private ListenableFuture<Boolean> processOnboarding(TbContext ctx, Device device, Asset installationPoint,
                                                        EntityRelation presentFromIpToDeviceRelation, String installationId, boolean onBoardNewDevice) {
        String operatorNumber = installationId.substring(5, 15);
        String tmNumber = installationId.substring(16, 19);
        String swappedOutName = getSwappedName(operatorNumber, tmNumber);
        Asset swappedOut = findAssetByName(ctx, swappedOutName);

        ListenableFuture<Boolean> relationFuture = Futures.transformAsync(
                deleteRelation(ctx, presentFromIpToDeviceRelation),
                b -> {
                    if (swappedOut != null) {
                        return createRelationFromSwappedOutToDevice(ctx, swappedOut, presentFromIpToDeviceRelation.getTo());
                    }
                    log.warn("[{}]: Didn't find {} asset", ctx.getTenantId(), swappedOutName);
                    return Futures.immediateFuture(false);
                }, ctx.getDbCallbackExecutor());

        ListenableFuture<Device> presentDeviceFuture = Futures.transformAsync(relationFuture,
                b -> ctx.getDeviceService().findDeviceByIdAsync(
                        installationPoint.getTenantId(),
                        new DeviceId(presentFromIpToDeviceRelation.getTo().getId())), ctx.getDbCallbackExecutor());

        return Futures.transformAsync(presentDeviceFuture, presentDevice -> {
            TbMsgMetaData metaData = createDeviceMetaData(presentDevice);
            metaData.putValue(INSTALLATION_POINT_ID, presentFromIpToDeviceRelation.getFrom().getId().toString());
            ctx.enqueueForTellNext(getTbMsg(presentFromIpToDeviceRelation.getTo(), metaData, createJsonObj(OUT_OF_SERVICE, swappedOutName)), SERVICE_MSG);
            if (presentDevice != null) {
                JsonObject jo = new JsonObject();
                jo.addProperty(INSTALLATION_ID, "");
                ctx.enqueueForTellNext(TbMsg.newMsg(
                        POST_ATTRIBUTES_REQUEST.name(),
                        presentDevice.getId(),
                        createDeviceMetaData(presentDevice),
                        TbMsgDataType.JSON,
                        gson.toJson(jo)), "Post attributes");
            }
            if (onBoardNewDevice) {
                return onBoardDevice(ctx, device, installationPoint, presentDevice);
            }
            return Futures.immediateFuture(false);
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> onBoardDevice(TbContext ctx, Device device, Asset installationPoint, Device prevDevice) {
        return Futures.transformAsync(getIpTopologyLocation(ctx, installationPoint), attributeKvEntries -> {
            String deviceTopologyLocation = getDeviceTopologyLocation(attributeKvEntries, installationPoint.getName());
            ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService().findByQuery(ctx.getTenantId(), buildQuery(device.getId()));
            return Futures.transformAsync(relationsFuture, relations -> {
                if (!CollectionUtils.isEmpty(relations)) {
                    for (EntityRelation relation : relations) {
                        if (relation.getType().equals(FROM_TM_TO_FLEET_TYPE)
                                || relation.getType().equals(FROM_TM_TO_LINE_TYPE)
                                || relation.getType().equals(FROM_TM_TO_DEPOT_TYPE)) {
                            relations.remove(relation);

                            ListenableFuture<List<AttributeKvEntry>> transitTypeAttrListFuture = ctx.getAttributesService()
                                    .find(ctx.getTenantId(), relation.getFrom(), DataConstants.SERVER_SCOPE, Collections.singletonList(TRANSIT_TYPE));
                            return Futures.transformAsync(transitTypeAttrListFuture, transitTypeAttrList -> {
                                final String transitType = CollectionUtils.isEmpty(transitTypeAttrList) ? null : transitTypeAttrList.get(0).getValueAsString();

                                ListenableFuture<Void> deletedRelationsFuture = deletePresentRelations(ctx, device, relations, transitType);
                                return Futures.transformAsync(deletedRelationsFuture, v -> {
                                    ctx.enqueueForTellNext(getTbMsg(device.getId(), createDeviceMetaData(device), createJsonObj(IN_SERVICE, deviceTopologyLocation)), SERVICE_MSG);
                                    return saveNewRelation(ctx, installationPoint, device, prevDevice, deviceTopologyLocation, transitType);
                                }, ctx.getDbCallbackExecutor());
                            }, ctx.getDbCallbackExecutor());
                        } else if (relation.getType().equals(FROM_SO_TO_DEVICE_RELATION)) {
                            return Futures.transformAsync(deleteRelation(ctx, relation), b -> {
                                ctx.enqueueForTellNext(getTbMsg(device.getId(), createDeviceMetaData(device), createJsonObj(IN_SERVICE, deviceTopologyLocation)), SERVICE_MSG);
                                return saveNewRelation(ctx, installationPoint, device, prevDevice, deviceTopologyLocation, null);
                            }, ctx.getDbCallbackExecutor());
                        }
                    }
                } else {
                    ctx.enqueueForTellNext(getTbMsg(device.getId(), createDeviceMetaData(device), createJsonObj(IN_SERVICE, deviceTopologyLocation)), SERVICE_MSG);
                    return saveNewRelation(ctx, installationPoint, device, prevDevice, deviceTopologyLocation, null);
                }
                return Futures.immediateFuture(false);
            }, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> saveNewRelation(TbContext ctx, Asset installationPoint, Device device, Device prevDevice,
                                                      String deviceTopologyLocation, String transitType) {
        return Futures.transform(saveRelation(ctx, installationPoint, device), savedRelation -> {
            if (savedRelation != null && savedRelation) {
                TbMsgMetaData metaData = getSwapEventMetaData(installationPoint, device, prevDevice, deviceTopologyLocation, transitType);
                pushDeviceSwapEventToRuleEngine(ctx, device, metaData);
                return true;
            }
            return false;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Boolean> saveRelation(TbContext ctx, Asset installationPoint, Device device) {
        return ctx.getRelationService().saveRelationAsync(
                installationPoint.getTenantId(),
                createRelation(installationPoint, device));
    }

    private EntityRelation createRelation(Asset asset, Device device) {
        EntityRelation relation = new EntityRelation();
        relation.setFrom(asset.getId());
        relation.setTo(device.getId());
        relation.setType(FROM_IP_TO_DEVICE_RELATION);
        relation.setTypeGroup(RelationTypeGroup.COMMON);
        return relation;
    }

    private void pushDeviceSwapEventToRuleEngine(TbContext ctx, Device device, TbMsgMetaData metaData) {
        try {
            ObjectNode entityNode = JacksonUtil.OBJECT_MAPPER.valueToTree(device);
            TbMsg msg = TbMsg.newMsg(DataConstants.SWAP_NOTIFICATION, device.getId(),
                    metaData, JacksonUtil.OBJECT_MAPPER.writeValueAsString(entityNode));
            ctx.enqueueForTellNext(msg, "Swap Notification");
        } catch (Exception e) {
            log.warn("[{}] Failed to push device swap notification to rule engine: {}", device.getId(), DataConstants.SWAP_NOTIFICATION, e);
        }
    }

    private TbMsgMetaData getSwapEventMetaData(Asset installationPoint, Device device, Device prevDevice, String deviceTopologyLocation, String transitType) {
        TbMsgMetaData metaData = createDeviceMetaData(device);
        metaData.putValue("installationId", installationPoint.getName());
        if (prevDevice != null) {
            metaData.putValue("prevDeviceName", prevDevice.getName());
            metaData.putValue("prevDeviceType", prevDevice.getType());
        }
        if (transitType != null) {
            metaData.putValue(TRANSIT_TYPE, transitType);
        }
        metaData.putValue("topologyLocation", deviceTopologyLocation);
        metaData.putValue(INSTALLATION_POINT_ID, installationPoint.getId().getId().toString());
        return metaData;
    }

    private ListenableFuture<Void> deletePresentRelations(TbContext ctx, Device device, List<EntityRelation> deviceToIpRelations, String transitType) {
        if (!CollectionUtils.isEmpty(deviceToIpRelations)) {
            List<ListenableFuture<Boolean>> futures = new ArrayList<>();

            EntityId entityId = null;
            for (EntityRelation deviceToIpRelation : deviceToIpRelations) {
                if (deviceToIpRelation.getType().equals(FROM_IP_TO_DEVICE_RELATION)) {
                    entityId = deviceToIpRelation.getFrom();
                }
                futures.add(deleteRelation(ctx, deviceToIpRelation));
            }

            pushDeviceClearAlarmsEventToRuleEngine(ctx, device, getClearAlarmsMsgMetaData(device, transitType, entityId));
            return Futures.transform(Futures.allAsList(futures), input -> null, ctx.getDbCallbackExecutor());
        }
        return Futures.immediateFuture(null);
    }

    private void pushDeviceClearAlarmsEventToRuleEngine(TbContext ctx, Device device, TbMsgMetaData metaData) {
        try {
            ObjectNode entityNode = JacksonUtil.OBJECT_MAPPER.valueToTree(device);
            TbMsg msg = TbMsg.newMsg(DataConstants.CLEAR_ALARMS, device.getId(),
                    metaData, JacksonUtil.OBJECT_MAPPER.writeValueAsString(entityNode));
            ctx.enqueueForTellNext(msg, "Clear Alarms Event");
        } catch (Exception e) {
            log.warn("[{}] Failed to push device clear alarms notification to rule engine: {}", device.getId(), DataConstants.CLEAR_ALARMS, e);
        }
    }

    private TbMsgMetaData getClearAlarmsMsgMetaData(Device device, String transitType, EntityId entityId) {
        TbMsgMetaData metaData = createDeviceMetaData(device);
        if (transitType != null) {
            metaData.putValue(TRANSIT_TYPE, transitType);
        }
        if (entityId != null) {
            metaData.putValue(INSTALLATION_POINT_ID, entityId.getId().toString());
        }
        return metaData;
    }

    private EntityRelationsQuery buildQuery(EntityId originator) {
        EntityRelationsQuery query = new EntityRelationsQuery();
        query.setFilters(constructEntityTypeFilters());
        query.setParameters(new RelationsSearchParameters(originator, EntitySearchDirection.TO, 4, false));
        return query;
    }

    private List<EntityTypeFilter> constructEntityTypeFilters() {
        List<EntityTypeFilter> entityTypeFilters = new ArrayList<>();
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_FLEET_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_LINE_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_TM_TO_DEPOT_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_IP_TO_DEVICE_RELATION, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_SO_TO_DEVICE_RELATION, Collections.singletonList(EntityType.ASSET)));
        return entityTypeFilters;
    }

    private EntityTypeFilter createTypeFilter(String relationType, List<EntityType> entityTypes) {
        return new EntityTypeFilter(relationType, entityTypes);
    }

    private String getDeviceTopologyLocation(List<AttributeKvEntry> attributeKvEntries, String installationPointName) {
        if (attributeKvEntries != null && !attributeKvEntries.isEmpty()) {
            return attributeKvEntries.get(0).getValueAsString() + IP_TL_PART + installationPointName;
        }
        return null;
    }

    private ListenableFuture<List<AttributeKvEntry>> getIpTopologyLocation(TbContext ctx, Asset installationPoint) {
        return ctx.getAttributesService().find(
                installationPoint.getTenantId(),
                installationPoint.getId(),
                DataConstants.SERVER_SCOPE,
                Collections.singletonList(TL_KEY));
    }

    private TbMsg getTbMsg(EntityId entityId, TbMsgMetaData metaData, JsonObject jo) {
        return TbMsg.newMsg(
                POST_ATTRIBUTES_REQUEST.name(),
                entityId,
                metaData,
                TbMsgDataType.JSON,
                gson.toJson(jo));
    }

    private JsonObject createJsonObj(String serviceStateValue, String deviceTopologyLocation) {
        JsonObject jo = new JsonObject();
        jo.addProperty(SERVICE_STATE_KEY, serviceStateValue);
        if (deviceTopologyLocation != null) {
            jo.addProperty(TL_KEY, deviceTopologyLocation);
        }
        return jo;
    }


    private ListenableFuture<Boolean> createRelationFromSwappedOutToDevice(TbContext ctx, Asset asset, EntityId entityId) {
        EntityRelation relation = new EntityRelation();
        relation.setFrom(asset.getId());
        relation.setTo(entityId);
        relation.setType(FROM_SO_TO_DEVICE_RELATION);
        relation.setTypeGroup(RelationTypeGroup.COMMON);
        return ctx.getRelationService().saveRelationAsync(asset.getTenantId(), relation);
    }

    private ListenableFuture<Boolean> deleteRelation(TbContext ctx, EntityRelation relation) {
        return ctx.getRelationService().deleteRelationAsync(ctx.getTenantId(), relation);
    }

    private String getSwappedName(String operatorNumber, String tmNumber) {
        return "operators/" + operatorNumber + "/transitModes/" + tmNumber + "/swappedOutDevices";
    }

    private void initTransitModeInfoByEntityIdMap(TbContext ctx) {
        List<Asset> transitModes = getAllTransitModes(ctx);
        List<ListenableFuture<List<AttributeKvEntry>>> futures = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(transitModes.size());
        for (Asset transitMode : transitModes) {
            ListenableFuture<List<AttributeKvEntry>> attributesFuture = ctx.getAttributesService().find(
                    ctx.getTenantId(),
                    transitMode.getId(),
                    DataConstants.SERVER_SCOPE,
                    Arrays.asList(NUMBER, TRANSIT_TYPE));
            futures.add(Futures.transform(attributesFuture, attributes -> {
                if (!CollectionUtils.isEmpty(attributes) && attributes.size() == 2) {
                    String number = null;
                    String transitType = null;
                    for (AttributeKvEntry attribute : attributes) {
                        if (attribute.getKey().equals(NUMBER)) {
                            number = attribute.getValueAsString();
                        } else {
                            transitType = attribute.getValueAsString();
                        }
                    }
                    transitModeInfoByNumber.put(number, new TransitModeInfo(transitMode.getId(), transitType));
                } else {
                    log.warn("[{}][{}] {} and {} attributes are not found!", ctx.getTenantId(), transitMode.getName(), NUMBER, TRANSIT_TYPE);
                }
                countDownLatch.countDown();
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        try {
            countDownLatch.await(10, TimeUnit.SECONDS);
            Futures.allAsList(futures).get();
            log.info("[{}] Transit Modes map contains: {}", ctx.getTenantId(), transitModes.size());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to find all transit modes and their attributes(" + NUMBER + ", " + TRANSIT_TYPE + ")", e);
        }
    }

    private List<Asset> getAllTransitModes(TbContext ctx) {
        List<Asset> transitModes = new ArrayList<>();
        int number = 0;
        while (true) {
            PageData<Asset> assetsPageData = ctx.getAssetService()
                    .findAssetsByTenantIdAndType(ctx.getTenantId(), TRANSIT_MODE, new PageLink(1000, number));
            transitModes.addAll(assetsPageData.getData());
            if (!assetsPageData.hasNext()) {
                break;
            }
        }
        return transitModes;
    }

    private TbMsgMetaData createDeviceMetaData(Device device) {
        TbMsgMetaData metaData = new TbMsgMetaData();
        if (device != null) {
            metaData.putValue("deviceName", device.getName());
            metaData.putValue("deviceType", device.getType());
        }
        return metaData;
    }

    private ListenableFuture<List<EntityRelation>> findPresentRelationsByIp(TbContext ctx, Asset installationPoint) {
        return ctx.getRelationService().findByFromAndTypeAsync(
                installationPoint.getTenantId(),
                installationPoint.getId(),
                FROM_IP_TO_DEVICE_RELATION,
                RelationTypeGroup.COMMON);
    }
}
