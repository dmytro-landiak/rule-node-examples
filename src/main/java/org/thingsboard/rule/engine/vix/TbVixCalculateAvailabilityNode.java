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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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
import org.thingsboard.rule.engine.api.TbRelationTypes;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.AssetId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.JsonDataEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.queue.ServiceQueue;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RuleNode(
        type = ComponentType.ANALYTICS,
        name = "calculate availability",
        configClazz = TbVixCalculateAvailabilityNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        inEnabled = false,
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbAnalyticsNodeCalculateAvailabilityConfig",
        icon = "functions")
public class TbVixCalculateAvailabilityNode implements TbNode {

    private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String TB_MSG_CUSTOM_NODE_MSG = "TbMsgCustomNodeMsg";
    private static final String IP_TYPE = "installationPoint";
    private static final String ACTUAL_AVAILABILITY_EVENTS = "actualAvailabilityEvents";
    private static final String TRIPS_KEY = "trips";
    private static final String FROM_VEHICLE_TO_IP_TYPE = "FromVehicleToInstallationPoint";
    private static final String FROM_STATION_TO_IP_TYPE = "FromStationToInstallationPoint";
    private static final String AVAILABILITY_TS_KEY = "availabilityEvents";

    private static final int ENTITIES_LIMIT = 1000;
    private static final long ONE_DAY_MS = 24 * 3600 * 1000;

    private ScheduledExecutorService scheduledExecutor;
    private TbVixCalculateAvailabilityNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbVixCalculateAvailabilityNodeConfiguration.class);
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

        long currTsMs = System.currentTimeMillis();
        long configTsMs = getTimeOfExecution();

        if (currTsMs <= configTsMs) {
            scheduledExecutor.scheduleAtFixedRate(() -> runTask(ctx),
                    configTsMs - currTsMs, ONE_DAY_MS, TimeUnit.MILLISECONDS);
        } else {
            scheduledExecutor.scheduleAtFixedRate(() -> runTask(ctx),
                    getInitDelayMs(currTsMs, configTsMs), ONE_DAY_MS, TimeUnit.MILLISECONDS);
        }
    }

    private long getInitDelayMs(long currTsMs, long configTsMs) {
        long diff = getMillisForTime(currTsMs) - getMillisForTime(configTsMs);
        if (diff < 0) {
            return Math.abs(diff);
        }
        return ONE_DAY_MS - diff;
    }

    private long getMillisForTime(long unixTimestampInMillis) {
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimestampInMillis), ZoneId.systemDefault());
        return (ldt.getHour() * 3600 + ldt.getMinute() * 60 + ldt.getSecond()) * 1000;
    }

    private long getTimeOfExecution() {
        try {
            return defaultDateFormat.parse(this.config.getDateOfExecution()).getTime();
        } catch (ParseException e) {
            log.error("Failed to parse date: {}", this.config.getDateOfExecution(), e);
        }
        return System.currentTimeMillis();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (msg.getType().equals(TB_MSG_CUSTOM_NODE_MSG)) {
            ctx.enqueueForTellNext(msg, "HighPriority", TbRelationTypes.SUCCESS,
                    () -> {},
                    throwable -> ctx.tellFailure(msg, throwable));
        } else {
            ctx.tellFailure(msg, new RuntimeException("Wrong message came!"));
        }
    }

    private void runTask(TbContext ctx) {
        ListenableFuture<List<AttributeKvEntry>> future = ctx.getAttributesService()
                .find(ctx.getTenantId(), ctx.getTenantId(), DataConstants.SERVER_SCOPE, Collections.singletonList("enableAvailabilityCalculation"));
        DonAsynchron.withCallback(future, attributeKvEntries -> {
            if (CollectionUtils.isEmpty(attributeKvEntries)) {
                log.warn("Did not find [enableAvailabilityCalculation] attribute for a tenant: {}", ctx.getTenantId());
            } else {
                if (!attributeKvEntries.get(0).getBooleanValue().orElse(false)) {
                    log.info("[enableAvailabilityCalculation] attribute is disabled for a tenant: {}", ctx.getTenantId());
                    return;
                }
                log.info("[{}] Starting calculation availability...", ctx.getTenantId());
                List<Asset> installationPoints = getInstallationPoints(ctx);
                if (!installationPoints.isEmpty()) {
                    ConcurrentMap<String, ParentAvailability> stationsAndVehiclesMap = new ConcurrentHashMap<>();

                    List<ListenableFuture<JsonObject>> resultFutures = new ArrayList<>();
                    for (Asset installationPoint : installationPoints) {
                        resultFutures.add(Futures.transformAsync(findAttributes(ctx, installationPoint.getId(), ACTUAL_AVAILABILITY_EVENTS), ipAttributes -> {
                            ListenableFuture<List<EntityRelation>> relationsFuture = ctx.getRelationService()
                                    .findByQuery(ctx.getTenantId(), buildQuery(installationPoint.getId()));
                            return Futures.transformAsync(relationsFuture, relations -> {
                                if (!CollectionUtils.isEmpty(relations)) {
                                    for (EntityRelation relation : relations) {
                                        EntityId fromEntityId = relation.getFrom();
                                        if (relation.getType().equals(FROM_STATION_TO_IP_TYPE)) {
                                            ListenableFuture<JsonObject> joFuture = processCalculateAvailability(
                                                    ctx,
                                                    installationPoint,
                                                    ipAttributes);
                                            return updateSVMap(ctx, fromEntityId, stationsAndVehiclesMap, joFuture,
                                                    installationPoint.getName());
                                        } else if (relation.getType().equals(FROM_VEHICLE_TO_IP_TYPE)) {
                                            return Futures.transformAsync(findAttributes(ctx, fromEntityId, TRIPS_KEY), vehicleAttributes -> {
                                                if (!CollectionUtils.isEmpty(vehicleAttributes)) {
                                                    ListenableFuture<JsonObject> joFuture = processCalculateAvailability(
                                                            ctx,
                                                            installationPoint,
                                                            ipAttributes,
                                                            fromEntityId,
                                                            vehicleAttributes);
                                                    return updateSVMap(ctx, fromEntityId, stationsAndVehiclesMap, joFuture,
                                                            installationPoint.getName());
                                                } else {
                                                    log.warn("[{}] Failed to find trips attributes for vehicle: {}", ctx.getTenantId(), fromEntityId);
                                                }
                                                return Futures.immediateFuture(null);
                                            }, ctx.getDbCallbackExecutor());
                                        } else {
                                            return Futures.immediateFuture(null);
                                        }
                                    }
                                } else {
                                    log.info("[{}] No entity related to IP found: {}", ctx.getTenantId(), installationPoint.getName());
                                }
                                return Futures.immediateFuture(null);
                            }, ctx.getDbCallbackExecutor());
                        }, ctx.getDbCallbackExecutor()));
                    }

                    ListenableFuture<Void> resFuture = Futures.transformAsync(Futures.successfulAsList(resultFutures), ipJsonObjects -> {
                        if (ipJsonObjects != null) {
                            tellSelf(ctx, ipJsonObjects);
                        }
                        List<ListenableFuture<List<Void>>> futures = new ArrayList<>();
                        for (Map.Entry<String, ParentAvailability> parentEntry : stationsAndVehiclesMap.entrySet()) {
                            for (Map.Entry<String, List<JsonObject>> entry : parentEntry.getValue().getAvailabilityEventsByDayMap().entrySet()) {

                                if (!CollectionUtils.isEmpty(entry.getValue())) {
                                    JsonObject parentJo = new JsonObject();

                                    addParentDeliveredAvailabilityAndMs(entry.getValue(), parentJo);
                                    parentJo.addProperty(parentEntry.getValue().getParentAsset().getType(), parentEntry.getKey());

                                    saveAvailabilityEvent(ctx, parentEntry.getValue().getParentAsset(), parentJo);
                                    tellSelf(ctx, parentJo, parentEntry.getValue().getInstallationPoint());
                                }
                            }
                            if (parentEntry.getValue().getParentAsset().getType().equals("vehicle")) {
                                futures.add(updateAttribute(
                                        ctx,
                                        parentEntry.getValue().getParentAsset().getId(),
                                        TRIPS_KEY,
                                        updateTrips(new JsonObject())));
                            }
                        }
                        return Futures.transform(Futures.allAsList(futures), v -> null, ctx.getDbCallbackExecutor());
                    }, ctx.getDbCallbackExecutor());

                    DonAsynchron.withCallback(resFuture,
                            v -> log.info("[{}] Finished calculation availability...", ctx.getTenantId()),
                            throwable -> log.error("[{}] Failed to process calculation of availability!", ctx.getTenantId(), throwable));
                } else {
                    log.warn("[{}] No IPs found!", ctx.getTenantId());
                }
            }
        }, throwable -> log.error("Failed to get [enableAvailabilityCalculation] attribute for a tenant: {}", ctx.getTenantId(), throwable));
    }

    private void addParentDeliveredAvailabilityAndMs(List<JsonObject> jsonObjects, JsonObject parentJo) {
        double parentDeliveredAvailability = 0.0;
        long parentDeliveredAvailabilityMs = 0;
        for (JsonObject jo : jsonObjects) {
            if (jo != null) {
                parentDeliveredAvailability += jo.get("deliveredAvailability").getAsDouble();
                parentDeliveredAvailabilityMs += jo.get("deliveredAvailabilityMs").getAsLong();
                parentJo.add("availabilityInterval", jo.get("availabilityInterval"));
            }
        }
        parentJo.addProperty("deliveredAvailabilityMs", parentDeliveredAvailabilityMs);
        parentDeliveredAvailability = parentDeliveredAvailability / jsonObjects.size();
        parentJo.addProperty("deliveredAvailability", parentDeliveredAvailability);
    }

    private ListenableFuture<JsonObject> updateSVMap(TbContext ctx, EntityId entityId,
                                                     ConcurrentMap<String, ParentAvailability> svMap,
                                                     ListenableFuture<JsonObject> future, String installationPoint) {
        return Futures.transformAsync(future, jo -> Futures.transform(findAsset(ctx, entityId), asset -> {
            if (asset != null && jo != null) {
                ParentAvailability parentAvailability = svMap.computeIfAbsent(asset.getName(), n -> new ParentAvailability(
                        new ConcurrentHashMap<>(), asset, installationPoint));
                for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
                    List<JsonObject> availabilityEventsByDay = parentAvailability.getAvailabilityEventsByDayMap()
                            .computeIfAbsent(entry.getKey(), s -> new CopyOnWriteArrayList<>());
                    availabilityEventsByDay.add(entry.getValue().getAsJsonObject());
                }
            }
            return jo;
        }, ctx.getDbCallbackExecutor()), ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Asset> findAsset(TbContext ctx, EntityId entityId) {
        return ctx.getAssetService().findAssetByIdAsync(ctx.getTenantId(), new AssetId(entityId.getId()));
    }

    private ListenableFuture<JsonObject> processCalculateAvailability(TbContext ctx, Asset installationPoint, List<AttributeKvEntry> ipAttributes) {
        JsonObject result = new JsonObject();

        JsonObject actualAvailabilityEventsObj = parseAttributes(ipAttributes);
        for (LocalDateTime localDateTimeAtStartOfCurrentDay : getSetOfDays(actualAvailabilityEventsObj)) {
            LocalDateTime localDateTimeAtStartOfNextDay = localDateTimeAtStartOfCurrentDay.plusDays(1);
            long startTs = localDateTimeAtStartOfCurrentDay.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
            long endTs = localDateTimeAtStartOfNextDay.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;

            JsonArray requiredAvailabilityIntervals = new JsonArray();
            requiredAvailabilityIntervals.add(getObjectWithTimestamps(startTs, endTs));

            ActualAvailability actualAvailability = getActualAvailability(actualAvailabilityEventsObj, startTs, endTs);

            JsonObject resultObject = new JsonObject();
            resultObject.add("availabilityInterval", getObjectWithTimestamps(startTs, endTs));
            resultObject.addProperty("deliveredAvailability",
                    getDeliveredAvailability(installationPoint.getName(), endTs - startTs, actualAvailability.getTotalAvailable()));
            resultObject.add("actualAvailabilityIntervals", actualAvailability.getActualAvailabilityIntervals());
            resultObject.add("requiredAvailabilityIntervals", requiredAvailabilityIntervals);
            resultObject.addProperty("installationPoint", installationPoint.getName());
            resultObject.addProperty("deliveredAvailabilityMs", actualAvailability.getTotalAvailable());

            saveAvailabilityEvent(ctx, installationPoint, resultObject);

            result.add(String.valueOf(localDateTimeAtStartOfCurrentDay.getDayOfMonth()), resultObject);
        }

        return Futures.transform(updateAttribute(
                ctx,
                installationPoint.getId(),
                ACTUAL_AVAILABILITY_EVENTS,
                updateActualAvailability(installationPoint, actualAvailabilityEventsObj)
        ), v -> result, ctx.getDbCallbackExecutor());
    }

    private Set<LocalDateTime> getSetOfDays(JsonObject actualAvailabilityEventsObj) {
        Set<LocalDateTime> setOfDays = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : actualAvailabilityEventsObj.entrySet()) {
            setOfDays.add(new Timestamp(Long.parseLong(entry.getKey())).toLocalDateTime().toLocalDate().atStartOfDay());
        }
        return setOfDays;
    }

    private ListenableFuture<JsonObject> processCalculateAvailability(TbContext ctx, Asset installationPoint, List<AttributeKvEntry> ipAttributes,
                                                                      EntityId vehicleId, List<AttributeKvEntry> vehicleAttributes) {
        JsonObject result = new JsonObject();

        JsonObject tripsObject = parseAttributes(vehicleAttributes);
        JsonObject actualAvailabilityEventsObj = parseAttributes(ipAttributes);

        ConcurrentMap<Integer, JsonArray> tripsMap = constructTripsMap(vehicleId, tripsObject);
        for (Map.Entry<Integer, JsonArray> entry : tripsMap.entrySet()) {
            JsonArray requiredAvailabilityIntervals = new JsonArray();
            long startTs = 0L;
            long endTs = 0L;

            long totalRequiredAvailability = 0;

            ActualAvailability totalActualAvailability = new ActualAvailability(new JsonArray(), 0L);

            for (JsonElement je : entry.getValue()) {
                JsonObject tripObject = je.getAsJsonObject();

                JsonElement startTimestamp = tripObject.get("start").getAsJsonObject().get("timestamp");
                JsonElement lastTimestamp = tripObject.get("lastUpdate").getAsJsonObject().get("timestamp");

                if (startTs == 0L || startTs > startTimestamp.getAsLong()) {
                    startTs = startTimestamp.getAsLong();
                }
                if (endTs == 0L || endTs < lastTimestamp.getAsLong()) {
                    endTs = lastTimestamp.getAsLong();
                }

                requiredAvailabilityIntervals.add(getRequiredAvailabilityInterval(startTimestamp, lastTimestamp));

                totalRequiredAvailability = getTotal(totalRequiredAvailability, startTimestamp, lastTimestamp);

                ActualAvailability actualAvailability = getActualAvailability(actualAvailabilityEventsObj, startTimestamp.getAsLong(), lastTimestamp.getAsLong());
                totalActualAvailability.getActualAvailabilityIntervals().addAll(actualAvailability.getActualAvailabilityIntervals());
                totalActualAvailability.setTotalAvailable(totalActualAvailability.getTotalAvailable() + actualAvailability.getTotalAvailable());
            }

            if (totalRequiredAvailability == 0) {
                log.info("[{}][{}] Total required availability interval is zero! {}", ctx.getTenantId(), vehicleId, tripsObject);
                continue;
            }

            JsonObject resultObject = new JsonObject();
            resultObject.add("availabilityInterval", getObjectWithTimestamps(startTs, endTs));
            resultObject.addProperty("deliveredAvailability", getDeliveredAvailability(installationPoint.getName(),
                    totalRequiredAvailability, totalActualAvailability.getTotalAvailable()));
            resultObject.add("actualAvailabilityIntervals", totalActualAvailability.getActualAvailabilityIntervals());
            resultObject.add("requiredAvailabilityIntervals", requiredAvailabilityIntervals);
            resultObject.addProperty("installationPoint", installationPoint.getName());
            resultObject.addProperty("deliveredAvailabilityMs", totalActualAvailability.getTotalAvailable());

            saveAvailabilityEvent(ctx, installationPoint, resultObject);
            result.add(entry.getKey().toString(), resultObject);
        }

        return Futures.transform(updateAttribute(
                ctx,
                installationPoint.getId(),
                ACTUAL_AVAILABILITY_EVENTS,
                updateActualAvailability(installationPoint, actualAvailabilityEventsObj)
        ), v -> result, ctx.getDbCallbackExecutor());
    }

    private ConcurrentMap<Integer, JsonArray> constructTripsMap(EntityId vehicleId, JsonObject tripsObject) {
        ConcurrentMap<Integer, JsonArray> tripsMap = new ConcurrentHashMap<>();

        for (Map.Entry<String, JsonElement> entry : tripsObject.entrySet()) {
            JsonObject tripObject = entry.getValue().getAsJsonObject();
            if (tripObject.has("start") && tripObject.has("lastUpdate")) {

                JsonObject startObject = getTripEventObject(tripObject, "start");
                JsonObject lastUpdateObject = getTripEventObject(tripObject, "lastUpdate");

                if (startObject != null && startObject.has("timestamp") &&
                        lastUpdateObject != null && lastUpdateObject.has("timestamp")) {

                    LocalDate startLocalDate = getLocalDate(startObject);
                    LocalDate lastUpdateLocalDate = getLocalDate(lastUpdateObject);

                    if (startLocalDate.getDayOfMonth() != lastUpdateLocalDate.getDayOfMonth()) {
                        log.info("[{}] Start timestamp relates to different day than last update timestamp of a trip: {}", vehicleId, tripObject);

                        long tsAtStartOfNextDay = lastUpdateLocalDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;

                        JsonArray tripObjectsPerFirstDay = tripsMap.computeIfAbsent(startLocalDate.getDayOfMonth(), i -> new JsonArray());
                        tripObjectsPerFirstDay.add(getFirstDayObject(tripObject, tsAtStartOfNextDay));

                        JsonArray tripObjectsPerSecondDay = tripsMap.computeIfAbsent(lastUpdateLocalDate.getDayOfMonth(), i -> new JsonArray());
                        tripObjectsPerSecondDay.add(getSecondDayObject(tripObject, tsAtStartOfNextDay));
                    } else {
                        JsonArray tripObjectsPerDay = tripsMap.computeIfAbsent(startLocalDate.getDayOfMonth(), i -> new JsonArray());
                        tripObjectsPerDay.add(tripObject);
                    }
                }
            }
        }
        return tripsMap;
    }

    private JsonObject getFirstDayObject(JsonObject tripObject, long tsAtStartOfNextDay) {
        JsonObject firstDayObject = new JsonObject();
        firstDayObject.add("tripState", tripObject.get("tripState"));
        firstDayObject.add("start", tripObject.get("start"));
        firstDayObject.add("lastUpdate", getUpdatedObject(tripObject, tsAtStartOfNextDay));
        return firstDayObject;
    }

    private JsonObject getSecondDayObject(JsonObject tripObject, long tsAtStartOfNextDay) {
        JsonObject secondDayObject = new JsonObject();
        secondDayObject.add("tripState", tripObject.get("tripState"));
        secondDayObject.add("start", getUpdatedObject(tripObject, tsAtStartOfNextDay));
        secondDayObject.add("lastUpdate", tripObject.get("lastUpdate"));
        return secondDayObject;
    }

    private JsonObject getUpdatedObject(JsonObject tripObject, long ts) {
        JsonObject jo = new JsonObject();
        jo.addProperty("timestamp", ts);
        jo.add("position", tripObject.get("lastUpdate").getAsJsonObject().get("position"));
        return jo;
    }

    private LocalDate getLocalDate(JsonObject jo) {
        return new Timestamp(jo.get("timestamp").getAsLong()).toLocalDateTime().toLocalDate();
    }

    private void saveAvailabilityEvent(TbContext ctx, Asset asset, JsonObject resultObject) {
        ctx.getTelemetryService().saveAndNotify(ctx.getTenantId(), asset.getId(),
                Collections.singletonList(
                        new BasicTsKvEntry(
                                System.currentTimeMillis(),
                                new JsonDataEntry(AVAILABILITY_TS_KEY, resultObject.toString()))),
                new VixNodeCallback(ctx, null));
    }

    private ActualAvailability getActualAvailability(JsonObject actualAvailabilityEventsObj, long startTs, long endTs) {
        JsonArray actualAvailabilityIntervals = new JsonArray();

        long totalAvailable = 0;

        int counter = 0;
        JsonObject actualAvailabilityInterval = null;

        SkippedActualAvailabilityEvents skippedActualAvailabilityEvents = getEntriesToSkip(actualAvailabilityEventsObj, startTs, endTs);

        int recordsToSkip = getNumberOfRecordsToSkip(skippedActualAvailabilityEvents.getEntriesToSkip());

        for (Map.Entry<String, JsonElement> entry : skippedActualAvailabilityEvents.getUpdatedActualAvailabilityEventsObj().entrySet()) {
            long ts = Long.parseLong(entry.getKey());
            String availabilityValue = entry.getValue().getAsString();

            if (recordsToSkip != 0) {
                recordsToSkip--;
                continue;
            }

            if (ts < startTs) {
                ts = startTs;
            }

            if (counter % 2 == 0) {
                if ("available".equals(availabilityValue)) {
                    actualAvailabilityInterval = new JsonObject();
                    actualAvailabilityInterval.addProperty("startTimestamp", ts);
                } else {
                    // skip "notAvailable" entry on first place
                    if (counter == 0) {
                        continue;
                    }
                }
            } else {
                if ("notAvailable".equals(availabilityValue)) {
                    actualAvailabilityInterval.addProperty("endTimestamp", ts);
                    actualAvailabilityIntervals.add(actualAvailabilityInterval);

                    totalAvailable = getTotal(totalAvailable, actualAvailabilityInterval.get("startTimestamp"), actualAvailabilityInterval.get("endTimestamp"));
                }
            }
            counter++;
        }

        if (actualAvailabilityInterval != null && actualAvailabilityInterval.entrySet().size() != 2) {
            actualAvailabilityInterval.addProperty("endTimestamp", endTs);
            actualAvailabilityIntervals.add(actualAvailabilityInterval);

            totalAvailable = getTotal(totalAvailable, actualAvailabilityInterval.get("startTimestamp"), actualAvailabilityInterval.get("endTimestamp"));
        }
        return new ActualAvailability(actualAvailabilityIntervals, totalAvailable);
    }

    private int getNumberOfRecordsToSkip(List<Map.Entry<String, JsonElement>> entries) {
        int recordsToSkip = 0;
        if (entries.size() > 0) {
            recordsToSkip = entries.size() - 1;
            if (entries.get(recordsToSkip).getValue().getAsString().equals("notAvailable")) {
                recordsToSkip++;
            }
        }
        return recordsToSkip;
    }

    private SkippedActualAvailabilityEvents getEntriesToSkip(JsonObject actualAvailabilityEventsObj, long startTs, long endTs) {
        List<Map.Entry<String, JsonElement>> entries = new ArrayList<>();
        JsonObject updatedActualAvailabilityEventsObj = new JsonObject();
        for (Map.Entry<String, JsonElement> entry : actualAvailabilityEventsObj.entrySet()) {
            long ts = Long.parseLong(entry.getKey());

            if (ts < startTs) {
                entries.add(entry);
            }
            if (ts <= endTs) {
                updatedActualAvailabilityEventsObj.add(entry.getKey(), entry.getValue());
            }
        }
        return new SkippedActualAvailabilityEvents(entries, updatedActualAvailabilityEventsObj);
    }

    private double getDeliveredAvailability(String assetName, long totalRequiredAvailability, long totalAvailable) {
        double deliveredAvailability = new BigDecimal(totalAvailable)
                .divide(new BigDecimal(totalRequiredAvailability), MathContext.DECIMAL32)
                .multiply(new BigDecimal(100))
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
        if (deliveredAvailability < 0) {
            log.info("[{}] Delivered availability {} is negative!", assetName, deliveredAvailability);
            return 0;
        } else if (deliveredAvailability > 100) {
            log.info("[{}] Delivered availability {} is greater than 100!", assetName, deliveredAvailability);
            return 100;
        } else {
            return deliveredAvailability;
        }
    }

    private long getTotal(long total, JsonElement startTimestamp, JsonElement lastTimestamp) {
        total += lastTimestamp.getAsLong() - startTimestamp.getAsLong();
        return total;
    }

    private JsonObject getObjectWithTimestamps(long startTs, long endTs) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("startTimestamp", startTs);
        jsonObject.addProperty("endTimestamp", endTs);
        return jsonObject;
    }

    private JsonObject getRequiredAvailabilityInterval(JsonElement startTimestamp, JsonElement lastTimestamp) {
        JsonObject requiredAvailabilityInterval = new JsonObject();
        requiredAvailabilityInterval.add("startTimestamp", startTimestamp);
        requiredAvailabilityInterval.add("endTimestamp", lastTimestamp);
        return requiredAvailabilityInterval;
    }

    private JsonObject getTripEventObject(JsonObject tripObject, String key) {
        JsonElement element = tripObject.get(key);
        if (element.isJsonObject()) {
            return element.getAsJsonObject();
        }
        return null;
    }

    private void tellSelf(TbContext ctx, List<JsonObject> jsonObjects) {
        int delayMs = 0;
        for (JsonObject jsonObject : jsonObjects) {
            if (jsonObject != null) {
                for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                    TbMsg tbMsg = ctx.newMsg(ServiceQueue.MAIN, TB_MSG_CUSTOM_NODE_MSG, ctx.getTenantId(), new TbMsgMetaData(), entry.getValue().toString());
                    ctx.tellSelf(tbMsg, delayMs);
                    delayMs++;
                }
            }
        }
    }

    private void tellSelf(TbContext ctx, JsonObject jsonObject, String installationPoint) {
        TbMsgMetaData metaData = new TbMsgMetaData();
        metaData.putValue("installationPoint", installationPoint);
        TbMsg tbMsg = ctx.newMsg(ServiceQueue.MAIN, TB_MSG_CUSTOM_NODE_MSG, ctx.getTenantId(), metaData, jsonObject.toString());
        ctx.tellSelf(tbMsg, 0);
    }

    private JsonObject parseAttributes(List<AttributeKvEntry> attributes) {
        if (CollectionUtils.isEmpty(attributes)) {
            return new JsonObject();
        }
        return new JsonParser().parse(attributes.get(0).getValueAsString()).getAsJsonObject();
    }

    private EntityRelationsQuery buildQuery(EntityId originator) {
        EntityRelationsQuery query = new EntityRelationsQuery();
        query.setFilters(constructEntityTypeFilters());
        query.setParameters(new RelationsSearchParameters(originator, EntitySearchDirection.TO, 1, false));
        return query;
    }

    private List<EntityTypeFilter> constructEntityTypeFilters() {
        List<EntityTypeFilter> entityTypeFilters = new ArrayList<>();
        entityTypeFilters.add(createTypeFilter(FROM_STATION_TO_IP_TYPE, Collections.singletonList(EntityType.ASSET)));
        entityTypeFilters.add(createTypeFilter(FROM_VEHICLE_TO_IP_TYPE, Collections.singletonList(EntityType.ASSET)));
        return entityTypeFilters;
    }

    private EntityTypeFilter createTypeFilter(String relationType, List<EntityType> entityTypes) {
        return new EntityTypeFilter(relationType, entityTypes);
    }

    private ListenableFuture<List<AttributeKvEntry>> findAttributes(TbContext ctx, EntityId entityId, String key) {
        return ctx.getAttributesService().find(
                ctx.getTenantId(),
                entityId,
                DataConstants.SERVER_SCOPE,
                Collections.singletonList(key));
    }

    private List<Asset> getInstallationPoints(TbContext ctx) {
        List<Asset> ips = new ArrayList<>();
        PageLink pageLink = new PageLink(ENTITIES_LIMIT);
        while (pageLink != null) {
            PageData<Asset> page = ctx.getAssetService().findAssetsByTenantIdAndType(ctx.getTenantId(), IP_TYPE, pageLink);
            pageLink = page.hasNext() ? pageLink.nextPageLink() : null;
            ips.addAll(page.getData());
        }
        return ips;
    }

    private JsonObject updateActualAvailability(Asset installationPoint, JsonObject actualAvailabilityEventsObj) {
        JsonObject updatedActualAvailabilityEventsObj = new JsonObject();
        String lastKey = null;
        JsonElement lastObject = null;
        for (Map.Entry<String, JsonElement> entry : actualAvailabilityEventsObj.entrySet()) {
            lastKey = entry.getKey();
            lastObject = entry.getValue();
        }
        if (lastKey != null && lastObject != null) {
            updatedActualAvailabilityEventsObj.add(lastKey, lastObject);
        } else {
            log.warn("[{}][{}] Actual availability events attribute is not updated! {}",
                    installationPoint.getTenantId(), installationPoint.getName(), actualAvailabilityEventsObj);
        }
        return updatedActualAvailabilityEventsObj;
    }

    private JsonObject updateTrips(JsonObject tripsObject) {
        JsonObject updatedTripsObject = new JsonObject();
        for (Map.Entry<String, JsonElement> entry : tripsObject.entrySet()) {
            JsonObject tripObject = entry.getValue().getAsJsonObject();
            if (tripObject.has("tripState")) {
                if (!"completed".equals(tripObject.get("tripState").getAsString())) {
                    updatedTripsObject.add(entry.getKey(), tripObject);
                }
            }
        }
        return updatedTripsObject;
    }

    private ListenableFuture<List<Void>> updateAttribute(TbContext ctx, EntityId entityId, String key, JsonObject value) {
        return ctx.getAttributesService().save(
                ctx.getTenantId(),
                entityId,
                DataConstants.SERVER_SCOPE,
                Collections.singletonList(
                        new BaseAttributeKvEntry(
                                new JsonDataEntry(key, value.toString()),
                                System.currentTimeMillis())));
    }

    @Override
    public void destroy() {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class ActualAvailability {
        private JsonArray actualAvailabilityIntervals;
        private long totalAvailable;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class ParentAvailability {
        private ConcurrentMap<String, List<JsonObject>> availabilityEventsByDayMap;
        private Asset parentAsset;
        private String installationPoint;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class SkippedActualAvailabilityEvents {
        private List<Map.Entry<String, JsonElement>> entriesToSkip;
        private JsonObject updatedActualAvailabilityEventsObj;
    }
}