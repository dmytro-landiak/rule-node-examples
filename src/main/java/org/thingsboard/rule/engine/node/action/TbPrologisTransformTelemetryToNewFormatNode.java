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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.IdBased;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.BooleanDataEntry;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis migration",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Migrate telemetry data from old json format to new separated",
        nodeDetails = "Get all devices and their keys with old json format and migrate to separated key-value pairs",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbPrologisActionMigrationNodeConfig")
public class TbPrologisTransformTelemetryToNewFormatNode implements TbNode {

    private static final String DEVICE_TYPE = "DATA_DEVICE";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TbPrologisTransformTelemetryToNewFormatNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbPrologisTransformTelemetryToNewFormatNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ctx.getPeContext().ack(msg);
        List<ListenableFuture<List<Void>>> resFutures = new ArrayList<>();
        List<AtomicLong> totalCounts = new ArrayList<>();
        int pageNumber = 0;
        while (true) {
            PageData<Device> devicesPageData = getDevices(ctx, pageNumber, config.getCountOfDevicesAtATime());
            TotalCountResFutures totalCountResFutures = transformTelemetryToNewFormat(ctx, msg,
                    devicesPageData
                            .getData()
                            .stream()
                            .map(IdBased::getId)
                            .collect(Collectors.toList()));
            resFutures.addAll(totalCountResFutures.getResFutures());
            totalCounts.add(totalCountResFutures.getTotalCount());
            if (devicesPageData.hasNext()) {
                pageNumber++;
            } else {
                break;
            }
        }
        DonAsynchron.withCallback(getFutureOfList(ctx, resFutures),
                voids -> {
                    Optional<Long> totalCount = totalCounts.stream().map(AtomicLong::get).reduce(Long::sum);
                    log.info("Finished migration, total count of records: {}", totalCount.get());
                    msg.getMetaData().putValue("totalCount", String.valueOf(totalCount.get()));
                    ctx.tellSuccess(msg);
                },
                throwable -> {
                    log.error("Failure occurred during migration!", throwable);
                    ctx.tellFailure(msg, throwable);
                });
    }

    @Override
    public void destroy() {

    }

    private TotalCountResFutures transformTelemetryToNewFormat(TbContext ctx, TbMsg msg, List<DeviceId> deviceIds) {
        log.info("Found {} devices! Starting migration...", deviceIds.size());
        AtomicLong totalCount = new AtomicLong();
        List<ListenableFuture<List<Void>>> resFutures = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        for (DeviceId deviceId : deviceIds) {
            ListenableFuture<List<String>> keysFuture = getTelemetryKeys(ctx, deviceId);
            resFutures.add(Futures.transformAsync(keysFuture, keys -> {
                log.info("[{}] Fetched telemetry keys {}...", deviceId, keys);
                List<ListenableFuture<List<Void>>> saveDeviceKeysFutures = new ArrayList<>();
                if (!CollectionUtils.isEmpty(keys)) {
                    for (String key : keys) {
                        long startTs = 0L;

                        final SimpleListenableFuture<CopyResponse> copyResponseFuture = new SimpleListenableFuture<>();
                        constructEntriesToCopy(ctx, copyResponseFuture, new CopyOnWriteArrayList<>(),
                                new CopyParams(deviceId, key, startTs, currentTime), totalCount);

                        saveDeviceKeysFutures.add(Futures.transformAsync(copyResponseFuture, copyResponse -> {
                            if (copyResponse != null && !CollectionUtils.isEmpty(copyResponse.getEntriesToCopy())) {
                                List<ListenableFuture<Integer>> savedFutures = new ArrayList<>();
                                List<TsKvEntry> entriesToSave = new ArrayList<>();
                                int count = 0;
                                for (TsKvEntry entry : copyResponse.getEntriesToCopy()) {
                                    entriesToSave.add(entry);
                                    count++;
                                    if (count == 1000) {
                                        savedFutures.add(saveTelemetry(ctx, deviceId, entriesToSave));
                                        count = 0;
                                        entriesToSave = new ArrayList<>();
                                    }
                                }
                                if (entriesToSave.size() > 0) {
                                    savedFutures.add(saveTelemetry(ctx, deviceId, entriesToSave));
                                }
                                return Futures.transform(Futures.allAsList(savedFutures), list -> {
                                    if (!CollectionUtils.isEmpty(list)) {
                                        AtomicInteger totalSaved = new AtomicInteger(0);
                                        list.forEach(totalSaved::addAndGet);
                                        log.info("[{}] Saved {} for key: {}", deviceId, totalSaved, key);
                                    }
                                    return null;
                                }, ctx.getDbCallbackExecutor());
                            }
                            return Futures.immediateFuture(null);
                        }, ctx.getDbCallbackExecutor()));
                    }
                }
                return getFutureOfList(ctx, saveDeviceKeysFutures);
            }, ctx.getDbCallbackExecutor()));
        }
        return new TotalCountResFutures(totalCount,resFutures);
    }

    private ListenableFuture<Integer> saveTelemetry(TbContext ctx, DeviceId deviceId, List<TsKvEntry> entriesToSave) {
        return ctx.getTimeseriesService().save(ctx.getTenantId(), deviceId, entriesToSave, 0L);
    }

    private void constructEntriesToCopy(TbContext ctx, final SimpleListenableFuture<CopyResponse> resultFuture,
                                        List<TsKvEntry> entriesToCopy, CopyParams params, AtomicLong totalCount) {
        ListenableFuture<List<TsKvEntry>> tsKvEntriesFuture = ctx.getTimeseriesService()
                .findAll(ctx.getTenantId(), params.getDeviceId(),
                        Collections.singletonList(new BaseReadTsKvQuery(params.getKey(), params.getStartTs(), params.getEndTs(), 1000, "ASC")));
        ListenableFuture<Long> lastProcessedTsFuture = Futures.transform(tsKvEntriesFuture, tsKvEntries -> {
            if (!CollectionUtils.isEmpty(tsKvEntries)) {
                long lastProcessedTs = 0L;
                for (TsKvEntry entry : tsKvEntries) {
                    lastProcessedTs = entry.getTs();
                    List<TsKvEntry> tempTsKvEntriesNewFormat = getNewFormatTsKvEntries(entry);
                    if (!CollectionUtils.isEmpty(tempTsKvEntriesNewFormat)) {
                        entriesToCopy.addAll(tempTsKvEntriesNewFormat);
                    } else {
                        log.info("[{}] Didn't transform {} to new format...", params.getDeviceId(), entry);
                    }
                }

                int size = tsKvEntries.size();
                log.info("[{}] Migrated {} telemetries for {}...", params.getDeviceId(), size, params.getKey());
                totalCount.addAndGet(size);

                if (size < 1000) {
                    return null;
                }
                return lastProcessedTs;
            } else {
                return null;
            }
        }, ctx.getDbCallbackExecutor());

        Futures.addCallback(lastProcessedTsFuture, new FutureCallback<Long>() {
            @Override
            public void onSuccess(@Nullable Long lastProcessedTs) {
                if (lastProcessedTs == null) {
                    resultFuture.set(new CopyResponse(entriesToCopy));
                } else {
                    params.setStartTs(lastProcessedTs);
                    constructEntriesToCopy(ctx, resultFuture, entriesToCopy, params, totalCount);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("[{}][{}] Failed to get entries to copy!", params.getDeviceId(), params.getKey(), t);
            }
        }, ctx.getDbCallbackExecutor());
    }


    private ListenableFuture<List<Void>> getFutureOfList(TbContext ctx, List<ListenableFuture<List<Void>>> list) {
        return Futures.transform(Futures.successfulAsList(list), saveTsList -> null, ctx.getDbCallbackExecutor());
    }

    private List<TsKvEntry> getNewFormatTsKvEntries(TsKvEntry kvEntry) {
        Optional<String> jsonValue = kvEntry.getJsonValue();
        if (jsonValue.isPresent()) {
            try {
                List<TsKvEntry> newTsKvEntries = new ArrayList<>();
                JsonNode oldValue = OBJECT_MAPPER.readTree(jsonValue.get());
                Iterator<String> fieldNames = oldValue.fieldNames();
                while (fieldNames.hasNext()) {
                    String suffix = fieldNames.next();
                    String newKey = kvEntry.getKey() + "_" + suffix;
                    JsonNode value = oldValue.get(suffix);
                    KvEntry newKvEntry;
                    if (value.isBoolean()) {
                        newKvEntry = new BooleanDataEntry(newKey, value.asBoolean());
                    } else if (value.isDouble()) {
                        newKvEntry = new DoubleDataEntry(newKey, value.asDouble());
                    } else if (value.isLong()) {
                        newKvEntry = new LongDataEntry(newKey, value.asLong());
                    } else {
                        newKvEntry = new StringDataEntry(newKey, value.asText());
                    }
                    newTsKvEntries.add(new BasicTsKvEntry(kvEntry.getTs(), newKvEntry));
                }
                return newTsKvEntries;
            } catch (IOException e) {
                log.warn("Can't transform old Ts to new format Ts for {}", kvEntry, e);
            }
        }
        return null;
    }

    private ListenableFuture<List<String>> getTelemetryKeys(TbContext ctx, DeviceId deviceId) {
        ListenableFuture<List<TsKvEntry>> allLatest = ctx.getTimeseriesService().findAllLatest(ctx.getTenantId(), deviceId);
        return Futures.transform(allLatest, values -> {
            if (!CollectionUtils.isEmpty(values)) {
                return values
                        .stream()
                        .map(KvEntry::getKey)
                        .filter(key -> !(key.endsWith("_VALUE") || key.endsWith("_UOM")))
                        .collect(Collectors.toList());
            }
            return new ArrayList<>();
        }, ctx.getDbCallbackExecutor());
    }

    private List<DeviceId> getAllDevicesIds(TbContext ctx) {
        List<DeviceId> deviceIds = new ArrayList<>();
        int pageNumber = 0;
        while (true) {
            PageData<Device> pageData = ctx.getDeviceService()
                    .findDevicesByTenantIdAndType(ctx.getTenantId(), DEVICE_TYPE, new PageLink(1000, pageNumber));
            deviceIds.addAll(pageData.getData().stream().map(IdBased::getId).collect(Collectors.toList()));
            if (!pageData.hasNext()) {
                break;
            }
            pageNumber++;
        }
        return deviceIds;
    }

    private PageData<Device> getDevices(TbContext ctx, int pageNumber, int pageSize) {
        return ctx.getDeviceService()
                .findDevicesByTenantIdAndType(ctx.getTenantId(), DEVICE_TYPE, new PageLink(pageSize, pageNumber));
    }

    @Data
    @AllArgsConstructor
    private static class TotalCountResFutures {
        private AtomicLong totalCount;
        private List<ListenableFuture<List<Void>>> resFutures;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class CopyParams {
        private DeviceId deviceId;
        private String key;
        private long startTs;
        private long endTs;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class CopyResponse {
        private List<TsKvEntry> entriesToCopy;
    }
}
