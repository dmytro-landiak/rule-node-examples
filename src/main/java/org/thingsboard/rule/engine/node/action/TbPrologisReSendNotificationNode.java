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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.alarm.AlarmQuery;
import org.thingsboard.server.common.data.alarm.AlarmStatus;
import org.thingsboard.server.common.data.asset.Asset;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.page.TimePageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis resend notification",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisReSendNotificationNode implements TbNode {

    private static final String PROJECT = "PROJECT";
    private static final String SERVER_SCOPE = "SERVER_SCOPE";
    private static final String RESEND_INTERVAL = "resendNotificationInterval";
    private static final String ENABLE_RESEND = "enableResendNotification";
    private static final String LAST_RESEND_NOTIFICATION_TS = "lastResendNotificationTs";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        DonAsynchron.withCallback(getUpdatedAlarms(ctx), alarms -> {
            ctx.ack(msg);
            if (!CollectionUtils.isEmpty(alarms)) {
                for (Alarm alarm : alarms) {
                    try {
                        ctx.enqueueForTellNext(TbMsg.newMsg(msg.getType(), alarm.getOriginator(), msg.getMetaData(), MAPPER.writeValueAsString(alarm)), SUCCESS);
                    } catch (JsonProcessingException e) {
                        ctx.tellFailure(TbMsg.newMsg(msg.getType(), alarm.getOriginator(), msg.getMetaData(), msg.getData()), e);
                    }
                }
            }
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    private ListenableFuture<List<Alarm>> getUpdatedAlarms(TbContext ctx) {
        return Futures.transformAsync(ctx.getAttributesService()
                        .find(ctx.getTenantId(), ctx.getTenantId(), SERVER_SCOPE, Arrays.asList(RESEND_INTERVAL, ENABLE_RESEND)),
                attrs -> {
                    if (!CollectionUtils.isEmpty(attrs)) {
                        Optional<Boolean> enableResend = getAttributeValue(ENABLE_RESEND, attrs).getBooleanValue();
                        Optional<Long> resendInterval = getAttributeValue(RESEND_INTERVAL, attrs).getLongValue();
                        if (enableResend.isPresent() && enableResend.get() && resendInterval.isPresent()) {
                            List<Asset> projects = ctx.getAssetService()
                                    .findAssetsByTenantIdAndType(ctx.getTenantId(), PROJECT, new PageLink(1000))
                                    .getData();
                            List<ListenableFuture<List<Alarm>>> updatedAlarmsList = new ArrayList<>();
                            for (Asset project : projects) {
                                updatedAlarmsList.add(updateAlarms(ctx, project, resendInterval.get()));
                            }
                            return Futures.transform(Futures.allAsList(updatedAlarmsList), updatedAlarmsForProjects -> {
                                if (!CollectionUtils.isEmpty(updatedAlarmsForProjects)) {
                                    return updatedAlarmsForProjects.stream().flatMap(Collection::stream).collect(Collectors.toList());
                                }
                                return null;
                            }, ctx.getDbCallbackExecutor());
                        }
                    } else {
                        log.warn("Didn't find attrs : {}", Arrays.asList(RESEND_INTERVAL, ENABLE_RESEND));
                    }
                    return Futures.immediateFuture(null);
                }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<Alarm>> updateAlarms(TbContext ctx, Asset project, long resendInterval) {
        return Futures.transform(ctx.getAlarmService()
                .findAlarms(ctx.getTenantId(), getAlarmQuery(project.getId())), alarmInfos -> {
            List<Alarm> alarms = new ArrayList<>();
            if (alarmInfos == null) {
                log.info("Didn't find alarms for Project [name = {}]", project.getName());
            } else {
                for (AlarmInfo alarmInfo : alarmInfos.getData()) {
                    long lastResendNotificationTs = alarmInfo.getDetails().has(LAST_RESEND_NOTIFICATION_TS) ?
                            alarmInfo.getDetails().get(LAST_RESEND_NOTIFICATION_TS).asLong() : alarmInfo.getCreatedTime();

                    if ((System.currentTimeMillis() - lastResendNotificationTs) / (resendInterval * 60000) > 0) {
                        alarmInfo.setDetails(((ObjectNode) alarmInfo.getDetails()).put(LAST_RESEND_NOTIFICATION_TS, System.currentTimeMillis()));
                        alarms.add(ctx.getAlarmService().createOrUpdateAlarm(alarmInfo));
                    }
                }
            }
            return alarms;
        }, ctx.getDbCallbackExecutor());
    }

    private KvEntry getAttributeValue(String attributeName, List<? extends KvEntry> attributes) {
        return attributes
                .stream()
                .filter(kv -> kv.getKey().equals(attributeName))
                .findFirst()
                .orElse(null);
    }

    private AlarmQuery getAlarmQuery(EntityId entityId) {
        return new AlarmQuery(entityId,
                new TimePageLink(new PageLink(1000), 0L, System.currentTimeMillis()),
                null, AlarmStatus.ACTIVE_UNACK, false, null);
    }

    @Override
    public void destroy() {

    }
}
