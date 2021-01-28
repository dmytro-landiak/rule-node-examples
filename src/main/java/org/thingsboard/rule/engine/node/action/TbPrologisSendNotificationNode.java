package org.thingsboard.rule.engine.node.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.*;
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

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "prologis send notification",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisSendNotificationNode implements TbNode {

    private static final String PROJECT = "PROJECT";
    private static final String SERVER_SCOPE = "SERVER_SCOPE";
    private static final String RESEND_INTERVAL = "resendNotificationInterval";
    private static final String ENABLE_RESEND = "enableResendNotification";
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
                        ctx.tellSuccess(TbMsg.newMsg(msg.getType(), msg.getOriginator(), msg.getMetaData(), MAPPER.writeValueAsString(alarm)));
                    } catch (JsonProcessingException e) {
                        ctx.tellFailure(TbMsg.newMsg(msg.getType(), msg.getOriginator(), msg.getMetaData(), msg.getData()), e);
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
                            for (Asset project : projects) {
                                return updateAlarms(ctx, project, resendInterval.get());
                            }
                        }
                    } else {
                        log.warn("Didn't find attrs : {}", Arrays.asList(RESEND_INTERVAL, ENABLE_RESEND));
                    }
                    return Futures.immediateFuture(new ArrayList<>());
                }
                , ctx.getDbCallbackExecutor());
    }


    private ListenableFuture<List<Alarm>> updateAlarms(TbContext ctx, Asset project, long resendInterval) {
        return Futures.transform(ctx.getAlarmService()
                .findAlarms(ctx.getTenantId(), getAlarmQuery(project.getId())), alarmInfos -> {
            List<Alarm> alarms = new ArrayList<>();
            if (alarmInfos == null) {
                log.info("Didn't find alarms for Project[name={}]", project.getName());
            } else {
                for (AlarmInfo alarmInfo : alarmInfos.getData()) {
                    long lastResendNotificationTs = alarmInfo.getDetails().has("lastResendNotificationTs") ?
                            alarmInfo.getDetails().get("lastResendNotificationTs").asLong() : alarmInfo.getCreatedTime();

                    if ((System.currentTimeMillis() - lastResendNotificationTs) / (resendInterval * 60000) > 0) {
                        alarmInfo.setDetails(((ObjectNode) alarmInfo.getDetails()).put("lastResendNotificationTs", System.currentTimeMillis()));
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
