package org.thingsboard.rule.engine.node.transformation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "prologis latest alarm",
        relationTypes = {"Success", "Not Found"},
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisLatestAlarmNode implements TbNode {

    private EmptyNodeConfiguration config;
    private Gson gson;

    private static final String ALARM_TYPE_SUFFIX = " Alarm";

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
        this.gson = new Gson();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ListenableFuture<Alarm> alarmListenableFuture = ctx.getAlarmService()
                .findLatestByOriginatorAndType(ctx.getTenantId(), msg.getOriginator(),
                        msg.getMetaData().getValue("alarmType") + ALARM_TYPE_SUFFIX);
        DonAsynchron.withCallback(alarmListenableFuture, alarm -> {
            if (alarm == null || alarm.getStatus().isCleared()) {
                ctx.tellNext(msg, "Not Found");
            } else {
                ctx.ack(msg);
                ctx.tellSuccess(TbMsg.newMsg(msg.getType(), msg.getOriginator(), msg.getMetaData(), gson.toJson(alarm)));
            }
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }
}