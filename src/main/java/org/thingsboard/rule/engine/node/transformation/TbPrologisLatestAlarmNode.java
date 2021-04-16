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
package org.thingsboard.rule.engine.node.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "prologis latest alarm",
        relationTypes = {"Success", "Not Found"},
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Transform message data to latest alarm data.",
        nodeDetails = "Transform data of incoming message to latest alarm data of message originator by alarm type.\n" +
                "The value of alarm type is in metadata of incoming message in alarmType field.\n" +
                "Node output:\n " +
                "If alarmType field isn't present, or originator of message doesn't have alarm, or alarm of this type is cleared " +
                "then original message is returned with \"Not Found\" relation type. " +
                "Otherwise, a new message is created with alarm data and returned with \"Success\" relation type.",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig")
public class TbPrologisLatestAlarmNode implements TbNode {

    private EmptyNodeConfiguration config;
    private Gson gson;
    private ObjectMapper mapper;
    private JsonParser jsonParser;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
        this.gson = new Gson();
        this.mapper = new ObjectMapper();
        this.jsonParser = new JsonParser();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ListenableFuture<Alarm> alarmListenableFuture = ctx.getAlarmService()
                .findLatestByOriginatorAndType(ctx.getTenantId(), msg.getOriginator(),
                        msg.getMetaData().getValue("alarmType"));
        DonAsynchron.withCallback(alarmListenableFuture, alarm -> {
            if (alarm == null || alarm.getStatus().isCleared()) {
                ctx.tellNext(msg, "Not Found");
            } else {
                JsonObject alarmJson = gson.toJsonTree(alarm).getAsJsonObject();
                try {
                    alarmJson.add("details", jsonParser.parse(mapper.writeValueAsString(alarm.getDetails())));
                } catch (JsonProcessingException e) {
                    ctx.tellFailure(msg, e);
                    return;
                }
                ctx.ack(msg);
                ctx.enqueueForTellNext(TbMsg.newMsg(msg.getType(), msg.getOriginator(), msg.getMetaData(), gson.toJson(alarmJson)), SUCCESS);
            }
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }
}