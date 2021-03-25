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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DonAsynchron;
import org.thingsboard.rule.engine.api.EmptyNodeConfiguration;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.TbRelationTypes;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.alarm.AlarmQuery;
import org.thingsboard.server.common.data.alarm.AlarmSearchStatus;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.TimePageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "vix clear alarms",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "",
        nodeDetails = "",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig",
        icon = "notifications_off")
public class TbVixClearAlarmsNode implements TbNode {

    private final ObjectMapper mapper = new ObjectMapper();

    EmptyNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        ListenableFuture<PageData<AlarmInfo>> future = ctx.getAlarmService().findAlarms(ctx.getTenantId(), getQuery(msg));
        ListenableFuture<Void> resultFuture = Futures.transformAsync(future, pageData -> {
            List<ListenableFuture<Boolean>> futures = new ArrayList<>();
            if (pageData != null) {
                for (AlarmInfo alarmInfo : pageData.getData()) {
                    futures.add(clearAlarm(ctx, alarmInfo));
                    String data = mapper.valueToTree(alarmInfo).toString();
                    TbMsg tbMsg = TbMsg.newMsg("ALARM", msg.getOriginator(), msg.getMetaData(), data);
                    ctx.enqueueForTellNext(tbMsg, TbRelationTypes.SUCCESS);
                }
            }
            return Futures.transform(Futures.allAsList(futures), l -> null, ctx.getDbCallbackExecutor());
        }, ctx.getDbCallbackExecutor());

        DonAsynchron.withCallback(resultFuture,
                v -> ctx.getPeContext().ack(msg),
                throwable -> ctx.tellFailure(msg, throwable));
    }

    @Override
    public void destroy() {

    }

    private ListenableFuture<Boolean> clearAlarm(TbContext ctx, AlarmInfo alarm) {
        return ctx.getAlarmService().clearAlarm(ctx.getTenantId(), alarm.getId(), alarm.getDetails(), System.currentTimeMillis());
    }

    private AlarmQuery getQuery(TbMsg msg) {
        return new AlarmQuery(msg.getOriginator(), getPageLink(), AlarmSearchStatus.ACTIVE, null, null, null);
    }

    private TimePageLink getPageLink() {
        return new TimePageLink(1000);
    }
}
