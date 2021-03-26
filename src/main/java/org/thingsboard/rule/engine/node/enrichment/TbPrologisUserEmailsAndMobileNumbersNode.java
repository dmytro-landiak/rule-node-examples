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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.relation.EntityRelationsQuery;
import org.thingsboard.server.common.data.relation.EntitySearchDirection;
import org.thingsboard.server.common.data.relation.EntityTypeFilter;
import org.thingsboard.server.common.data.relation.RelationsSearchParameters;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;

@Slf4j
@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "prologis user numbers and emails",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Add user numbers and emails to Message Metadata",
        nodeDetails = "Get user notification preference based on the notificationType attribute.\n" +
                "The node pushes messages with type [email] and [sms] with filled Message Metadata with comma-separated list\n" +
                "of emails and mobile numbers.",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbNodeEmptyConfig"
)
public class TbPrologisUserEmailsAndMobileNumbersNode implements TbNode {

    private final static String NOTIFICATION_TYPE = "notificationType";
    private final static String MOBILE_PHONE_NUMBER = "mobilePhoneNumber";
    private final static String TEMPERATURE_UNIT = "temperatureUnit";
    private final static String EMAIL_NOTIFICATION = "email";
    private final static String SMS_NOTIFICATION = "sms";
    private final static String ALL_NOTIFICATION = "all";
    private final static String FAHRENHEIT_SIGN = "F";

    private final static List<String> ATTRIBUTES = Arrays.asList(NOTIFICATION_TYPE, MOBILE_PHONE_NUMBER, TEMPERATURE_UNIT);

    private EmptyNodeConfiguration config;
    private Customer prologis;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
        ctx.getCustomerService().findCustomerByTenantIdAndTitle(ctx.getTenantId(), "Prologis").ifPresent(customer -> prologis = customer);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        EntityRelationsQuery entityRelationsQuery = getEntityRelationsQuery(msg.getOriginator());
        ListenableFuture<List<EntityRelation>> future = ctx.getRelationService().findByQuery(ctx.getTenantId(), entityRelationsQuery);
        ListenableFuture<List<TbMsg>> messagesFuture = Futures.transformAsync(future, entityRelations -> {
            if (!CollectionUtils.isEmpty(entityRelations)) {
                if (prologis != null) {
                    List<UserId> entityIds = entityRelations
                            .stream()
                            .map(EntityRelation::getTo)
                            .map(entityId -> new UserId(entityId.getId()))
                            .collect(Collectors.toList());
                    return Futures.transformAsync(ctx.getUserService().findUsersByTenantIdAndIdsAsync(ctx.getTenantId(), entityIds), users -> {
                        if (!CollectionUtils.isEmpty(users)) {
                            return getMessages(ctx, msg, users);
                        }
                        return Futures.immediateFuture(null);
                    }, ctx.getDbCallbackExecutor());
                }
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(messagesFuture, messages -> {
            ctx.ack(msg);
            if (!CollectionUtils.isEmpty(messages)) {
                for (TbMsg tbMsg : messages) {
                    ctx.enqueueForTellNext(tbMsg, SUCCESS);
                }
            }
        }, throwable -> ctx.tellFailure(msg, throwable));
    }

    private ListenableFuture<List<TbMsg>> getMessages(TbContext ctx, TbMsg msg, List<User> users) {
        return Futures.transform(getUsersData(ctx, users), usersData -> {
            List<TbMsg> messages = new ArrayList<>();
            if (!CollectionUtils.isEmpty(usersData)) {
                for (Map.Entry<String, List<UserData>> usersDataByNotificationTypeEntry : usersData.stream()
                        .collect(Collectors.groupingBy(UserData::getNotificationType))
                        .entrySet()) {
                    for (Map.Entry<String, List<UserData>> usersDataByTempUnitEntry : usersDataByNotificationTypeEntry.getValue()
                            .stream()
                            .collect(Collectors.groupingBy(UserData::getTemperatureUnit))
                            .entrySet()) {
                        if (!CollectionUtils.isEmpty(usersDataByTempUnitEntry.getValue())) {
                            TbMsgMetaData metaData = msg.getMetaData().copy();
                            metaData.putValue("fullNames", usersDataByTempUnitEntry.getValue()
                                    .stream()
                                    .map(UserData::getFullName)
                                    .collect(Collectors.joining(",")));
                            metaData.putValue("temperatureUnit", usersDataByTempUnitEntry.getKey());
                            String sourcesOfCommunications = usersDataByTempUnitEntry.getValue()
                                    .stream()
                                    .map(UserData::getSourceOfCommunicateWay)
                                    .collect(Collectors.joining(","));
                            switch (usersDataByNotificationTypeEntry.getKey()) {
                                case EMAIL_NOTIFICATION:
                                    metaData.putValue("emails", sourcesOfCommunications);
                                    messages.add(TbMsg.newMsg(EMAIL_NOTIFICATION, msg.getOriginator(), metaData, msg.getData()));
                                    break;
                                case SMS_NOTIFICATION:
                                    metaData.putValue("mobilePhoneNumbers", sourcesOfCommunications);
                                    messages.add(TbMsg.newMsg(SMS_NOTIFICATION, msg.getOriginator(), metaData, msg.getData()));
                                    break;
                            }
                        }
                    }
                }
            }
            return messages;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<UserData>> getUsersData(TbContext ctx, List<User> users) {
        List<ListenableFuture<List<UserData>>> userDataListFutures = new ArrayList<>();
        for (User user : users) {
            userDataListFutures.add(Futures.transform(ctx.getAttributesService()
                    .find(ctx.getTenantId(), user.getId(), DataConstants.SERVER_SCOPE, ATTRIBUTES), attrs -> {
                if (CollectionUtils.isEmpty(attrs)) {
                    return null;
                }
                Map<String, String> attributesMap = attrs.stream().collect(Collectors.toMap(KvEntry::getKey, KvEntry::getValueAsString));
                String notificationType = attributesMap.get(NOTIFICATION_TYPE);
                String fullName = user.getFirstName() + " " + user.getLastName();
                String temperatureUnit = attributesMap.getOrDefault(TEMPERATURE_UNIT, FAHRENHEIT_SIGN);
                String mobileNumber = attributesMap.get(MOBILE_PHONE_NUMBER);
                if (notificationType != null) {
                    switch (notificationType) {
                        case EMAIL_NOTIFICATION:
                            return Collections.singletonList(new UserData(fullName, EMAIL_NOTIFICATION, user.getEmail(), temperatureUnit));
                        case SMS_NOTIFICATION:
                            return Collections.singletonList(new UserData(fullName, SMS_NOTIFICATION, mobileNumber, temperatureUnit));
                        case ALL_NOTIFICATION:
                            return Arrays.asList(new UserData(fullName, EMAIL_NOTIFICATION, user.getEmail(), temperatureUnit),
                                    new UserData(fullName, SMS_NOTIFICATION, mobileNumber, temperatureUnit));
                        default:
                            log.warn("User with email = {} doesn't have valid notification type[{}]", user.getEmail(), notificationType);
                    }
                } else {
                    log.warn("User with email = {} doesn't have notificationType attribute", user.getEmail());
                }
                return null;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(userDataListFutures), usersDataList -> {
            if (CollectionUtils.isEmpty(usersDataList)) {
                return new ArrayList<>();
            }
            return usersDataList.stream()
                    .filter(list -> !CollectionUtils.isEmpty(list))
                    .flatMap(Collection::stream)
                    .filter(Objects::nonNull)
                    .filter(userData -> userData.getSourceOfCommunicateWay() != null)
                    .collect(Collectors.toList());
        }, ctx.getDbCallbackExecutor());
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId) {
        RelationsSearchParameters relationsSearchParameters = new RelationsSearchParameters(originatorId,
                EntitySearchDirection.TO, 10, false);

        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(Arrays.asList(new EntityTypeFilter("FromCustomerToTech", Collections.singletonList(EntityType.CUSTOMER)),
                new EntityTypeFilter("FromCustomerToManager", Collections.singletonList(EntityType.CUSTOMER))));
        return entityRelationsQuery;
    }

    @Override
    public void destroy() {
    }

    @Data
    @AllArgsConstructor
    private static class UserData {
        private String fullName;
        private String notificationType;
        private String sourceOfCommunicateWay;
        private String temperatureUnit;
    }
}