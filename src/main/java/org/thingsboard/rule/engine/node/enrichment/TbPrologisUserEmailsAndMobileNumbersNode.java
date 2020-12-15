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
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private final static String EMAIL_NOTIFICATION = "email";
    private final static String SMS_NOTIFICATION = "sms";
    private final static String ALL_NOTIFICATION = "all";

    private final static List<String> ATTRIBUTES = Arrays.asList(NOTIFICATION_TYPE, MOBILE_PHONE_NUMBER);

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
        ListenableFuture<Void> resFuture = Futures.transformAsync(future, entityRelations -> {
            if (!CollectionUtils.isEmpty(entityRelations)) {
                if (prologis != null) {
                    List<UserId> entityIds = entityRelations
                            .stream()
                            .map(EntityRelation::getTo)
                            .map(entityId -> new UserId(entityId.getId()))
                            .collect(Collectors.toList());
                    return Futures.transformAsync(ctx.getUserService().findUsersByTenantIdAndIdsAsync(ctx.getTenantId(), entityIds), users -> {
                        if (!CollectionUtils.isEmpty(users)) {
                            List<String> emails = new ArrayList<>();
                            List<String> mobileNumbers = new ArrayList<>();
                            Map<User, ListenableFuture<List<AttributeKvEntry>>> usersAttributes = getUsersAttributes(ctx, users);
                            List<ListenableFuture<Void>> updateEmailsAndMobileNumbersFutures = new ArrayList<>();
                            for (Map.Entry<User, ListenableFuture<List<AttributeKvEntry>>> entry : usersAttributes.entrySet()) {
                                updateEmailsAndMobileNumbersFutures.add(Futures.transform(entry.getValue(), attributeKvEntries -> {
                                    updateEmailsAndPhoneNumbers(entry.getKey(), attributeKvEntries, emails, mobileNumbers);
                                    return null;
                                }, ctx.getDbCallbackExecutor()));
                            }
                            return Futures.transform(Futures.allAsList(updateEmailsAndMobileNumbersFutures), voids -> {
                                sendMessages(msg, ctx, emails, mobileNumbers);
                                return null;
                            }, ctx.getDbCallbackExecutor());
                        }
                        return Futures.immediateFuture(null);
                    }, ctx.getDbCallbackExecutor());
                }
            }
            return Futures.immediateFuture(null);
        }, ctx.getDbCallbackExecutor());
        DonAsynchron.withCallback(resFuture, v -> ctx.tellSuccess(msg), throwable -> ctx.tellFailure(msg, throwable));
    }

    private void updateEmailsAndPhoneNumbers(User user, List<AttributeKvEntry> attributeKvEntries, List<String> emails, List<String> mobileNumbers) {
        if (CollectionUtils.isEmpty(attributeKvEntries)) {
            return;
        }
        Map<String, String> attributesMap = getAttributesMap(attributeKvEntries);
        if (attributesMap.containsKey(NOTIFICATION_TYPE)) {
            switch (attributesMap.get(NOTIFICATION_TYPE)) {
                case EMAIL_NOTIFICATION:
                    emails.add(user.getEmail());
                    break;
                case SMS_NOTIFICATION:
                    addToMobileNumbersList(attributesMap, user, mobileNumbers);
                    break;
                case ALL_NOTIFICATION:
                    addToMobileNumbersList(attributesMap, user, mobileNumbers);
                    emails.add(user.getEmail());
                    break;
                default:
                    log.warn("User with email = {} doesn't have valid notification type", user.getEmail());
            }
        } else {
            log.warn("User with email = {} doesn't have notificationType attribute", user.getEmail());
        }
    }

    private Map<User, ListenableFuture<List<AttributeKvEntry>>> getUsersAttributes(TbContext ctx, List<User> users) {
        Map<User, ListenableFuture<List<AttributeKvEntry>>> usersAttributes = new HashMap<>();
        for (User user : users) {
            ListenableFuture<List<AttributeKvEntry>> attributesFuture =
                    ctx.getAttributesService().find(ctx.getTenantId(), user.getId(), "SERVER_SCOPE", ATTRIBUTES);
            usersAttributes.put(user, attributesFuture);
        }
        return usersAttributes;
    }

    private void sendMessages(TbMsg msg, TbContext ctx, List<String> emails, List<String> mobileNumbers) {
        ctx.ack(msg);
        if (!emails.isEmpty()) {
            TbMsgMetaData metaData = msg.getMetaData().copy();
            metaData.putValue("emails", String.join(",", emails));
            ctx.tellSuccess(TbMsg.newMsg(EMAIL_NOTIFICATION, msg.getOriginator(), metaData, msg.getData()));
        }
        if (!mobileNumbers.isEmpty()) {
            TbMsgMetaData metaData = msg.getMetaData().copy();
            metaData.putValue("mobilePhoneNumbers", String.join(",", mobileNumbers));
            ctx.tellSuccess(TbMsg.newMsg(SMS_NOTIFICATION, msg.getOriginator(), metaData, msg.getData()));
        }
    }

    private Map<String, String> getAttributesMap(List<AttributeKvEntry> attributeKvEntries) {
        Map<String, String> resMap = new HashMap<>();
        for (AttributeKvEntry attributeKvEntry : attributeKvEntries) {
            resMap.put(attributeKvEntry.getKey(), attributeKvEntry.getValueAsString());
        }
        return resMap;
    }

    private void addToMobileNumbersList(Map<String, String> attributesMap, User user, List<String> mobileNumbers) {
        if (!attributesMap.containsKey(MOBILE_PHONE_NUMBER)) {
            log.warn("User with email = {} doesn't have mobilePhoneNumber attribute", user.getEmail());
            return;
        }
        String phoneNumber = attributesMap.get(MOBILE_PHONE_NUMBER);
        if (phoneNumber != null && !phoneNumber.isEmpty()) {
            mobileNumbers.add(phoneNumber);
        } else {
            log.warn("User with email = {} have empty mobilePhoneNumber attribute", user.getEmail());
        }
    }

    private EntityRelationsQuery getEntityRelationsQuery(EntityId originatorId) {
        RelationsSearchParameters relationsSearchParameters = new RelationsSearchParameters(originatorId,
                EntitySearchDirection.TO, 4, true);
        EntityTypeFilter entityTypeFilter = new EntityTypeFilter("FromCustomerToTech", Collections.singletonList(EntityType.CUSTOMER));

        EntityRelationsQuery entityRelationsQuery = new EntityRelationsQuery();
        entityRelationsQuery.setParameters(relationsSearchParameters);
        entityRelationsQuery.setFilters(Collections.singletonList(entityTypeFilter));
        return entityRelationsQuery;
    }

    @Override
    public void destroy() {
    }

}