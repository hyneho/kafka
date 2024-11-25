/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.modern.ModernGroupMember.hasAssignedPartitionsChanged;

public class ConsumerGroupMetadataManager {

    private final GroupStore groupStore;
    private final GroupMetaManagerHelper helper;

    /**
     * The default consumer group assignor used.
     */
    private final ConsumerGroupPartitionAssignor defaultConsumerGroupAssignor;

    /**
     * The supported consumer group partition assignors keyed by their name.
     */
    private final Map<String, ConsumerGroupPartitionAssignor> consumerGroupAssignors;

    /**
     * The metadata refresh interval.
     */
    private final int consumerGroupMetadataRefreshIntervalMs;

    /**
     * The maximum number of members allowed in a single consumer group.
     */
    private final int consumerGroupMaxSize;

    /**
     * The default heartbeat interval for consumer groups.
     */
    private final int consumerGroupHeartbeatIntervalMs;

    /**
     * Initial rebalance delay for members joining a classic group.
     */
    private final int classicGroupInitialRebalanceDelayMs;

    /**
     * The maximum number of members allowed in a single classic group.
     */
    private final int classicGroupMaxSize;

    /**
     * The config indicating whether group protocol upgrade/downgrade is allowed.
     */
    private final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy;

    /**
     * The group manager.
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * The default session timeout for consumer groups.
     */
    private final int consumerGroupSessionTimeoutMs;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;


    public ConsumerGroupMetadataManager(
        GroupStore groupStore,
        List<ConsumerGroupPartitionAssignor> consumerGroupAssignors,
        int consumerGroupMetadataRefreshIntervalMs,
        int classicGroupInitialRebalanceDelayMs,
        int consumerGroupMaxSize,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupSessionTimeoutMs,
        int classicGroupMaxSize,
        GroupConfigManager groupConfigManager,
        ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy,
        LogContext logContext,
        GroupCoordinatorMetricsShard metrics,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer) {
        this.groupStore = groupStore;
        this.consumerGroupAssignors = consumerGroupAssignors.stream().collect(Collectors.toMap(ConsumerGroupPartitionAssignor::name, Function.identity()));
        this.defaultConsumerGroupAssignor = consumerGroupAssignors.get(0);
        this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
        this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.classicGroupMaxSize = classicGroupMaxSize;
        this.groupConfigManager = groupConfigManager;
        this.consumerGroupMigrationPolicy = consumerGroupMigrationPolicy;
        this.metrics = metrics;
        this.logContext = logContext;
        this.log = this.logContext.logger(ShareGroupMetadataManager.class);
        this.time = time;
        this.timer = timer;
        this.helper = new GroupMetaManagerHelper(groupStore, time, timer, logContext, log, consumerGroupMigrationPolicy, classicGroupMaxSize, classicGroupInitialRebalanceDelayMs);
    }

    /**
     * Handles a ConsumerGroupDescribe request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ConsumerGroupDescribeResponseData.DescribedGroup.
     */
    public List<ConsumerGroupDescribeResponseData.DescribedGroup> consumerGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<ConsumerGroupDescribeResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                describedGroups.add(helper.consumerGroup(groupId, committedOffset).asDescribedGroup(
                    committedOffset,
                    defaultConsumerGroupAssignor.name(),
                    groupStore.image().topics()
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Handles a ConsumerGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ConsumerGroupHeartbeat request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) throws ApiException {
        throwIfConsumerGroupHeartbeatRequestIsInvalid(request, context.apiVersion());

        if (request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH || request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            // -2 means that a static member wants to leave the group.
            return consumerGroupLeave(
                request.groupId(),
                request.instanceId(),
                request.memberId(),
                request.memberEpoch()
            );
        } else {
            // Otherwise, it is a regular heartbeat.
            return consumerGroupHeartbeat(
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                context.clientId(),
                context.clientAddress.toString(),
                request.subscribedTopicNames(),
                request.subscribedTopicRegex(),
                request.serverAssignor(),
                request.topicPartitions()
            );
        }
    }

    /**
     * Handles a regular heartbeat from a consumer group member. It mainly consists of
     * three parts:
     * 1) The member is created or updated. The group epoch is bumped if the member
     *    has been created or updated.
     * 2) The target assignment for the consumer group is updated if the group epoch
     *    is larger than the current target assignment epoch.
     * 3) The member's assignment is reconciled with the target assignment.
     *
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param clientId              The client id.
     * @param clientHost            The client host.
     * @param subscribedTopicNames  The list of subscribed topic names from the request
     *                              or null.
     * @param subscribedTopicRegex  The regular expression based subscription from the request
     *                              or null.
     * @param assignorName          The assignor name from the request or null.
     * @param ownedTopicPartitions  The list of owned partitions from the request or null.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String assignorName,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();

        // Get or create the consumer group.
        boolean createIfNotExists = memberEpoch == 0;
        final ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, createIfNotExists, records);
        helper.throwIfConsumerGroupIsFull(group, memberId, consumerGroupMaxSize);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        final ConsumerGroupMember member;
        if (instanceId == null) {
            member = helper.getOrMaybeSubscribeDynamicConsumerGroupMember(
                group,
                memberId,
                memberEpoch,
                ownedTopicPartitions,
                createIfNotExists,
                false,
                log
            );
        } else {
            member = helper.getOrMaybeSubscribeStaticConsumerGroupMember(
                group,
                memberId,
                memberEpoch,
                instanceId,
                ownedTopicPartitions,
                createIfNotExists,
                false,
                records,
                log
            );
        }

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateServerAssignorName(Optional.ofNullable(assignorName))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .maybeUpdateSubscribedTopicRegex(Optional.ofNullable(subscribedTopicRegex))
            .setClientId(clientId)
            .setClientHost(clientHost)
            .setClassicMemberMetadata(null)
            .build();

        boolean bumpGroupEpoch = helper.hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records,
            log
        );

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        Map<String, Integer> subscribedTopicNamesMap = group.subscribedTopicNames();
        SubscriptionType subscriptionType = group.subscriptionType();

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            subscribedTopicNamesMap = group.computeSubscribedTopicNames(member, updatedMember);
            subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNamesMap,
                groupStore.image().topics(),
                groupStore.image().cluster()
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId()) && !group.hasStaticMember(updatedMember.instanceId())) {
                numMembers++;
            }

            subscriptionType = ModernGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        groupId, subscriptionMetadata);
                }
                bumpGroupEpoch = true;
                records.add(newConsumerGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newConsumerGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
                metrics.record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
            }

            group.setMetadataRefreshDeadline(currentTimeMs + consumerGroupMetadataRefreshIntervalMs, groupEpoch);
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The delta between
        // the existing and the new target assignment is persisted to the partition.
        final int targetAssignmentEpoch;
        final Assignment targetAssignment;

        if (groupEpoch > group.assignmentEpoch()) {
            targetAssignment = helper.updateTargetAssignment(
                group,
                groupEpoch,
                member,
                updatedMember,
                subscriptionMetadata,
                subscriptionType,
                records,
                log,
                time,
                defaultConsumerGroupAssignor.name(),
                consumerGroupAssignors,
                groupStore.image().topics()
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = group.assignmentEpoch();
            targetAssignment = group.targetAssignment(updatedMember.memberId(), updatedMember.instanceId());
        }

        // 3. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = helper.maybeReconcile(
            groupId,
            updatedMember,
            group::currentPartitionEpoch,
            targetAssignmentEpoch,
            targetAssignment,
            ownedTopicPartitions,
            records
        );

        scheduleConsumerGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member sent a full request. It does so when joining or rejoining the group with zero
        //    as the member epoch; or on any errors (e.g. timeout). We use all the non-optional fields
        //    (rebalanceTimeoutMs, subscribedTopicNames and ownedTopicPartitions) to detect a full request
        //    as those must be set in a full request.
        // 2. The member's assignment has been updated.
        boolean isFullRequest = memberEpoch == 0 || (rebalanceTimeoutMs != -1 && subscribedTopicNames != null && ownedTopicPartitions != null);
        if (isFullRequest || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(createConsumerGroupResponseAssignment(updatedMember));
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Replays ConsumerGroupMemberMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ConsumerGroupMemberMetadataKey key.
     * @param value A ConsumerGroupMemberMetadataValue record.
     */
    public void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames().keySet());

        if (value != null) {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, true);
            consumerGroup.updateMember(new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ConsumerGroupMember oldMember;
            try {
                oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore it.
                return;
            }

            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (consumerGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            consumerGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays ConsumerGroupMetadataKey/Value to update the hard state of
     * the consumer group. It updates the group epoch of the consumer
     * group or deletes the consumer group.
     *
     * @param key   A ConsumerGroupMetadataKey key.
     * @param value A ConsumerGroupMetadataValue record.
     */
    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            consumerGroup.setGroupEpoch(value.epoch());
        } else {
            ConsumerGroup consumerGroup;
            try {
                consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            if (!consumerGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + consumerGroup.members().size() + " members.");
            }
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + consumerGroup.targetAssignment().size()
                    + " members.");
            }
            if (consumerGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeConsumerGroup(groupId);
        }
    }

    /**
     * Replays ConsumerGroupPartitionMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription metadata of the consumer
     * group.
     *
     * @param key   A ConsumerGroupPartitionMetadataKey key.
     * @param value A ConsumerGroupPartitionMetadataValue record.
     */
    public void replay(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();

        ConsumerGroup group;
        try {
            group = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist, we can ignore the tombstone.
            return;
        }

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata -> {
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata));
            });
            group.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            group.setSubscriptionMetadata(Collections.emptyMap());
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ConsumerGroupTargetAssignmentMemberKey key.
     * @param value A ConsumerGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMemberKey key,
        ConsumerGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            group.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the consumer group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ConsumerGroupTargetAssignmentMetadataKey key.
     * @param value A ConsumerGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }
            if (!group.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + group.targetAssignment().size() + " members.");
            }
            group.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ConsumerGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ConsumerGroupCurrentMemberAssignmentKey key.
     * @param value A ConsumerGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            ConsumerGroupMember oldMember = group.getOrMaybeCreateMember(memberId, true);
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            group.updateMember(newMember);
        } else {
            ConsumerGroup group;
            try {
                group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember oldMember;
            try {
                oldMember = group.getOrMaybeCreateMember(memberId, false);
            } catch (UnknownMemberIdException ex) {
                // If the member does not exist, we can ignore the tombstone.
                return;
            }

            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Collections.emptyMap())
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .build();
            group.updateMember(newMember);
        }
    }

    /**
     * Replays ConsumerGroupRegularExpressionKey/Value to update the hard state of
     * the consumer group.
     *
     * @param key   A ConsumerGroupRegularExpressionKey key.
     * @param value A ConsumerGroupRegularExpressionValue record.
     */
    public void replay(
        ConsumerGroupRegularExpressionKey key,
        ConsumerGroupRegularExpressionValue value
    ) {
        String groupId = key.groupId();
        String regex = key.regularExpression();

        if (value != null) {
            ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
            group.updateResolvedRegularExpression(
                regex,
                new ResolvedRegularExpression(
                    new HashSet<>(value.topics()),
                    value.version(),
                    value.timestamp()
                )
            );
        } else {
            try {
                ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
                group.removeResolvedRegularExpression(regex);
            } catch (GroupIdNotFoundException ex) {
                // If the group does not exist, we can ignore the tombstone.
            }
        }
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.CONSUMER.toString()), 0);
        groups.forEach(group -> {
            final String groupId = group.groupId();
            ConsumerGroup consumerGroup = (ConsumerGroup) groupStore.group(groupId);
            log.info("Loaded consumer group {} with {} members.", groupId, consumerGroup.members().size());
            consumerGroup.members().forEach((memberId, member) -> {
                log.debug("Loaded member {} in consumer group {}.", memberId, groupId);
                scheduleConsumerGroupSessionTimeout(groupId, memberId);
                if (member.state() == MemberState.UNREVOKED_PARTITIONS) {
                    helper.scheduleConsumerGroupRebalanceTimeout(
                        groupId,
                        member.memberId(),
                        member.memberEpoch(),
                        member.rebalanceTimeoutMs()
                    );
                }
            });
        });
    }

    /**
     * Called when the partition is unloaded.
     */
    public void onUnloaded() {
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.CONSUMER.toString()), 0);
        groups.forEach(group -> {
            final String groupId = group.groupId();
            ConsumerGroup consumerGroup = (ConsumerGroup) groupStore.group(groupId);
            log.info("Loaded consumer group {} with {} members.", groupId, consumerGroup.members().size());
            log.info("[GroupId={}] Unloaded group metadata for group epoch {}.",
                consumerGroup.groupId(), consumerGroup.groupEpoch());
        });
    }

    /**
     * Validates the request.
     *
     * @param request The request to validate.
     * @param apiVersion The version of ConsumerGroupHeartbeat RPC
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(
        ConsumerGroupHeartbeatRequestData request,
        short apiVersion
    ) throws InvalidRequestException, UnsupportedAssignorException {
        if (apiVersion >= CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION ||
            request.memberEpoch() > 0 ||
            request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH
        ) {
            throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        }

        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.instanceId(), "InstanceId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");

        if (request.memberEpoch() == 0) {
            if (request.rebalanceTimeoutMs() == -1) {
                throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
            }
            if (request.topicPartitions() == null || !request.topicPartitions().isEmpty()) {
                throw new InvalidRequestException("TopicPartitions must be empty when (re-)joining.");
            }
            boolean hasSubscribedTopicNames = request.subscribedTopicNames() != null && !request.subscribedTopicNames().isEmpty();
            boolean hasSubscribedTopicRegex = request.subscribedTopicRegex() != null && !request.subscribedTopicRegex().isEmpty();
            if (!hasSubscribedTopicNames && !hasSubscribedTopicRegex) {
                throw new InvalidRequestException("SubscribedTopicNames or SubscribedTopicRegex must be set in first request.");
            }
        } else if (request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throwIfNull(request.instanceId(), "InstanceId can't be null.");
        } else if (request.memberEpoch() < LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }

        if (request.serverAssignor() != null && !consumerGroupAssignors.containsKey(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor()
                + " is not supported. Supported assignors: " + String.join(", ", consumerGroupAssignors.keySet())
                + ".");
        }
    }

    /**
     * Handles leave request from a consumer group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     * @param memberEpoch   The member epoch from the request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupLeave(
        String groupId,
        String instanceId,
        String memberId,
        int memberEpoch
    ) throws ApiException {
        ConsumerGroup group = helper.consumerGroup(groupId);
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);

        if (instanceId == null) {
            ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} left the consumer group.", groupId, memberId);
            return helper.consumerGroupFenceMember(group, member, response);
        } else {
            ConsumerGroupMember member = group.staticMember(instanceId);
            throwIfStaticMemberIsUnknown(member, instanceId);
            throwIfInstanceIdIsFenced(member, groupId, memberId, instanceId);
            if (memberEpoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
                log.info("[GroupId {}] Static member {} with instance id {} temporarily left the consumer group.",
                    group.groupId(), memberId, instanceId);
                return consumerGroupStaticMemberGroupLeave(group, member);
            } else {
                log.info("[GroupId {}] Static member {} with instance id {} left the consumer group.",
                    group.groupId(), memberId, instanceId);
                return helper.consumerGroupFenceMember(group, member, response);
            }
        }
    }

    /**
     * Gets or maybe creates a consumer group without updating the groups map.
     * The group will be materialized during the replay.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist or is an empty classic group.
     * @param records           The record list to which the group tombstones are written
     *                          if the group is empty and is a classic group.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    private ConsumerGroup getOrMaybeCreateConsumerGroup(
        String groupId,
        boolean createIfNotExists,
        List<CoordinatorRecord> records
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found.", groupId));
        }

        if (group == null || (createIfNotExists && maybeDeleteEmptyClassicGroup(group, records))) {
            return new ConsumerGroup(groupStore.snapshotRegistry(), groupId, metrics);
        } else {
            if (group.type() == CONSUMER) {
                return (ConsumerGroup) group;
            } else if (createIfNotExists && group.type() == CLASSIC && validateOnlineUpgrade((ClassicGroup) group)) {
                return convertToConsumerGroup((ClassicGroup) group, records);
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
            }
        }
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        scheduleConsumerGroupSessionTimeout(groupId, memberId, consumerGroupSessionTimeoutMs(groupId));
    }


    /**
     * Get the heartbeat interval of the provided consumer group.
     */
    private int consumerGroupHeartbeatIntervalMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::consumerHeartbeatIntervalMs)
            .orElse(consumerGroupHeartbeatIntervalMs);
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createConsumerGroupResponseAssignment(
        ConsumerGroupMember member
    ) {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(fromAssignmentMap(member.assignedPartitions()));
    }

    /**
     * Throws an InvalidRequestException if the value is non-null and empty.
     * A string containing only whitespaces is also considered empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfEmptyString(
        String value,
        String error
    ) throws InvalidRequestException {
        if (value != null && value.trim().isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value == null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param staticMember          The static member in the group.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnknownMemberIdException if no static member exists in the group against the provided instance id.
     */
    private void throwIfStaticMemberIsUnknown(ConsumerGroupMember staticMember, String receivedInstanceId) {
        if (staticMember == null) {
            throw Errors.UNKNOWN_MEMBER_ID.exception("Instance id " + receivedInstanceId + " is unknown.");
        }
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws FencedInstanceIdException if the instance id provided is fenced because of another static member.
     */
    private void throwIfInstanceIdIsFenced(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId) {
        if (!member.memberId().equals(receivedMemberId)) {
            log.info("[GroupId {}] Static member {} with instance id {} is fenced by existing member {}.",
                groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.FENCED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " was fenced by member " + member.memberId() + ".");
        }
    }

    /**
     * Handles the case when a static member decides to leave the group.
     * The member is not actually fenced from the group, and instead it's
     * member epoch is updated to -2 to reflect that a member using the given
     * instance id decided to leave the group and would be back within session
     * timeout.
     *
     * @param group     The group.
     * @param member    The static member in the group for the instance id.
     *
     * @return A CoordinatorResult with a single record signifying that the static member is leaving.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupStaticMemberGroupLeave(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        // We will write a member epoch of -2 for this departing static member.
        ConsumerGroupMember leavingStaticMember = new ConsumerGroupMember.Builder(member)
            .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
            .setPartitionsPendingRevocation(Collections.emptyMap())
            .build();

        return new CoordinatorResult<>(
            Collections.singletonList(newConsumerGroupCurrentAssignmentRecord(group.groupId(), leavingStaticMember)),
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member.memberId())
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );
    }

    /**
     * Write tombstones for the group if it's empty and is a classic group.
     *
     * @param group     The group to be deleted.
     * @param records   The list of records to delete the group.
     *
     * @return true if the group is empty
     */
    private boolean maybeDeleteEmptyClassicGroup(Group group, List<CoordinatorRecord> records) {
        if (isEmptyClassicGroup(group)) {
            // Delete the classic group by adding tombstones.
            // There's no need to remove the group as the replay of tombstones removes it.
            if (group != null) createGroupTombstoneRecords(group, records);
            return true;
        }
        return false;
    }

    /**
     * Validates the online upgrade if the Classic Group receives a ConsumerGroupHeartbeat request.
     *
     * @param classicGroup A ClassicGroup.
     * @return A boolean indicating whether it's valid to online upgrade the classic group.
     */
    private boolean validateOnlineUpgrade(ClassicGroup classicGroup) {
        if (!consumerGroupMigrationPolicy.isUpgradeEnabled()) {
            log.info("Cannot upgrade classic group {} to consumer group because the online upgrade is disabled.",
                classicGroup.groupId());
            return false;
        } else if (!classicGroup.usesConsumerGroupProtocol()) {
            log.info("Cannot upgrade classic group {} to consumer group because the group does not use the consumer embedded protocol.",
                classicGroup.groupId());
            return false;
        } else if (classicGroup.numMembers() > consumerGroupMaxSize) {
            log.info("Cannot upgrade classic group {} to consumer group because the group size exceeds the consumer group maximum size.",
                classicGroup.groupId());
            return false;
        }
        return true;
    }

    /**
     * Creates a ConsumerGroup corresponding to the given classic group.
     *
     * @param classicGroup  The ClassicGroup to convert.
     * @param records       The list of Records.
     * @return The created ConsumerGroup.
     */
    ConsumerGroup convertToConsumerGroup(ClassicGroup classicGroup, List<CoordinatorRecord> records) {
        // The upgrade is always triggered by a new member joining the classic group, which always results in
        // updatedMember.subscribedTopicNames changing, the group epoch being bumped, and triggering a new rebalance.
        // If the ClassicGroup is rebalancing, inform the awaiting consumers of another ongoing rebalance
        // so that they will rejoin for the new rebalance.
        classicGroup.completeAllJoinFutures(Errors.REBALANCE_IN_PROGRESS);
        classicGroup.completeAllSyncFutures(Errors.REBALANCE_IN_PROGRESS);

        classicGroup.createGroupTombstoneRecords(records);

        ConsumerGroup consumerGroup;
        try {
            consumerGroup = ConsumerGroup.fromClassicGroup(
                groupStore.snapshotRegistry(),
                metrics,
                classicGroup,
                groupStore.image().topics()
            );
        } catch (SchemaException e) {
            log.warn("Cannot upgrade the classic group " + classicGroup.groupId() +
                " to consumer group because the embedded consumer protocol is malformed: "
                + e.getMessage() + ".", e);

            throw new GroupIdNotFoundException("Cannot upgrade the classic group " + classicGroup.groupId() +
                " to consumer group because the embedded consumer protocol is malformed.");
        }
        consumerGroup.createConsumerGroupRecords(records);

        // Create the session timeouts for the new members. If the conversion fails, the group will remain a
        // classic group, thus these timers will fail the group type check and do nothing.
        consumerGroup.members().forEach((memberId, member) ->
            scheduleConsumerGroupSessionTimeout(consumerGroup.groupId(), memberId, member.classicProtocolSessionTimeout().get())
        );

        return consumerGroup;
    }

    /**
     * Get the session timeout of the provided consumer group.
     */
    private int consumerGroupSessionTimeoutMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::consumerSessionTimeoutMs)
            .orElse(consumerGroupSessionTimeoutMs);
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId           The group id.
     * @param memberId          The member id.
     * @param sessionTimeoutMs  The session timeout.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId,
        int sessionTimeoutMs
    ) {
        timer.schedule(
            GroupMetaManagerHelper.groupSessionTimeoutKey(groupId, memberId),
            sessionTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the member session expired")
        );
    }

    private List<ConsumerGroupHeartbeatResponseData.TopicPartitions> fromAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
    }

    /**
     * @return true if the group is an empty classic group.
     */
    private static boolean isEmptyClassicGroup(Group group) {
        return group != null && group.type() == CLASSIC && group.isEmpty();
    }

    /**
     * Populates the record list passed in with record to update the state machine.
     *
     * @param group The group to be deleted.
     * @param records The record list to populate.
     */
    public void createGroupTombstoneRecords(
        Group group,
        List<CoordinatorRecord> records
    ) {
        group.createGroupTombstoneRecords(records);
    }

    public static String consumerGroupRebalanceTimeoutKey(String groupId, String memberId) {
        return "rebalance-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Fences a member from a consumer group. Returns an empty CoordinatorResult
     * if the group or the member doesn't exist.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     * @param reason    The reason for fencing the member.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMemberOperation(
        String groupId,
        String memberId,
        String reason
    ) {
        try {
            ConsumerGroup group = helper.consumerGroup(groupId);
            ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} fenced from the group because {}.",
                groupId, memberId, reason);

            return helper.consumerGroupFenceMember(group, member, null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                groupId, memberId);
        } catch (UnknownMemberIdException ex) {
            log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                groupId, memberId);
        }

        return new CoordinatorResult<>(Collections.emptyList());
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeConsumerGroup(
        String groupId
    ) {
        Group group = groupStore.removeGroup(groupId);
        if (group == null) {
            log.warn("The group {} is not existing.", groupId);
        } else if (group.type() != SHARE) {
            log.warn("Removed group {} with an unknown group type {}.", groupId, group.type());
        } else {
            ConsumerGroup consumerGroup = (ConsumerGroup) group;
            metrics.onConsumerGroupStateTransition(consumerGroup.state(), null);
        }
    }

    /**
     * The method should be called on the replay path.
     * Gets or maybe creates a consumer group and updates the groups map if a new group is created.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     * @throws IllegalStateException    if the group does not have the expected type.
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreatePersistedConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException, IllegalStateException {
        Group group = groupStore.group(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(groupStore.snapshotRegistry(), groupId, metrics);
            groupStore.addGroup(groupId, consumerGroup);
            metrics.onConsumerGroupStateTransition(null, consumerGroup.state());
            return consumerGroup;
        } else if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else if (group.type() == CLASSIC && ((ClassicGroup) group).isSimpleGroup()) {
            // If the group is a simple classic group, it was automatically created to hold committed
            // offsets if no group existed. Simple classic groups are not backed by any records
            // in the __consumer_offsets topic hence we can safely replace it here. Without this,
            // replaying consumer group records after offset commit records would not work.
            ConsumerGroup consumerGroup = new ConsumerGroup(groupStore.snapshotRegistry(), groupId, metrics);
            groupStore.addGroup(groupId, consumerGroup);
            metrics.onConsumerGroupStateTransition(null, consumerGroup.state());
            return consumerGroup;
        } else {
            throw new IllegalStateException(String.format("Group %s is not a consumer group", groupId));
        }
    }

    /**
     * Updates the group by topics mapping.
     *
     * @param groupId               The group id.
     * @param oldSubscribedTopics   The old group subscriptions.
     * @param newSubscribedTopics   The new group subscriptions.
     */
    private void updateGroupsByTopics(
        String groupId,
        Set<String> oldSubscribedTopics,
        Set<String> newSubscribedTopics
    ) {
        if (oldSubscribedTopics.isEmpty()) {
            newSubscribedTopics.forEach(topicName ->
                groupStore.subscribeGroupToTopic(groupId, topicName)
            );
        } else if (newSubscribedTopics.isEmpty()) {
            oldSubscribedTopics.forEach(topicName ->
                groupStore.unsubscribeGroupFromTopic(groupId, topicName)
            );
        } else {
            oldSubscribedTopics.forEach(topicName -> {
                if (!newSubscribedTopics.contains(topicName)) {
                    groupStore.unsubscribeGroupFromTopic(groupId, topicName);
                }
            });
            newSubscribedTopics.forEach(topicName -> {
                if (!oldSubscribedTopics.contains(topicName)) {
                    groupStore.subscribeGroupToTopic(groupId, topicName);
                }
            });
        }
    }

    /**
     * Delete and write tombstones for the group if it's empty and is a consumer group.
     *
     * @param groupId The group id to be deleted.
     * @param records The list of records to delete the group.
     */
    private void maybeDeleteEmptyConsumerGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groupStore.group(groupId, Long.MAX_VALUE);
        if (isEmptyConsumerGroup(group)) {
            // Add tombstones for the previous consumer group. The tombstones won't actually be
            // replayed because its coordinator result has a non-null appendFuture.
            createGroupTombstoneRecords(group, records);
            removeConsumerGroup(groupId);
        }
    }

    /**
     * @return true if the group is an empty consumer group.
     */
    private static boolean isEmptyConsumerGroup(Group group) {
        return group != null && group.type() == CONSUMER && group.isEmpty();
    }

}
