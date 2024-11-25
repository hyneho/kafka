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

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.CurrentAssignmentBuilder;
import org.apache.kafka.image.TopicsImage;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.Utils.assignmentToString;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;

public class GroupMetaManagerHelper {

    /**
     * The reference to the group store.
     */
    private final GroupStore groupStore;

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

    /**
     * The config indicating whether group protocol upgrade/downgrade is allowed.
     */
    private final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy;

    /**
     * The maximum number of members allowed in a single classic group.
     */
    private final int classicGroupMaxSize;


    /**
     * Initial rebalance delay for members joining a classic group.
     */
    private final int classicGroupInitialRebalanceDelayMs;

    /**
     * An empty result returned to the state machine. This means that
     * there are no records to append to the log.
     *
     * Package private for testing.
     */
    static final CoordinatorResult<Void, CoordinatorRecord> EMPTY_RESULT =
        new CoordinatorResult<>(Collections.emptyList(), CompletableFuture.completedFuture(null), false);

    GroupMetaManagerHelper(
        GroupStore groupStore,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        LogContext logContext,
        Logger log,
        ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy,
        int classicGroupMaxSize,
        int classicGroupInitialRebalanceDelayMs) {
        this.groupStore = groupStore;
        this.time = time;
        this.timer = timer;
        this.logContext = logContext;
        this.log = log;
        this.consumerGroupMigrationPolicy = consumerGroupMigrationPolicy;
        this.classicGroupMaxSize = classicGroupMaxSize;
        this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
    }

    /**
     * Gets a consumer group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a consumer group.
     */
    public ConsumerGroup consumerGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId, committedOffset);

        if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
        }
    }


    /**
     * An overloaded method of {@link GroupMetadataManager#consumerGroup(String, long)}
     */
    public ConsumerGroup consumerGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        return consumerGroup(groupId, Long.MAX_VALUE);
    }

    /**
     * Checks whether the consumer group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The consumer group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    public void throwIfConsumerGroupIsFull(
        ConsumerGroup group,
        String memberId,
        int consumerGroupMaxSize
    ) throws GroupMaxSizeReachedException {
        // If the consumer group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the consumer group.
        if (group.numMembers() >= consumerGroupMaxSize && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The consumer group has reached its maximum capacity of "
                + consumerGroupMaxSize + " members.");
        }
    }

    /**
     * Gets or subscribes a new dynamic consumer group member.
     *
     * @param group                 The consumer group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     *
     * @return The existing consumer group member or a new one.
     */
    public ConsumerGroupMember getOrMaybeSubscribeDynamicConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        int memberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol,
        Logger log
    ) {
        ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        if (!useClassicProtocol) {
            throwIfConsumerGroupMemberEpochIsInvalid(member, memberEpoch, ownedTopicPartitions);
        }
        if (createIfNotExists) {
            log.info("[GroupId {}] Member {} joins the consumer group using the {} protocol.",
                group.groupId(), memberId, useClassicProtocol ? "classic" : "consumer");
        }
        return member;
    }

    /**
     * Gets or subscribes a static consumer group member. This method also replaces the
     * previous static member if allowed.
     *
     * @param group                 The consumer group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param instanceId            The instance id.
     * @param ownedTopicPartitions  The owned partitions reported by the member.
     * @param createIfNotExists     Whether the member should be created or not.
     * @param useClassicProtocol    Whether the member uses the classic protocol.
     * @param records               The list to accumulate records created to replace
     *                              the previous static member.
     *
     * @return The existing consumer group member or a new one.
     */
    public ConsumerGroupMember getOrMaybeSubscribeStaticConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        int memberEpoch,
        String instanceId,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        boolean createIfNotExists,
        boolean useClassicProtocol,
        List<CoordinatorRecord> records,
        Logger log
    ) {
        ConsumerGroupMember existingStaticMemberOrNull = group.staticMember(instanceId);

        if (createIfNotExists) {
            // A new static member joins or the existing static member rejoins.
            if (existingStaticMemberOrNull == null) {
                // New static member.
                ConsumerGroupMember newMember = group.getOrMaybeCreateMember(memberId, true);
                log.info("[GroupId {}] Static member {} with instance id {} joins the consumer group using the {} protocol.",
                    group.groupId(), memberId, instanceId, useClassicProtocol ? "classic" : "consumer");
                return newMember;
            } else {
                if (!useClassicProtocol && !existingStaticMemberOrNull.useClassicProtocol()) {
                    // If both the rejoining static member and the existing static member use the consumer
                    // protocol, replace the previous instance iff the previous member had sent a leave group.
                    throwIfInstanceIdIsUnreleased(existingStaticMemberOrNull, group.groupId(), memberId, instanceId, log);
                }

                // Copy the member but with its new member id.
                ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(existingStaticMemberOrNull, memberId)
                    .setMemberEpoch(0)
                    .setPreviousMemberEpoch(0)
                    .build();

                // Generate the records to replace the member.
                replaceMember(records, group, existingStaticMemberOrNull, newMember);

                log.info("[GroupId {}] Static member with instance id {} re-joins the consumer group " +
                        "using the {} protocol. Created a new member {} to replace the existing member {}.",
                    group.groupId(), instanceId, useClassicProtocol ? "classic" : "consumer", memberId, existingStaticMemberOrNull.memberId());

                return newMember;
            }
        } else {
            throwIfStaticMemberIsUnknown(existingStaticMemberOrNull, instanceId);
            throwIfInstanceIdIsFenced(existingStaticMemberOrNull, group.groupId(), memberId, instanceId, log);
            if (!useClassicProtocol) {
                throwIfConsumerGroupMemberEpochIsInvalid(existingStaticMemberOrNull, memberEpoch, ownedTopicPartitions);
            }
            return existingStaticMemberOrNull;
        }
    }

    /**
     * Creates the member subscription record if the updatedMember is different from
     * the old member. Returns true if the subscribedTopicNames/subscribedTopicRegex
     * has changed.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the updatedMember has a different
     *         subscribedTopicNames/subscribedTopicRegex from the old member.
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    public boolean hasMemberSubscriptionChanged(
        String groupId,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        List<CoordinatorRecord> records,
        Logger log
    ) throws InvalidRegularExpression {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupMemberSubscriptionRecord(groupId, updatedMember));

            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.debug("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                return true;
            }

            if (!updatedMember.subscribedTopicRegex().equals(member.subscribedTopicRegex())) {
                log.debug("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicRegex());
                // If the regular expression has changed, we compile it to ensure that
                // its syntax is valid.
                if (updatedMember.subscribedTopicRegex() != null) {
                    throwIfRegularExpressionIsInvalid(updatedMember.subscribedTopicRegex());
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group                 The ConsumerGroup.
     * @param groupEpoch            The group epoch.
     * @param member                The existing member.
     * @param updatedMember         The updated member.
     * @param subscriptionMetadata  The subscription metadata.
     * @param subscriptionType      The group subscription type.
     * @param records               The list to accumulate any new records.
     * @return The new target assignment.
     */
    public Assignment updateTargetAssignment(
        ConsumerGroup group,
        int groupEpoch,
        ConsumerGroupMember member,
        ConsumerGroupMember updatedMember,
        Map<String, TopicMetadata> subscriptionMetadata,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records,
        Logger log,
        Time time,
        String defaultConsumerGroupAssignorName,
        Map<String, ConsumerGroupPartitionAssignor> consumerGroupAssignors,
        TopicsImage topicsImage
    ) {
        String preferredServerAssignor = group.computePreferredServerAssignor(
            member,
            updatedMember
        ).orElse(defaultConsumerGroupAssignorName);
        try {
            TargetAssignmentBuilder<ConsumerGroupMember> assignmentResultBuilder =
                new TargetAssignmentBuilder<ConsumerGroupMember>(group.groupId(), groupEpoch, consumerGroupAssignors.get(preferredServerAssignor))
                    .withMembers(group.members())
                    .withStaticMembers(group.staticMembers())
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withTopicsImage(topicsImage)
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            // If the instance id was associated to a different member, it means that the
            // static member is replaced by the current member hence we remove the previous one.
            String previousMemberId = group.staticMemberId(updatedMember.instanceId());
            if (previousMemberId != null && !updatedMember.memberId().equals(previousMemberId)) {
                assignmentResultBuilder.removeMember(previousMemberId);
            }

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                    group.groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs, assignmentResult.targetAssignment());
            } else {
                log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms.",
                    group.groupId(), groupEpoch, preferredServerAssignor, assignorTimeMs);
            }

            records.addAll(assignmentResult.records());

            MemberAssignment newMemberAssignment = assignmentResult.targetAssignment().get(updatedMember.memberId());
            if (newMemberAssignment != null) {
                return new Assignment(newMemberAssignment.partitions());
            } else {
                return Assignment.EMPTY;
            }
        } catch (PartitionAssignorException ex) {
            String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                groupEpoch, ex.getMessage());
            log.error("[GroupId {}] {}.", group.groupId(), msg);
            throw new UnknownServerException(msg, ex);
        }
    }

    /**
     * Reconciles the current assignment of the member towards the target assignment if needed.
     *
     * @param groupId               The group id.
     * @param member                The member to reconcile.
     * @param currentPartitionEpoch The function returning the current epoch of
     *                              a given partition.
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param ownedTopicPartitions  The list of partitions owned by the member. This
     *                              is reported in the ConsumerGroupHeartbeat API and
     *                              it could be null if not provided.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    public ConsumerGroupMember maybeReconcile(
        String groupId,
        ConsumerGroupMember member,
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .withCurrentPartitionEpoch(currentPartitionEpoch)
            .withOwnedTopicPartitions(ownedTopicPartitions)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newConsumerGroupCurrentAssignmentRecord(groupId, updatedMember));

            if (log.isDebugEnabled()) {
                log.debug("[GroupId {}] Member {} new assignment state: epoch={}, previousEpoch={}, state={}, "
                        + "assignedPartitions={} and revokedPartitions={}.",
                    groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                    assignmentToString(updatedMember.assignedPartitions()), assignmentToString(updatedMember.partitionsPendingRevocation()));
            }

            // Schedule/cancel the rebalance timeout if the member uses the consumer protocol.
            // The members using classic protocol only have join timer and sync timer.
            if (!updatedMember.useClassicProtocol()) {
                if (updatedMember.state() == MemberState.UNREVOKED_PARTITIONS) {
                    scheduleConsumerGroupRebalanceTimeout(
                        groupId,
                        updatedMember.memberId(),
                        updatedMember.memberEpoch(),
                        updatedMember.rebalanceTimeoutMs()
                    );
                } else {
                    cancelConsumerGroupRebalanceTimeout(groupId, updatedMember.memberId());
                }
            }
        }

        return updatedMember;
    }

    /**
     * Cancels the rebalance timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupRebalanceTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupRebalanceTimeoutKey(groupId, memberId));
    }

    /**
     * Schedules a rebalance timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    public void scheduleConsumerGroupRebalanceTimeout(
        String groupId,
        String memberId,
        int memberEpoch,
        int rebalanceTimeoutMs
    ) {
        String key = consumerGroupRebalanceTimeoutKey(groupId, memberId);
        timer.schedule(key, rebalanceTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = consumerGroup(groupId, Long.MAX_VALUE);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

                if (member.memberEpoch() == memberEpoch) {
                    log.info("[GroupId {}] Member {} fenced from the group because " +
                            "it failed to transition from epoch {} within {}ms.",
                        groupId, memberId, memberEpoch, rebalanceTimeoutMs);

                    return consumerGroupFenceMember(group, member, null);
                } else {
                    log.debug("[GroupId {}] Ignoring rebalance timeout for {} because the member " +
                        "left the epoch {}.", groupId, memberId, memberEpoch);
                    return new CoordinatorResult<>(Collections.emptyList());
                }
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(Collections.emptyList());
        });
    }

    /**
     * Fences a member from a consumer group and maybe downgrade the consumer group to a classic group.
     *
     * @param group     The group.
     * @param member    The member.
     * @param response  The response of the CoordinatorResult.
     *
     * @return The CoordinatorResult to be applied.
     */
    public  <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupFenceMember(
        ConsumerGroup group,
        ConsumerGroupMember member,
        T response
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        if (validateOnlineDowngradeWithFencedMember(group, member.memberId())) {
            convertToClassicGroup(group, member.memberId(), null, records);
            return new CoordinatorResult<>(records, response, null, false);
        } else {
            removeMember(records, group.groupId(), member.memberId());

            // We update the subscription metadata without the leaving member.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                group.computeSubscribedTopicNames(member, null),
                groupStore.image().topics(),
                groupStore.image().cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                if (log.isDebugEnabled()) {
                    log.debug("[GroupId {}] Computed new subscription metadata: {}.",
                        group.groupId(), subscriptionMetadata);
                }
                records.add(newConsumerGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
            }

            // We bump the group epoch.
            int groupEpoch = group.groupEpoch() + 1;
            records.add(newConsumerGroupEpochRecord(group.groupId(), groupEpoch));

            cancelTimers(group.groupId(), member.memberId());

            return new CoordinatorResult<>(records, response);
        }
    }

    /**
     * Cancel all the timers of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    public void cancelTimers(String groupId, String memberId) {
        cancelGroupSessionTimeout(groupId, memberId);
        cancelConsumerGroupRebalanceTimeout(groupId, memberId);
        cancelConsumerGroupJoinTimeout(groupId, memberId);
        cancelConsumerGroupSyncTimeout(groupId, memberId);
    }

    /**
     * Cancels the sync timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    public void cancelConsumerGroupSyncTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupSyncKey(groupId, memberId));
    }

    /**
     * Generate a consumer group sync key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the sync key.
     */
    static String consumerGroupSyncKey(String groupId, String memberId) {
        return "sync-" + groupId + "-" + memberId;
    }

    /**
     * Cancels the join timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    public void cancelConsumerGroupJoinTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupJoinKey(groupId, memberId));
    }

    /**
     * Generate a consumer group join key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the sync key.
     */
    static String consumerGroupJoinKey(String groupId, String memberId) {
        return "join-" + groupId + "-" + memberId;
    }

    /**
     * Creates a ClassicGroup corresponding to the given ConsumerGroup.
     *
     * @param consumerGroup     The converted ConsumerGroup.
     * @param leavingMemberId   The leaving member that triggers the downgrade validation.
     * @param joiningMember     The newly joined member if the downgrade is triggered by static member replacement.
     * @param records           The record list to which the conversion records are added.
     */
    public void convertToClassicGroup(
        ConsumerGroup consumerGroup,
        String leavingMemberId,
        ConsumerGroupMember joiningMember,
        List<CoordinatorRecord> records
    ) {
        if (joiningMember == null) {
            consumerGroup.createGroupTombstoneRecords(records);
        } else {
            consumerGroup.createGroupTombstoneRecordsWithReplacedMember(records, leavingMemberId, joiningMember.memberId());
        }

        ClassicGroup classicGroup;
        try {
            classicGroup = ClassicGroup.fromConsumerGroup(
                consumerGroup,
                leavingMemberId,
                joiningMember,
                logContext,
                time,
                groupStore.image()
            );
        } catch (SchemaException e) {
            log.warn("Cannot downgrade the consumer group " + consumerGroup.groupId() + ": fail to parse " +
                "the Consumer Protocol " + ConsumerProtocol.PROTOCOL_TYPE + ".", e);

            throw new GroupIdNotFoundException(String.format("Cannot downgrade the classic group %s: %s.",
                consumerGroup.groupId(), e.getMessage()));
        }
        classicGroup.createClassicGroupRecords(groupStore.image().features().metadataVersion(), records);

        // Directly update the states instead of replaying the records because
        // the classicGroup reference is needed for triggering the rebalance.
        removeClassicGroup(consumerGroup.groupId());
        groupStore.addGroup(consumerGroup.groupId(), classicGroup);

        classicGroup.allMembers().forEach(member -> rescheduleClassicGroupMemberHeartbeat(classicGroup, member));

        // If the downgrade is triggered by a member leaving the group, a rebalance should be triggered.
        if (joiningMember == null) {
            prepareRebalance(classicGroup, String.format("Downgrade group %s from consumer to classic.", classicGroup.groupId()));
        }
    }

    /**
     * Prepare a rebalance.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     *
     * Package private for testing.
     */
    CoordinatorResult<Void, CoordinatorRecord> prepareRebalance(
        ClassicGroup group,
        String reason) {
        // If any members are awaiting sync, cancel their request and have them rejoin.
        if (group.isInState(COMPLETING_REBALANCE)) {
            resetAndPropagateAssignmentWithError(group, Errors.REBALANCE_IN_PROGRESS);
        }

        // If a sync expiration is pending, cancel it.
        removeSyncExpiration(group);

        boolean isInitialRebalance = group.isInState(EMPTY);
        if (isInitialRebalance) {
            // The group is new. Provide more time for the members to join.
            int delayMs = classicGroupInitialRebalanceDelayMs;
            int remainingMs = Math.max(group.rebalanceTimeoutMs() - classicGroupInitialRebalanceDelayMs, 0);

            timer.schedule(
                classicGroupJoinKey(group.groupId()),
                delayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group.groupId(), delayMs, remainingMs)
            );
        }

        group.transitionTo(PREPARING_REBALANCE);

        log.info("Preparing to rebalance group {} in state {} with old generation {} (reason: {}).",
            group.groupId(), group.currentState(), group.generationId(), reason);

        return isInitialRebalance ? EMPTY_RESULT : maybeCompleteJoinElseSchedule(group);
    }

    /**
     * Try to complete the join phase. Otherwise, schedule a new join operation.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> maybeCompleteJoinElseSchedule(
        ClassicGroup group
    ) {
        String classicGroupJoinKey = classicGroupJoinKey(group.groupId());
        if (group.hasAllMembersJoined()) {
            // All members have joined. Proceed to sync phase.
            return completeClassicGroupJoin(group);
        } else {
            timer.schedule(
                classicGroupJoinKey,
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeClassicGroupJoin(group.groupId())
            );
            return EMPTY_RESULT;
        }
    }

    /**
     * Try to complete the join phase of the initial rebalance.
     * Otherwise, extend the rebalance.
     *
     * @param groupId The group under initial rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> tryCompleteInitialRebalanceElseSchedule(
        String groupId,
        int delayMs,
        int remainingMs
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Cannot find the group, skipping the initial rebalance stage.", exception);
            return EMPTY_RESULT;
        }

        if (group.newMemberAdded() && remainingMs != 0) {
            // A new member was added. Extend the delay.
            group.setNewMemberAdded(false);
            int newDelayMs = Math.min(classicGroupInitialRebalanceDelayMs, remainingMs);
            int newRemainingMs = Math.max(remainingMs - delayMs, 0);

            timer.schedule(
                classicGroupJoinKey(group.groupId()),
                newDelayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group.groupId(), newDelayMs, newRemainingMs)
            );
        } else {
            // No more time remaining. Complete the join phase.
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }


    /**
     * Remove the sync key from the timer and clear all pending sync members from the group.
     * Invoked when a new rebalance is triggered.
     *
     * @param group  The group.
     */
    private void removeSyncExpiration(ClassicGroup group) {
        group.clearPendingSyncMembers();
        timer.cancel(classicGroupSyncKey(group.groupId()));
    }

    /**
     * Reset assignment for all members and propagate the error to all members in the group.
     *
     * @param group  The group.
     * @param error  The error to propagate.
     */
    public void resetAndPropagateAssignmentWithError(ClassicGroup group, Errors error) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("Group " + group.groupId() + " must be in " + COMPLETING_REBALANCE.name() +
                " state but is in " + group.currentState() + ".");
        }

        group.allMembers().forEach(member -> member.setAssignment(EMPTY_ASSIGNMENT));
        propagateAssignment(group, error);
    }


    /**
     * Propagate assignment and error to all members.
     *
     * @param group  The group.
     * @param error  The error to propagate.
     */
    public void propagateAssignment(ClassicGroup group, Errors error) {
        Optional<String> protocolName = Optional.empty();
        Optional<String> protocolType = Optional.empty();
        if (error == Errors.NONE) {
            protocolName = group.protocolName();
            protocolType = group.protocolType();
        }

        for (ClassicGroupMember member : group.allMembers()) {
            if (!member.hasAssignment() && error == Errors.NONE) {
                log.warn("Sending empty assignment to member {} of {} for " + "generation {} with no errors",
                    member.memberId(), group.groupId(), group.generationId());
            }

            if (group.completeSyncFuture(member,
                new SyncGroupResponseData()
                    .setProtocolName(protocolName.orElse(null))
                    .setProtocolType(protocolType.orElse(null))
                    .setAssignment(member.assignment())
                    .setErrorCode(error.code()))) {

                // Reset the session timeout for members after propagating the member's assignment.
                // This is because if any member's session expired while we were still awaiting either
                // the leader sync group or the append future, its expiration will be ignored and no
                // future heartbeat expectations will not be scheduled.
                rescheduleClassicGroupMemberHeartbeat(group, member);
            }
        }
    }

    /**
     * Complete and schedule next heartbeat.
     *
     * @param group    The group.
     * @param member   The member.
     */
    public void rescheduleClassicGroupMemberHeartbeat(
        ClassicGroup group,
        ClassicGroupMember member
    ) {
        rescheduleClassicGroupMemberHeartbeat(group, member, member.sessionTimeoutMs());
    }

    /**
     * Generate a classic group heartbeat key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the heartbeat key.
     */
    static String classicGroupHeartbeatKey(String groupId, String memberId) {
        return "heartbeat-" + groupId + "-" + memberId;
    }
    /**
     * Reschedule the heartbeat.
     *
     * @param group      The group.
     * @param member     The member.
     * @param timeoutMs  The timeout for the new heartbeat.
     */
    public void rescheduleClassicGroupMemberHeartbeat(
        ClassicGroup group,
        ClassicGroupMember member,
        long timeoutMs
    ) {
        String classicGroupHeartbeatKey = classicGroupHeartbeatKey(group.groupId(), member.memberId());

        // Reschedule the next heartbeat expiration deadline
        timer.schedule(classicGroupHeartbeatKey,
            timeoutMs,
            TimeUnit.MILLISECONDS,
            false,
            () -> expireClassicGroupMemberHeartbeat(group.groupId(), member.memberId()));
    }

    /**
     * Invoked when the heartbeat operation is expired from the timer. Possibly remove the member and
     * try complete the join phase.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    public CoordinatorResult<Void, CoordinatorRecord> expireClassicGroupMemberHeartbeat(
        String groupId,
        String memberId
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Received notification of heartbeat expiration for member {} after group {} " +
                "had already been deleted or upgraded.", memberId, groupId);
            return EMPTY_RESULT;
        }

        if (group.isInState(DEAD)) {
            log.info("Received notification of heartbeat expiration for member {} after group {} " +
                    "had already been unloaded or deleted.",
                memberId, group.groupId());
        } else if (group.isPendingMember(memberId)) {
            log.info("Pending member {} in group {} has been removed after session timeout expiration.",
                memberId, group.groupId());

            return removePendingMemberAndUpdateClassicGroup(group, memberId);
        } else if (!group.hasMember(memberId)) {
            log.debug("Member {} has already been removed from the group.", memberId);
        } else {
            ClassicGroupMember member = group.member(memberId);
            if (!member.hasSatisfiedHeartbeat()) {
                log.info("Member {} in group {} has failed, removing it from the group.",
                    member.memberId(), group.groupId());

                return removeMemberAndUpdateClassicGroup(
                    group,
                    member,
                    "removing member " + member.memberId() + " on heartbeat expiration."
                );
            }
        }
        return EMPTY_RESULT;
    }

    /**
     * Invoked when the heartbeat key is expired from the timer. Possibly remove the member
     * from the group and try to complete the join phase.
     *
     * @param group     The group.
     * @param member    The member.
     * @param reason    The reason for removing the member.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> removeMemberAndUpdateClassicGroup(
        ClassicGroup group,
        ClassicGroupMember member,
        String reason
    ) {
        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the response future before removing the member. We return UNKNOWN_MEMBER_ID so
        // that the consumer will retry the JoinGroup request if it is still active.
        group.completeJoinFuture(member, new JoinGroupResponseData()
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());

        if (group.isInState(STABLE) || group.isInState(COMPLETING_REBALANCE)) {
            return maybePrepareRebalanceOrCompleteJoin(group, reason);
        } else if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Generate a classic group join key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the join key.
     */
    static String classicGroupJoinKey(String groupId) {
        return "join-" + groupId;
    }

    /**
     * An overload of {@link GroupMetadataManager#completeClassicGroupJoin(ClassicGroup)} used as
     * timeout operation. It additionally looks up the group by the id and checks the group type.
     * completeClassicGroupJoin will only be called if the group is CLASSIC.
     */
    private CoordinatorResult<Void, CoordinatorRecord> completeClassicGroupJoin(String groupId) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Cannot find the group, skipping rebalance stage.", exception);
            return EMPTY_RESULT;
        }
        return completeClassicGroupJoin(group);
    }

    /**
     * Complete the join group phase. Remove all dynamic members that have not rejoined
     * during this stage and proceed with the next generation for this group. The generation id
     * is incremented and the group transitions to CompletingRebalance state if there is at least
     * one member.
     *
     * If the group is in Empty state, append a new group metadata record to the log. Otherwise,
     * complete all members' join group response futures and wait for sync requests from members.
     *
     * @param group The group that is completing the join group phase.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> completeClassicGroupJoin(
        ClassicGroup group
    ) {
        timer.cancel(classicGroupJoinKey(group.groupId()));
        String groupId = group.groupId();

        Map<String, ClassicGroupMember> notYetRejoinedDynamicMembers =
            group.notYetRejoinedMembers().entrySet().stream()
                .filter(entry -> !entry.getValue().isStaticMember())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!notYetRejoinedDynamicMembers.isEmpty()) {
            notYetRejoinedDynamicMembers.values().forEach(failedMember -> {
                group.remove(failedMember.memberId());
                timer.cancel(classicGroupHeartbeatKey(group.groupId(), failedMember.memberId()));
            });

            log.info("Group {} removed dynamic members who haven't joined: {}",
                groupId, notYetRejoinedDynamicMembers.keySet());
        }

        if (group.isInState(DEAD)) {
            log.info("Group {} is dead, skipping rebalance stage.", groupId);
        } else if (!group.maybeElectNewJoinedLeader() && !group.allMembers().isEmpty()) {
            // If all members are not rejoining, we will postpone the completion
            // of rebalance preparing stage, and send out another delayed operation
            // until session timeout removes all the non-responsive members.
            log.error("Group {} could not complete rebalance because no members rejoined.", groupId);

            timer.schedule(
                classicGroupJoinKey(groupId),
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeClassicGroupJoin(group.groupId())
            );

            return EMPTY_RESULT;
        } else {
            group.initNextGeneration();
            if (group.isInState(EMPTY)) {
                log.info("Group {} with generation {} is now empty.", groupId, group.generationId());

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        // We failed to write the empty group metadata. If the broker fails before another rebalance,
                        // the previous generation written to the log will become active again (and most likely timeout).
                        // This should be safe since there are no active members in an empty generation, so we just warn.
                        Errors error = appendGroupMetadataErrorToResponseError(Errors.forException(t));
                        log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), error.message());
                    }
                });

                List<CoordinatorRecord> records = Collections.singletonList(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(
                    group, Collections.emptyMap(), groupStore.image().features().metadataVersion()));

                return new CoordinatorResult<>(records, appendFuture, false);

            } else {
                log.info("Stabilized group {} generation {} with {} members.",
                    groupId, group.generationId(), group.numMembers());

                // Complete the awaiting join group response future for all the members after rebalancing
                group.allMembers().forEach(member -> {
                    List<JoinGroupResponseData.JoinGroupResponseMember> members = Collections.emptyList();
                    if (group.isLeader(member.memberId())) {
                        members = group.currentClassicGroupMembers();
                    }

                    JoinGroupResponseData response = new JoinGroupResponseData()
                        .setMembers(members)
                        .setMemberId(member.memberId())
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                        .setErrorCode(Errors.NONE.code());

                    group.completeJoinFuture(member, response);
                    rescheduleClassicGroupMemberHeartbeat(group, member);
                    member.setIsNew(false);

                    group.addPendingSyncMember(member.memberId());
                });

                schedulePendingSync(group);
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Generate a classic group sync key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the sync key.
     */
    static String classicGroupSyncKey(String groupId) {
        return "sync-" + groupId;
    }

    /**
     * Wait for sync requests for the group.
     *
     * @param group The group.
     */
    private void schedulePendingSync(ClassicGroup group) {
        timer.schedule(
            classicGroupSyncKey(group.groupId()),
            group.rebalanceTimeoutMs(),
            TimeUnit.MILLISECONDS,
            false,
            () -> expirePendingSync(group.groupId(), group.generationId()));
    }

    /**
     * Expire pending sync.
     *
     * @param groupId         The group id.
     * @param generationId    The generation when the pending sync was originally scheduled.
     *
     * @return The coordinator result that will be appended to the log.
     * */
    private CoordinatorResult<Void, CoordinatorRecord> expirePendingSync(
        String groupId,
        int generationId
    ) {
        ClassicGroup group;
        try {
            group = getOrMaybeCreateClassicGroup(groupId, false);
        } catch (GroupIdNotFoundException exception) {
            log.debug("Received notification of sync expiration for an unknown classic group {}.", groupId);
            return EMPTY_RESULT;
        }

        if (generationId != group.generationId()) {
            log.error("Received unexpected notification of sync expiration for {} with an old " +
                "generation {} while the group has {}.", group.groupId(), generationId, group.generationId());
        } else {
            if (group.isInState(DEAD) || group.isInState(EMPTY) || group.isInState(PREPARING_REBALANCE)) {
                log.error("Received unexpected notification of sync expiration after group {} already " +
                    "transitioned to {} state.", group.groupId(), group.stateAsString());
            } else if (group.isInState(COMPLETING_REBALANCE) || group.isInState(STABLE)) {
                if (!group.hasReceivedSyncFromAllMembers()) {
                    Set<String> pendingSyncMembers = new HashSet<>(group.allPendingSyncMembers());
                    pendingSyncMembers.forEach(memberId -> {
                        group.remove(memberId);
                        timer.cancel(classicGroupHeartbeatKey(group.groupId(), memberId));
                    });

                    log.debug("Group {} removed members who haven't sent their sync requests: {}",
                        group.groupId(), pendingSyncMembers);

                    return prepareRebalance(group, "Removing " + pendingSyncMembers + " on pending sync request expiration");
                }
            }
        }
        return EMPTY_RESULT;
    }

    // Visible for testing
    static Errors appendGroupMetadataErrorToResponseError(Errors appendError) {
        switch (appendError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case NOT_ENOUGH_REPLICAS:
            case REQUEST_TIMED_OUT:
                return COORDINATOR_NOT_AVAILABLE;

            case NOT_LEADER_OR_FOLLOWER:
            case KAFKA_STORAGE_ERROR:
                return NOT_COORDINATOR;

            case MESSAGE_TOO_LARGE:
            case RECORD_LIST_TOO_LARGE:
            case INVALID_FETCH_SIZE:
                return UNKNOWN_SERVER_ERROR;

            default:
                return appendError;
        }
    }

    /**
     * Prepare a rebalance if the group is in a valid state. Otherwise, try
     * to complete the join phase.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    public CoordinatorResult<Void, CoordinatorRecord> maybePrepareRebalanceOrCompleteJoin(
        ClassicGroup group,
        String reason
    ) {
        if (group.canRebalance()) {
            return prepareRebalance(group, reason);
        } else {
            return maybeCompleteJoinPhase(group);
        }
    }

    /**
     * Attempt to complete join group phase. We do not complete
     * the join group phase if this is the initial rebalance.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    public CoordinatorResult<Void, CoordinatorRecord> maybeCompleteJoinPhase(ClassicGroup group) {
        if (!group.isInState(PREPARING_REBALANCE)) {
            log.debug("Cannot complete join phase of group {} because the group is in {} state.",
                group.groupId(), group.currentState());
            return EMPTY_RESULT;
        }

        if (group.previousState() == EMPTY) {
            log.debug("Cannot complete join phase of group {} because this is an initial rebalance.",
                group.groupId());
            return EMPTY_RESULT;
        }

        if (!group.hasAllMembersJoined()) {
            log.debug("Cannot complete join phase of group {} because not all the members have rejoined. " +
                    "Members={}, AwaitingJoinResponses={}, PendingJoinMembers={}.",
                group.groupId(), group.numMembers(), group.numAwaitingJoinResponse(), group.numPendingJoinMembers());
            return EMPTY_RESULT;
        }

        return completeClassicGroupJoin(group);
    }

    /**
     * Remove a pending member from the group and possibly complete the join phase.
     *
     * @param group     The group.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> removePendingMemberAndUpdateClassicGroup(
        ClassicGroup group,
        String memberId
    ) {
        group.remove(memberId);

        if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeClassicGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Gets or maybe creates a classic group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ClassicGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a classic group.
     *
     * Package private for testing.
     */
    ClassicGroup getOrMaybeCreateClassicGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Classic group %s not found.", groupId));
        }

        if (group == null) {
            ClassicGroup classicGroup = new ClassicGroup(logContext, groupId, ClassicGroupState.EMPTY, time);
            groupStore.addGroup(groupId, classicGroup);
            return classicGroup;
        } else {
            if (group.type() == CLASSIC) {
                return (ClassicGroup) group;
            } else {
                throw new GroupIdNotFoundException(String.format("Group %s is not a classic group.", groupId));
            }
        }
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeClassicGroup(
        String groupId
    ) {
        Group group = groupStore.removeGroup(groupId);
        if (group == null) {
            log.warn("The group {} is not existing.", groupId);
        } else if (group.type() != CLASSIC) {
            log.warn("Removed group {} with an unknown group type {}.", groupId, group.type());
        } else {
            // The classic group size counter is implemented as scheduled task.
        }
    }

    /**
     * Cancels the session timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(groupSessionTimeoutKey(groupId, memberId));
    }

    public static String groupSessionTimeoutKey(String groupId, String memberId) {
        return "session-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Validates the online downgrade if a consumer member is fenced from the consumer group.
     *
     * @param consumerGroup     The ConsumerGroup.
     * @param fencedMemberId    The fenced member id.
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithFencedMember(ConsumerGroup consumerGroup, String fencedMemberId) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(fencedMemberId)) {
            return false;
        } else if (consumerGroup.numMembers() <= 1) {
            log.debug("Skip downgrading the consumer group {} to classic group because it's empty.",
                consumerGroup.groupId());
            return false;
        } else if (!consumerGroupMigrationPolicy.isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() - 1 > classicGroupMaxSize) {
            log.info("Cannot downgrade consumer group {} to classic group because its group size is greater than classic group max size.",
                consumerGroup.groupId());
            return false;
        }
        return true;
    }


    public static String consumerGroupRebalanceTimeoutKey(String groupId, String memberId) {
        return "rebalance-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Validates if the provided regular expression is valid.
     *
     * @param regex The regular expression to validate.
     * @throws InvalidRegularExpression if the regular expression is invalid.
     */
    private static void throwIfRegularExpressionIsInvalid(
        String regex
    ) throws InvalidRegularExpression {
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException ex) {
            throw new InvalidRegularExpression(
                String.format("SubscribedTopicRegex `%s` is not a valid regular expression: %s.",
                    regex, ex.getDescription()));
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
    private static void throwIfStaticMemberIsUnknown(ConsumerGroupMember staticMember, String receivedInstanceId) {
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
    private static void throwIfInstanceIdIsFenced(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId, Logger log) {
        if (!member.memberId().equals(receivedMemberId)) {
            log.info("[GroupId {}] Static member {} with instance id {} is fenced by existing member {}.",
                groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.FENCED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " was fenced by member " + member.memberId() + ".");
        }
    }

    /**
     * Write records to replace the old member by the new member.
     *
     * @param records   The list of records to append to.
     * @param group     The consumer group.
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private static void replaceMember(
        List<CoordinatorRecord> records,
        ConsumerGroup group,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        String groupId = group.groupId();

        // Remove the member without canceling its timers in case the change is reverted. If the
        // change is not reverted, the group validation will fail and the timer will do nothing.
        removeMember(records, groupId, oldMember.memberId());

        // Generate records.
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(
            groupId,
            newMember
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
            groupId,
            newMember.memberId(),
            group.targetAssignment(oldMember.memberId()).partitions()
        ));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(
            groupId,
            newMember
        ));
    }

    /**
     * Write tombstones for the member. The order matters here.
     *
     * @param records       The list of records to append the member assignment tombstone records.
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    public static void removeMember(List<CoordinatorRecord> records, String groupId, String memberId) {
        records.add(newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId));
        records.add(newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId));
    }

    /**
     * Validates if the received instanceId has been released from the group
     *
     * @param member                The consumer group member.
     * @param groupId               The consumer group id.
     * @param receivedMemberId      The member id received in the request.
     * @param receivedInstanceId    The instance id received in the request.
     *
     * @throws UnreleasedInstanceIdException if the instance id received in the request is still in use by an existing static member.
     */
    private static void throwIfInstanceIdIsUnreleased(ConsumerGroupMember member, String groupId, String receivedMemberId, String receivedInstanceId, Logger log) {
        if (member.memberEpoch() != LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            // The new member can't join.
            log.info("[GroupId {}] Static member {} with instance id {} cannot join the group because the instance id is" +
                " owned by member {}.", groupId, receivedMemberId, receivedInstanceId, member.memberId());
            throw Errors.UNRELEASED_INSTANCE_ID.exception("Static member " + receivedMemberId + " with instance id "
                + receivedInstanceId + " cannot join the group because the instance id is owned by " + member.memberId() + " member.");
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The consumer group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedTopicPartitions  The owned partitions.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead of or behind the epoch known
     *                                    by this coordinator.
     */
    private static void throwIfConsumerGroupMemberEpochIsInvalid(
        ConsumerGroupMember member,
        int receivedMemberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The consumer group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch() || !isSubset(ownedTopicPartitions, member.assignedPartitions())) {
                throw new FencedMemberEpochException("The consumer group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    /**
     * Verifies that the partitions currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the consumer
     * only owns partitions which are in the assigned partitions. It does not match if
     * it owns any other partitions.
     *
     * @param ownedTopicPartitions  The partitions provided by the consumer in the request.
     * @param target                The partitions that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private static boolean isSubset(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        Map<Uuid, Set<Integer>> target
    ) {
        if (ownedTopicPartitions == null) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = target.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }
}
