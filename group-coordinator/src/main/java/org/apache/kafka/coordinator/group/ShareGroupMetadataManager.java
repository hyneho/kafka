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
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupAssignmentBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.image.MetadataImage;
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
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.Utils.assignmentToString;
import static org.apache.kafka.coordinator.group.modern.ModernGroupMember.hasAssignedPartitionsChanged;

public class ShareGroupMetadataManager {

    GroupStore groupStore;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * The share group partition assignor.
     */
    private final ShareGroupPartitionAssignor shareGroupAssignor;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * The share group metadata refresh interval.
     */
    private final int shareGroupMetadataRefreshIntervalMs;

    /**
     * The maximum number of members allowed in a single share group.
     */
    private final int shareGroupMaxSize;

    /**
     * The session timeout for share groups.
     */
    private final int shareGroupSessionTimeoutMs;

    /**
     * The heartbeat interval for share groups.
     */
    private final int shareGroupHeartbeatIntervalMs;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The group manager.
     */
    private final GroupConfigManager groupConfigManager;

    public ShareGroupMetadataManager(
        GroupStore groupStore,
        ShareGroupPartitionAssignor shareGroupAssignor,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        int shareGroupMaxSize,
        int shareGroupMetadataRefreshIntervalMs,
        int shareGroupSessionTimeoutMs,
        int shareGroupHeartbeatIntervalMs,
        LogContext logContext,
        GroupConfigManager groupConfigManager) {
        this.groupStore = groupStore;
        this.shareGroupAssignor = shareGroupAssignor;
        this.time = time;
        this.timer = timer;
        this.shareGroupMaxSize = shareGroupMaxSize;
        this.metadataImage = groupStore.image();
        this.shareGroupMetadataRefreshIntervalMs = shareGroupMetadataRefreshIntervalMs;
        this.shareGroupSessionTimeoutMs = shareGroupSessionTimeoutMs;
        this.shareGroupHeartbeatIntervalMs = shareGroupHeartbeatIntervalMs;
        this.logContext = logContext;
        this.log = this.logContext.logger(ShareGroupMetadataManager.class);
        this.groupConfigManager = groupConfigManager;
    }

    /**
     * Handles a ShareGroupDescribe request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ShareGroupDescribeResponseData.DescribedGroup.
     */
    public List<ShareGroupDescribeResponseData.DescribedGroup> shareGroupDescribe(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<ShareGroupDescribeResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                describedGroups.add(shareGroup(groupId, committedOffset).asDescribedGroup(
                    committedOffset,
                    shareGroupAssignor.name(),
                    metadataImage.topics()
                ));
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new ShareGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                );
            }
        });

        return describedGroups;
    }

    /**
     * Gets a share group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a consumer group.
     */
    public ShareGroup shareGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId, committedOffset);

        if (group.type() == SHARE) {
            return (ShareGroup) group;
        } else {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.",
                groupId));
        }
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ShareGroupHeartbeat request.
     *
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupHeartbeat(
        RequestContext context,
        ShareGroupHeartbeatRequestData request
    ) throws ApiException {
        throwIfShareGroupHeartbeatRequestIsInvalid(request);

        if (request.memberEpoch() == ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH) {
            // -1 means that the member wants to leave the group.
            return shareGroupLeave(
                request.groupId(),
                request.memberId(),
                request.memberEpoch());
        }
        // Otherwise, it is a regular heartbeat.
        return shareGroupHeartbeat(
            request.groupId(),
            request.memberId(),
            request.memberEpoch(),
            request.rackId(),
            context.clientId(),
            context.clientAddress.toString(),
            request.subscribedTopicNames());
    }

    /**
     * Handles a ShareGroupHeartbeat request.
     *
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param rackId                The rack id from the request or null.
     * @param clientId              The client id.
     * @param clientHost            The client host.
     * @param subscribedTopicNames  The list of subscribed topic names from the request or null.
     *
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String rackId,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();

        // Get or create the share group.
        boolean createIfNotExists = memberEpoch == 0;
        final ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, createIfNotExists);
        throwIfShareGroupIsFull(group, memberId);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        ShareGroupMember member = getOrMaybeSubscribeShareGroupMember(
            group,
            memberId,
            memberEpoch,
            createIfNotExists
        );

        // 1. Create or update the member. If the member is new or has changed, a ShareGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ShareGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ShareGroupMetadataValue record to the partition.
        ShareGroupMember updatedMember = new ShareGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .setClientId(clientId)
            .setClientHost(clientHost)
            .build();

        boolean bumpGroupEpoch = hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records
        );

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        SubscriptionType subscriptionType = group.subscriptionType();

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            Map<String, Integer> subscribedTopicNamesMap = group.computeSubscribedTopicNames(member, updatedMember);
            subscriptionMetadata = group.computeSubscriptionMetadata(
                subscribedTopicNamesMap,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            int numMembers = group.numMembers();
            if (!group.hasMember(updatedMember.memberId())) {
                numMembers++;
            }

            subscriptionType = ModernGroup.subscriptionType(
                subscribedTopicNamesMap,
                numMembers
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    groupId, subscriptionMetadata);
                bumpGroupEpoch = true;
                records.add(newShareGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newShareGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
            }

            group.setMetadataRefreshDeadline(currentTimeMs + shareGroupMetadataRefreshIntervalMs, groupEpoch);
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The delta between
        // the existing and the new target assignment is persisted to the partition.
        final int targetAssignmentEpoch;
        final Assignment targetAssignment;

        if (groupEpoch > group.assignmentEpoch()) {
            targetAssignment = updateTargetAssignment(
                group,
                groupEpoch,
                updatedMember,
                subscriptionMetadata,
                subscriptionType,
                records
            );
            targetAssignmentEpoch = groupEpoch;
        } else {
            targetAssignmentEpoch = group.assignmentEpoch();
            targetAssignment = group.targetAssignment(updatedMember.memberId());
        }

        // 3. Reconcile the member's assignment with the target assignment if the member is not
        // fully reconciled yet.
        updatedMember = maybeReconcile(
            groupId,
            updatedMember,
            targetAssignmentEpoch,
            targetAssignment,
            records
        );

        scheduleShareGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ShareGroupHeartbeatResponseData response = new ShareGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(shareGroupHeartbeatIntervalMs(groupId));

        // The assignment is only provided in the following cases:
        // 1. The member just joined or rejoined to group (epoch equals to zero);
        // 2. The member's assignment has been updated.
        if (memberEpoch == 0 || hasAssignedPartitionsChanged(member, updatedMember)) {
            response.setAssignment(createShareGroupResponseAssignment(updatedMember));
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Validates the ShareGroupHeartbeat request.
     *
     * @param request The request to validate.
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfShareGroupHeartbeatRequestIsInvalid(
        ShareGroupHeartbeatRequestData request
    ) throws InvalidRequestException, UnsupportedAssignorException {
        throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");

        if (request.memberEpoch() == 0) {
            if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
                throw new InvalidRequestException("SubscribedTopicNames must be set in first request.");
            }
        } else if (request.memberEpoch() < ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH) {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }
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
     * Gets or maybe creates a share group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ShareGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ShareGroup getOrMaybeCreatePersistedShareGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId);

        if (group == null && !createIfNotExists) {
            throw new IllegalStateException(String.format("Share group %s not found.", groupId));
        }

        if (group == null) {
            ShareGroup shareGroup = new ShareGroup(groupStore.snapshotRegistry(), groupId);
            groupStore.addGroup(groupId, shareGroup);
            return shareGroup;
        }

        if (group.type() != SHARE) {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a share group.", groupId));
        }

        return (ShareGroup) group;
    }

    /**
     * Handles leave request from a share group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     * @param memberEpoch   The member epoch from the request.
     *
     * @return A Result containing the ShareGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> shareGroupLeave(
        String groupId,
        String memberId,
        int memberEpoch
    ) throws ApiException {
        ShareGroup group = shareGroup(groupId);
        ShareGroupHeartbeatResponseData response = new ShareGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(memberEpoch);

        ShareGroupMember member = group.getOrMaybeCreateMember(memberId, false);
        log.info("[GroupId {}] Member {} left the share group.", groupId, memberId);

        return shareGroupFenceMember(group, member, response);
    }

    /**
     * Checks whether the share group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The share group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfShareGroupIsFull(
        ShareGroup group,
        String memberId
    ) throws GroupMaxSizeReachedException {
        // The member is rejected, if the share group has reached its maximum capacity, or it is not
        // a member of the share group.
        if (group.numMembers() >= shareGroupMaxSize && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The share group has reached its maximum capacity of "
                + shareGroupMaxSize + " members.");
        }
    }

    /**
     * Gets or subscribes a new dynamic share group member.
     *
     * @param group                 The share group.
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param createIfNotExists     Whether the member should be created or not.
     *
     * @return The existing share group member or a new one.
     */
    private ShareGroupMember getOrMaybeSubscribeShareGroupMember(
        ShareGroup group,
        String memberId,
        int memberEpoch,
        boolean createIfNotExists
    ) {
        ShareGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        throwIfShareGroupMemberEpochIsInvalid(member, memberEpoch);
        if (createIfNotExists) {
            log.info("[GroupId {}] Member {} joins the share group using the share protocol.",
                group.groupId(), memberId);
        }
        return member;
    }

    /**
     * Creates the member subscription record if the updatedMember is different from
     * the old member. Returns true if the subscribedTopicNames has changed.
     *
     * @param groupId       The group id.
     * @param member        The old member.
     * @param updatedMember The updated member.
     * @param records       The list to accumulate any new records.
     * @return A boolean indicating whether the updatedMember has a different
     *         subscribedTopicNames from the old member.
     */
    private boolean hasMemberSubscriptionChanged(
        String groupId,
        ShareGroupMember member,
        ShareGroupMember updatedMember,
        List<CoordinatorRecord> records
    ) {
        String memberId = updatedMember.memberId();
        if (!updatedMember.equals(member)) {
            records.add(newShareGroupMemberSubscriptionRecord(groupId, updatedMember));

            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.info("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the target assignment according to the updated member and subscription metadata.
     *
     * @param group                 The ShareGroup.
     * @param groupEpoch            The group epoch.
     * @param updatedMember         The updated member.
     * @param subscriptionMetadata  The subscription metadata.
     * @param subscriptionType      The group subscription type.
     * @param records               The list to accumulate any new records.
     * @return The new target assignment.
     */
    private Assignment updateTargetAssignment(
        ShareGroup group,
        int groupEpoch,
        ShareGroupMember updatedMember,
        Map<String, TopicMetadata> subscriptionMetadata,
        SubscriptionType subscriptionType,
        List<CoordinatorRecord> records
    ) {
        try {
            TargetAssignmentBuilder<ShareGroupMember> assignmentResultBuilder =
                new TargetAssignmentBuilder<ShareGroupMember>(group.groupId(), groupEpoch, shareGroupAssignor)
                    .withMembers(group.members())
                    .withSubscriptionMetadata(subscriptionMetadata)
                    .withSubscriptionType(subscriptionType)
                    .withTargetAssignment(group.targetAssignment())
                    .withInvertedTargetAssignment(group.invertedTargetAssignment())
                    .withTopicsImage(metadataImage.topics())
                    .withTargetAssignmentRecordBuilder(GroupCoordinatorRecordHelpers::newShareGroupTargetAssignmentRecord)
                    .withTargetAssignmentEpochRecordBuilder(GroupCoordinatorRecordHelpers::newShareGroupTargetAssignmentEpochRecord)
                    .addOrUpdateMember(updatedMember.memberId(), updatedMember);

            long startTimeMs = time.milliseconds();
            TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                assignmentResultBuilder.build();
            long assignorTimeMs = time.milliseconds() - startTimeMs;

            log.info("[GroupId {}] Computed a new target assignment for epoch {} with '{}' assignor in {}ms: {}.",
                group.groupId(), groupEpoch, shareGroupAssignor, assignorTimeMs, assignmentResult.targetAssignment());

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
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @param records               The list to accumulate any new records.
     * @return The received member if no changes have been made; or a new
     *         member containing the new assignment.
     */
    private ShareGroupMember maybeReconcile(
        String groupId,
        ShareGroupMember member,
        int targetAssignmentEpoch,
        Assignment targetAssignment,
        List<CoordinatorRecord> records
    ) {
        if (member.isReconciledTo(targetAssignmentEpoch)) {
            return member;
        }

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
            .build();

        if (!updatedMember.equals(member)) {
            records.add(newShareGroupCurrentAssignmentRecord(groupId, updatedMember));

            log.info("[GroupId {}] Member {} new assignment state: epoch={}, previousEpoch={}, state={}, "
                    + "assignedPartitions={}.",
                groupId, updatedMember.memberId(), updatedMember.memberEpoch(), updatedMember.previousMemberEpoch(), updatedMember.state(),
                assignmentToString(updatedMember.assignedPartitions()));
        }

        return updatedMember;
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleShareGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        scheduleShareGroupSessionTimeout(groupId, memberId, shareGroupSessionTimeoutMs(groupId));
    }

    /**
     * Get the session timeout of the provided share group.
     */
    private int shareGroupSessionTimeoutMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::shareSessionTimeoutMs)
            .orElse(shareGroupSessionTimeoutMs);
    }

    /**
     * Schedules (or reschedules) the session timeout for the share group member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleShareGroupSessionTimeout(
        String groupId,
        String memberId,
        int sessionTimeoutMs
    ) {
        timer.schedule(
            groupSessionTimeoutKey(groupId, memberId),
            sessionTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> shareGroupFenceMemberOperation(groupId, memberId, "the member session expired")
        );
    }

    /**
     * Get the heartbeat interval of the provided share group.
     */
    private int shareGroupHeartbeatIntervalMs(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::shareHeartbeatIntervalMs)
            .orElse(shareGroupHeartbeatIntervalMs);
    }

    private ShareGroupHeartbeatResponseData.Assignment createShareGroupResponseAssignment(
        ShareGroupMember member
    ) {
        return new ShareGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(fromShareGroupAssignmentMap(member.assignedPartitions()));
    }

    /**
     * An overloaded method of {@link GroupMetadataManager#shareGroup(String, long)}
     */
    ShareGroup shareGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        return shareGroup(groupId, Long.MAX_VALUE);
    }

    /**
     * Removes a member from a share group.
     *
     * @param group       The group.
     * @param member      The member.
     *
     * @return A list of records to be applied to the state.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> shareGroupFenceMember(
        ShareGroup group,
        ShareGroupMember member,
        T response
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();
        records.add(newShareGroupCurrentAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newShareGroupTargetAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newShareGroupMemberSubscriptionTombstoneRecord(group.groupId(), member.memberId()));

        // We update the subscription metadata without the leaving member.
        Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
            group.computeSubscribedTopicNames(member, null),
            metadataImage.topics(),
            metadataImage.cluster()
        );

        if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
            log.info("[GroupId {}] Computed new subscription metadata: {}.",
                group.groupId(), subscriptionMetadata);
            records.add(newShareGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
        }

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newShareGroupEpochRecord(group.groupId(), groupEpoch));

        cancelGroupSessionTimeout(group.groupId(), member.memberId());

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The share group member.
     * @param receivedMemberEpoch   The member epoch.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead of or behind the epoch known
     *                                    by this coordinator.
     */
    private void throwIfShareGroupMemberEpochIsInvalid(
        ShareGroupMember member,
        int receivedMemberEpoch
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The share group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch()) {
                throw new FencedMemberEpochException("The share group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    public static String groupSessionTimeoutKey(String groupId, String memberId) {
        return "session-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Fences a member from a share group. Returns an empty CoordinatorResult
     * if the group or the member doesn't exist.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     * @param reason    The reason for fencing the member.
     *
     * @return The CoordinatorResult to be applied.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> shareGroupFenceMemberOperation(
        String groupId,
        String memberId,
        String reason
    ) {
        try {
            ShareGroup group = shareGroup(groupId);
            ShareGroupMember member = group.getOrMaybeCreateMember(memberId, false);
            log.info("[GroupId {}] Member {} fenced from the group because {}.",
                groupId, memberId, reason);

            return shareGroupFenceMember(group, member, null);
        } catch (GroupIdNotFoundException ex) {
            log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                groupId, memberId);
        } catch (UnknownMemberIdException ex) {
            log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                groupId, memberId);
        }

        return new CoordinatorResult<>(Collections.emptyList());
    }

    private List<ShareGroupHeartbeatResponseData.TopicPartitions> fromShareGroupAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ShareGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
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

    /**
     * Replays ShareGroupMemberMetadataKey/Value to update the hard state of
     * the share group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ShareGroupMemberMetadataKey key.
     * @param value A ShareGroupMemberMetadataValue record.
     */
    public void replay(
        ShareGroupMemberMetadataKey key,
        ShareGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, value != null);
        Set<String> oldSubscribedTopicNames = new HashSet<>(shareGroup.subscribedTopicNames().keySet());

        if (value != null) {
            ShareGroupMember oldMember = shareGroup.getOrMaybeCreateMember(memberId, true);
            shareGroup.updateMember(new ShareGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ShareGroupMember oldMember = shareGroup.getOrMaybeCreateMember(memberId, false);
            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " with invalid leave group epoch.");
            }
            if (shareGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but member exists in target assignment.");
            }
            shareGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, shareGroup.subscribedTopicNames().keySet());
    }

    /**
     * Replays ShareGroupMetadataKey/Value to update the hard state of
     * the share group. It updates the group epoch of the share
     * group or deletes the share group.
     *
     * @param key   A ShareGroupMetadataKey key.
     * @param value A ShareGroupMetadataValue record.
     */
    public void replay(
        ShareGroupMetadataKey key,
        ShareGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, true);
            shareGroup.setGroupEpoch(value.epoch());
        } else {
            ShareGroup shareGroup = getOrMaybeCreatePersistedShareGroup(groupId, false);
            if (!shareGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + shareGroup.members().size() + " members.");
            }
            if (!shareGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + shareGroup.targetAssignment().size()
                    + " members.");
            }
            if (shareGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but target assignment epoch in invalid.");
            }
            removeShareGroup(groupId);
        }

    }

    /**
     * Replays ShareGroupPartitionMetadataKey/Value to update the hard state of
     * the share group. It updates the subscription metadata of the share
     * group.
     *
     * @param key   A ShareGroupPartitionMetadataKey key.
     * @param value A ShareGroupPartitionMetadataValue record.
     */
    public void replay(
        ShareGroupPartitionMetadataKey key,
        ShareGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

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
     * Replays ShareGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the share group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ShareGroupTargetAssignmentMemberKey key.
     * @param value A ShareGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ShareGroupTargetAssignmentMemberKey key,
        ShareGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

        if (value != null) {
            group.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            group.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ShareGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the share group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ShareGroupTargetAssignmentMetadataKey key.
     * @param value A ShareGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ShareGroupTargetAssignmentMetadataKey key,
        ShareGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();
        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);

        if (value != null) {
            group.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            if (!group.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + group.targetAssignment().size() + " members.");
            }
            group.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ShareGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the share group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ShareGroupCurrentMemberAssignmentKey key.
     * @param value A ShareGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ShareGroupCurrentMemberAssignmentKey key,
        ShareGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ShareGroup group = getOrMaybeCreatePersistedShareGroup(groupId, false);
        ShareGroupMember oldMember = group.getOrMaybeCreateMember(memberId, false);

        if (value != null) {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            group.updateMember(newMember);
        } else {
            ShareGroupMember newMember = new ShareGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Collections.emptyMap())
                .build();
            group.updateMember(newMember);
        }
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.SHARE.toString()), 0);
        groups.forEach(group -> {
            // Nothing for now for the ShareGroup, as no members are persisted.
        });
    }

    /**
     * Called when the partition is unloaded.
     */
    public void onUnloaded() {
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton("SHARE"), 0);
        groups.forEach(group -> {
            ShareGroup shareGroup = (ShareGroup) groupStore.group(group.groupId());
            log.info("[GroupId={}] Unloaded group metadata for group epoch {}.",
                shareGroup.groupId(), shareGroup.groupEpoch());
        });
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
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeShareGroup(
        String groupId
    ) {
        Group group = groupStore.removeGroup(groupId);
        if (group == null) {
            log.warn("The group {} is not existing.", groupId);
        } else if (group.type() != SHARE) {
            log.warn("Removed group {} with an unknown group type {}.", groupId, group.type());
        } else {
            // Nothing for now, but we may want to add metrics in the future.
        }
    }
}

