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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.ILLEGAL_GENERATION;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Group.GroupType.CLASSIC;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
import static org.apache.kafka.coordinator.group.Utils.toConsumerProtocolAssignment;
import static org.apache.kafka.coordinator.group.Utils.toTopicPartitions;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;

public class ClassicGroupMetadataManager {

    private final GroupStore groupStore;
    private final GroupMetaManagerHelper helper;

    /**
     * The maximum number of members allowed in a single classic group.
     */
    private final int classicGroupMaxSize;

    /**
     * Initial rebalance delay for members joining a classic group.
     */
    private final int classicGroupInitialRebalanceDelayMs;

    /**
     * The metadata refresh interval.
     */
    private final int consumerGroupMetadataRefreshIntervalMs;

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    private final int classicGroupNewMemberJoinTimeoutMs;

    /**
     * The maximum number of members allowed in a single consumer group.
     */
    private final int consumerGroupMaxSize;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The supported consumer group partition assignors keyed by their name.
     */
    private final Map<String, ConsumerGroupPartitionAssignor> consumerGroupAssignors;

    /**
     * The default consumer group assignor used.
     */
    private final ConsumerGroupPartitionAssignor defaultConsumerGroupAssignor;

    /**
     * The config indicating whether group protocol upgrade/downgrade is allowed.
     */
    private final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy;


    /**
     * An empty result returned to the state machine. This means that
     * there are no records to append to the log.
     *
     * Package private for testing.
     */
    static final CoordinatorResult<Void, CoordinatorRecord> EMPTY_RESULT =
        new CoordinatorResult<>(Collections.emptyList(), CompletableFuture.completedFuture(null), false);


    public ClassicGroupMetadataManager(
        GroupStore groupStore,
        GroupCoordinatorMetricsShard metrics,
        int classicGroupMaxSize,
        int classicGroupInitialRebalanceDelayMs,
        int consumerGroupMetadataRefreshIntervalMs,
        int classicGroupNewMemberJoinTimeoutMs,
        int consumerGroupMaxSize,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        Time time,
        LogContext logContext,
        List<ConsumerGroupPartitionAssignor> consumerGroupAssignors,
        ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy) {

        this.groupStore = groupStore;
        this.metrics = metrics;
        this.classicGroupMaxSize = classicGroupMaxSize;
        this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
        this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
        this.classicGroupNewMemberJoinTimeoutMs = classicGroupNewMemberJoinTimeoutMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.timer = timer;
        this.time = time;
        this.logContext = logContext;
        this.log = logContext.logger(ClassicGroupMetadataManager.class);
        this.consumerGroupAssignors = consumerGroupAssignors.stream().collect(Collectors.toMap(ConsumerGroupPartitionAssignor::name, Function.identity()));
        this.defaultConsumerGroupAssignor = consumerGroupAssignors.get(0);
        this.consumerGroupMigrationPolicy = consumerGroupMigrationPolicy;
        this.helper = new GroupMetaManagerHelper(groupStore, time, timer, logContext, log, consumerGroupMigrationPolicy, classicGroupMaxSize, classicGroupInitialRebalanceDelayMs);
    }

    /**
     * Handles a DescribeGroup request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the DescribeGroupsResponseData.DescribedGroup.
     */
    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<DescribeGroupsResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                ClassicGroup group = classicGroup(groupId, committedOffset);

                if (group.isInState(STABLE)) {
                    if (!group.protocolName().isPresent()) {
                        throw new IllegalStateException("Invalid null group protocol for stable group");
                    }

                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setProtocolData(group.protocolName().get())
                        .setMembers(group.allMembers().stream()
                            .map(member -> member.describe(group.protocolName().get()))
                            .collect(Collectors.toList())
                        )
                    );
                } else {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setMembers(group.allMembers().stream()
                            .map(member -> member.describeNoMetadata())
                            .collect(Collectors.toList())
                        )
                    );
                }
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setGroupState(DEAD.toString())
                );
            }
        });
        return describedGroups;
    }

    /**
     * Gets a classic group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A ClassicGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a classic group.
     */
    public ClassicGroup classicGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId, committedOffset);

        if (group.type() == CLASSIC) {
            return (ClassicGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a classic group.", groupId));
        }
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        Map<ClassicGroupState, Long> classicGroupSizeCounter = new HashMap<>();
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.CONSUMER.toString()), 0);
        groups.forEach(group -> {
            final String groupId = group.groupId();
            ClassicGroup classicGroup = (ClassicGroup) groupStore.group(groupId);
            log.info("Loaded classic group {} with {} members.", groupId, classicGroup.allMembers().size());
            classicGroup.allMembers().forEach(member -> {
                log.debug("Loaded member {} in classic group {}.", member.memberId(), groupId);
                helper.rescheduleClassicGroupMemberHeartbeat(classicGroup, member);
            });

            if (classicGroup.numMembers() > classicGroupMaxSize) {
                // In case the max size config has changed.
                helper.prepareRebalance(classicGroup, "Freshly-loaded group " + groupId +
                    " (size " + classicGroup.numMembers() + ") is over capacity " + classicGroupMaxSize +
                    ". Rebalancing in order to give a chance for consumers to commit offsets");
            }

            classicGroupSizeCounter.compute(classicGroup.currentState(), Utils::incValue);
        });
    }

    /**
     * Called when the partition is unloaded.
     * ClassicGroup: Complete all awaiting join and sync futures. Transition group to Dead.
     */
    public void onUnloaded() {
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.CONSUMER.toString()), 0);
        groups.forEach(group -> {
            final String groupId = group.groupId();
            ClassicGroup classicGroup = (ClassicGroup) groupStore.group(groupId);
            log.info("[GroupId={}] Unloading group metadata for generation {}.",
                classicGroup.groupId(), classicGroup.generationId());

            classicGroup.transitionTo(DEAD);
        });
    }

    /**
     * Handle a JoinGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupJoin(
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Group group = groupStore.group(request.groupId(), Long.MAX_VALUE);
        if (group != null) {
            if (group.type() == CONSUMER && !group.isEmpty()) {
                // classicGroupJoinToConsumerGroup takes the join requests to non-empty consumer groups.
                // The empty consumer groups should be converted to classic groups in classicGroupJoinToClassicGroup.
                return classicGroupJoinToConsumerGroup((ConsumerGroup) group, context, request, responseFuture);
            } else if (group.type() == CONSUMER || group.type() == CLASSIC) {
                return classicGroupJoinToClassicGroup(context, request, responseFuture);
            } else {
                // Group exists but it's not a consumer group
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(UNKNOWN_MEMBER_ID)
                    .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
                );
                return EMPTY_RESULT;
            }
        } else {
            return classicGroupJoinToClassicGroup(context, request, responseFuture);
        }
    }

    /**
     * Handle a SyncGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains records to append.
     */
    public CoordinatorResult<Void, CoordinatorRecord> classicGroupSync(
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws UnknownMemberIdException {
        Group group;
        try {
            group = groupStore.group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }

        if (group.isEmpty()) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }

        if (group.type() == CLASSIC) {
            return classicGroupSyncToClassicGroup((ClassicGroup) group, context, request, responseFuture);
        } else if (group.type() == CONSUMER) {
            return classicGroupSyncToConsumerGroup((ConsumerGroup) group, context, request, responseFuture);
        } else {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
            return EMPTY_RESULT;
        }
    }

    /**
     * Handle a classic group HeartbeatRequest.
     *
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    public CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        Group group;
        try {
            group = groupStore.group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            throw new UnknownMemberIdException(
                String.format("Group %s not found.", request.groupId())
            );
        }

        if (group.type() == CLASSIC) {
            return classicGroupHeartbeatToClassicGroup((ClassicGroup) group, context, request);
        } else if (group.type() == CONSUMER) {
            return classicGroupHeartbeatToConsumerGroup((ConsumerGroup) group, context, request);
        } else {
            throw new UnknownMemberIdException(
                String.format("Group %s not found.", request.groupId())
            );
        }
    }

    /**
     * Handle a classic group HeartbeatRequest to a consumer group. A response with
     * REBALANCE_IN_PROGRESS is returned if 1) the member epoch is smaller than the
     * group epoch, 2) the member is in UNREVOKED_PARTITIONS, or 3) the member is in
     * UNRELEASED_PARTITIONS and all its partitions pending assignment are free.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    private CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeatToConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        HeartbeatRequestData request
    ) throws UnknownMemberIdException, FencedInstanceIdException, IllegalGenerationException {
        String groupId = request.groupId();
        String memberId = request.memberId();
        String instanceId = request.groupInstanceId();
        ConsumerGroupMember member = validateConsumerGroupMember(group, memberId, instanceId);

        throwIfMemberDoesNotUseClassicProtocol(member);
        throwIfGenerationIdUnmatched(memberId, member.memberEpoch(), request.generationId());

        scheduleConsumerGroupSessionTimeout(groupId, memberId, member.classicProtocolSessionTimeout().get());

        Errors error = Errors.NONE;
        // The member should rejoin if any of the following conditions is met.
        // 1) The group epoch is bumped so the member need to rejoin to catch up.
        // 2) The member needs to revoke some partitions and rejoin to reconcile with the new epoch.
        // 3) The member's partitions pending assignment are free, so it can rejoin to get the complete assignment.
        if (member.memberEpoch() < group.groupEpoch() ||
            member.state() == MemberState.UNREVOKED_PARTITIONS ||
            (member.state() == MemberState.UNRELEASED_PARTITIONS && !group.waitingOnUnreleasedPartition(member))) {
            error = Errors.REBALANCE_IN_PROGRESS;
            scheduleConsumerGroupJoinTimeoutIfAbsent(groupId, memberId, member.rebalanceTimeoutMs());
        }

        return new CoordinatorResult<>(
            Collections.emptyList(),
            new HeartbeatResponseData().setErrorCode(error.code())
        );
    }

    /**
     * Handle a classic group HeartbeatRequest to a classic group.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The coordinator result that contains the heartbeat response.
     */
    private CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> classicGroupHeartbeatToClassicGroup(
        ClassicGroup group,
        RequestContext context,
        HeartbeatRequestData request
    ) {
        validateClassicGroupHeartbeat(group, request.memberId(), request.groupInstanceId(), request.generationId());

        switch (group.currentState()) {
            case EMPTY:
                return new CoordinatorResult<>(
                    Collections.emptyList(),
                    new HeartbeatResponseData().setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );

            case PREPARING_REBALANCE:
                helper.rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    Collections.emptyList(),
                    new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code())
                );

            case COMPLETING_REBALANCE:
            case STABLE:
                // Consumers may start sending heartbeats after join-group response, while the group
                // is in CompletingRebalance state. In this case, we should treat them as
                // normal heartbeat requests and reset the timer
                helper.rescheduleClassicGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new CoordinatorResult<>(
                    Collections.emptyList(),
                    new HeartbeatResponseData()
                );

            default:
                throw new IllegalStateException("Reached unexpected state " +
                    group.currentState() + " for group " + group.groupId());
        }
    }

    /**
     * Validates a classic group heartbeat request.
     *
     * @param group              The group.
     * @param memberId           The member id.
     * @param groupInstanceId    The group instance id.
     * @param generationId       The generation id.
     *
     * @throws CoordinatorNotAvailableException If group is Dead.
     * @throws IllegalGenerationException       If the generation id in the request and the generation id of the
     *                                          group does not match.
     */
    private void validateClassicGroupHeartbeat(
        ClassicGroup group,
        String memberId,
        String groupInstanceId,
        int generationId
    ) throws CoordinatorNotAvailableException, IllegalGenerationException {
        if (group.isInState(DEAD)) {
            throw COORDINATOR_NOT_AVAILABLE.exception();
        } else {
            group.validateMember(
                memberId,
                groupInstanceId,
                "heartbeat"
            );

            if (generationId != group.generationId()) {
                throw ILLEGAL_GENERATION.exception();
            }
        }
    }

    /**
     * Handle a SyncGroupRequest to a ConsumerGroup.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains the appendFuture to return the response.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupSyncToConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws UnknownMemberIdException, FencedInstanceIdException, IllegalGenerationException,
        InconsistentGroupProtocolException, RebalanceInProgressException, IllegalStateException {
        String groupId = request.groupId();
        String memberId = request.memberId();
        String instanceId = request.groupInstanceId();
        ConsumerGroupMember member = validateConsumerGroupMember(group, memberId, instanceId);

        throwIfMemberDoesNotUseClassicProtocol(member);
        throwIfGenerationIdUnmatched(member.memberId(), member.memberEpoch(), request.generationId());
        throwIfClassicProtocolUnmatched(member, request.protocolType(), request.protocolName());
        throwIfRebalanceInProgress(group, member);

        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        appendFuture.whenComplete((__, t) -> {
            if (t == null) {
                helper.cancelConsumerGroupSyncTimeout(groupId, memberId);
                scheduleConsumerGroupSessionTimeout(groupId, memberId, member.classicProtocolSessionTimeout().get());

                responseFuture.complete(new SyncGroupResponseData()
                    .setProtocolType(request.protocolType())
                    .setProtocolName(request.protocolName())
                    .setAssignment(prepareAssignment(member)));
            }
        });

        return new CoordinatorResult<>(Collections.emptyList(), appendFuture, false);
    }

    /**
     * Validates that (1) the instance id exists and is mapped to the member id
     * if the group instance id is provided; and (2) the member id exists in the group.
     *
     * @param group             The consumer group.
     * @param memberId          The member id.
     * @param instanceId        The instance id.
     *
     * @return The ConsumerGroupMember.
     */
    private ConsumerGroupMember validateConsumerGroupMember(
        ConsumerGroup group,
        String memberId,
        String instanceId
    ) throws UnknownMemberIdException, FencedInstanceIdException {
        ConsumerGroupMember member;
        if (instanceId == null) {
            member = group.getOrMaybeCreateMember(memberId, false);
        } else {
            member = group.staticMember(instanceId);
            if (member == null) {
                throw new UnknownMemberIdException(
                    String.format("Member with instance id %s is not a member of group %s.", instanceId, group.groupId())
                );
            }
            throwIfInstanceIdIsFenced(member, group.groupId(), memberId, instanceId);
        }
        return member;
    }

    /**
     * Handle a SyncGroupRequest to a ClassicGroup.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains records to append if the group metadata manager received assignments.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupSyncToClassicGroup(
        ClassicGroup group,
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws IllegalStateException {
        String groupId = request.groupId();
        String memberId = request.memberId();

        Optional<Errors> errorOpt = validateSyncGroup(group, request);
        if (errorOpt.isPresent()) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(errorOpt.get().code()));
        } else if (group.isInState(PREPARING_REBALANCE)) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.REBALANCE_IN_PROGRESS.code()));
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            group.member(memberId).setAwaitingSyncFuture(responseFuture);
            removePendingSyncMember(group, request.memberId());

            // If this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
                log.info("Assignment received from leader {} for group {} for generation {}. " +
                        "The group has {} members, {} of which are static.",
                    memberId, groupId, group.generationId(), group.numMembers(), group.allStaticMemberIds().size());

                // Fill all members with corresponding member assignment. If the member assignment
                // does not exist, fill with an empty assignment.
                Map<String, byte[]> assignment = new HashMap<>();
                request.assignments().forEach(memberAssignment ->
                    assignment.put(memberAssignment.memberId(), memberAssignment.assignment())
                );

                Map<String, byte[]> membersWithMissingAssignment = new HashMap<>();
                group.allMembers().forEach(member -> {
                    if (!assignment.containsKey(member.memberId())) {
                        membersWithMissingAssignment.put(member.memberId(), EMPTY_ASSIGNMENT);
                    }
                });
                assignment.putAll(membersWithMissingAssignment);

                if (!membersWithMissingAssignment.isEmpty()) {
                    log.warn("Setting empty assignments for members {} of {} for generation {}.",
                        membersWithMissingAssignment, groupId, group.generationId());
                }

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    // Another member may have joined the group while we were awaiting this callback,
                    // so we must ensure we are still in the CompletingRebalance state and the same generation
                    // when it gets invoked. if we have transitioned to another state, then do nothing
                    if (group.isInState(COMPLETING_REBALANCE) && request.generationId() == group.generationId()) {
                        if (t != null) {
                            Errors error = GroupMetaManagerHelper.appendGroupMetadataErrorToResponseError(Errors.forException(t));
                            helper.resetAndPropagateAssignmentWithError(group, error);
                            helper.maybePrepareRebalanceOrCompleteJoin(group, "Error " + error + " when storing group assignment" +
                                "during SyncGroup (member: " + memberId + ").");
                        } else {
                            // Update group's assignment and propagate to all members.
                            setAndPropagateAssignment(group, assignment);
                            group.transitionTo(STABLE);
                            metrics.record(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
                        }
                    }
                });

                List<CoordinatorRecord> records = Collections.singletonList(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignment, groupStore.image().features().metadataVersion())
                );
                return new CoordinatorResult<>(records, appendFuture, false);
            }
        } else if (group.isInState(STABLE)) {
            removePendingSyncMember(group, memberId);

            // If the group is stable, we just return the current assignment
            ClassicGroupMember member = group.member(memberId);
            responseFuture.complete(new SyncGroupResponseData()
                .setProtocolType(group.protocolType().orElse(null))
                .setProtocolName(group.protocolName().orElse(null))
                .setAssignment(member.assignment())
                .setErrorCode(Errors.NONE.code()));
        } else if (group.isInState(DEAD)) {
            throw new IllegalStateException("Reached unexpected condition for Dead group " + groupId);
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle a JoinGroupRequest to a ConsumerGroup.
     *
     * @param group          The group to join.
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinToConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final String groupId = request.groupId();
        final String instanceId = request.groupInstanceId();
        final int sessionTimeoutMs = request.sessionTimeoutMs();
        final JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = request.protocols();

        String memberId = request.memberId();
        final boolean isUnknownMember = memberId.equals(UNKNOWN_MEMBER_ID);
        if (isUnknownMember) memberId = Uuid.randomUuid().toString();

        helper.throwIfConsumerGroupIsFull(group, memberId, consumerGroupMaxSize);
        throwIfClassicProtocolIsNotSupported(group, memberId, request.protocolType(), protocols);

        if (JoinGroupRequest.requiresKnownMemberId(request, context.apiVersion())) {
            // A dynamic member requiring a member id joins the group. Send back a response to call for another
            // join group request with allocated member id.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.MEMBER_ID_REQUIRED.code())
            );
            log.info("[GroupId {}] Dynamic member with unknown member id joins the consumer group. " +
                "Created a new member id {} and requesting the member to rejoin with this id.", groupId, memberId);
            return EMPTY_RESULT;
        }

        // Get or create the member.
        final ConsumerGroupMember member;
        if (instanceId == null) {
            member = helper.getOrMaybeSubscribeDynamicConsumerGroupMember(
                group,
                memberId,
                -1,
                Collections.emptyList(),
                true,
                true,
                log
            );
        } else {
            member = helper.getOrMaybeSubscribeStaticConsumerGroupMember(
                group,
                memberId,
                -1,
                instanceId,
                Collections.emptyList(),
                isUnknownMember,
                true,
                records,
                log
            );
        }

        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        Map<String, Integer> subscribedTopicNamesMap = group.subscribedTopicNames();
        SubscriptionType subscriptionType = group.subscriptionType();
        final ConsumerProtocolSubscription subscription = deserializeSubscription(protocols);

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Utils.toOptional(subscription.rackId()))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(request.rebalanceTimeoutMs()))
            .maybeUpdateServerAssignorName(Optional.empty())
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscription.topics()))
            .setClientId(context.clientId())
            .setClientHost(context.clientAddress.toString())
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeoutMs)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols)))
            .build();

        boolean bumpGroupEpoch = helper.hasMemberSubscriptionChanged(
            groupId,
            member,
            updatedMember,
            records,
            log
        );

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

            subscriptionType = ConsumerGroup.subscriptionType(
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
            toTopicPartitions(subscription.ownedPartitions(), groupStore.image().topics()),
            records
        );

        // 4. Maybe downgrade the consumer group if the last static member using the
        // consumer protocol is replaced by the joining static member.
        String existingStaticMemberIdOrNull = group.staticMemberId(request.groupInstanceId());
        boolean downgrade = existingStaticMemberIdOrNull != null &&
            validateOnlineDowngradeWithReplacedMemberId(group, existingStaticMemberIdOrNull);
        if (downgrade) {
            convertToClassicGroup(
                group,
                existingStaticMemberIdOrNull,
                updatedMember,
                records
            );
        }

        final JoinGroupResponseData response = new JoinGroupResponseData()
            .setMemberId(updatedMember.memberId())
            .setGenerationId(updatedMember.memberEpoch())
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocolName(updatedMember.supportedClassicProtocols().get().iterator().next().name());

        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        appendFuture.whenComplete((__, t) -> {
            if (t == null) {
                helper.cancelConsumerGroupJoinTimeout(groupId, response.memberId());
                if (!downgrade) {
                    // If the group is still a consumer group, schedule the session
                    // timeout for the joining member and the sync timeout to ensure
                    // that the member send sync request within the rebalance timeout.
                    scheduleConsumerGroupSessionTimeout(groupId, response.memberId(), sessionTimeoutMs);
                    scheduleConsumerGroupSyncTimeout(groupId, response.memberId(), request.rebalanceTimeoutMs());
                }
                responseFuture.complete(response);
            }
        });

        // If the joining member triggers a valid downgrade, the soft states will be directly
        // updated in the conversion method, so the records don't need to be replayed.
        // If the joining member doesn't trigger a valid downgrade, the group is still a
        // consumer group. We still rely on replaying records to update the soft states.
        return new CoordinatorResult<>(records, null, appendFuture, !downgrade);
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

        classicGroup.allMembers().forEach(member -> helper.rescheduleClassicGroupMemberHeartbeat(classicGroup, member));

        // If the downgrade is triggered by a member leaving the group, a rebalance should be triggered.
        if (joiningMember == null) {
            helper.prepareRebalance(classicGroup, String.format("Downgrade group %s from consumer to classic.", classicGroup.groupId()));
        }
    }

    /**
     * Validates if the received classic member protocols are supported by the group.
     *
     * @param group         The ConsumerGroup.
     * @param memberId      The joining member id.
     * @param protocolType  The joining member protocol type.
     * @param protocols     The joining member protocol collection.
     */
    private void throwIfClassicProtocolIsNotSupported(
        ConsumerGroup group,
        String memberId,
        String protocolType,
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols
    ) {
        if (!group.supportsClassicProtocols(protocolType, ClassicGroupMember.plainProtocolSet(protocols))) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception("Member " + memberId + "'s protocols are not supported.");
        }
    }

    /**
     * Handle a JoinGroupRequest to a ClassicGroup or a group to be created.
     *
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinToClassicGroup(
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        CoordinatorResult<Void, CoordinatorRecord> result = EMPTY_RESULT;
        List<CoordinatorRecord> records = new ArrayList<>();

        String groupId = request.groupId();
        String memberId = request.memberId();

        boolean isUnknownMember = memberId.equals(UNKNOWN_MEMBER_ID);
        // Group is created if it does not exist and the member id is UNKNOWN. if member
        // is specified but group does not exist, request is rejected with GROUP_ID_NOT_FOUND
        ClassicGroup group;
        maybeDeleteEmptyConsumerGroup(groupId, records);
        boolean isNewGroup = groupStore.group(groupId) == null;
        try {
            group = helper.getOrMaybeCreateClassicGroup(groupId, isUnknownMember);
        } catch (GroupIdNotFoundException t) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
            );
            return EMPTY_RESULT;
        }

        if (!acceptJoiningMember(group, memberId)) {
            group.remove(memberId);
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.GROUP_MAX_SIZE_REACHED.code())
            );
        } else if (isUnknownMember) {
            result = classicGroupJoinNewMember(
                context,
                request,
                group,
                responseFuture
            );
        } else {
            result = classicGroupJoinExistingMember(
                context,
                request,
                group,
                responseFuture
            );
        }

        if (isNewGroup && result == EMPTY_RESULT) {
            // If there are no records to append and if a group was newly created, we need to append
            // records to the log to commit the group to the timeline data structure.
            CompletableFuture<Void> appendFuture = new CompletableFuture<>();
            appendFuture.whenComplete((__, t) -> {
                if (t != null) {
                    // We failed to write the empty group metadata. This will revert the snapshot, removing
                    // the newly created group.
                    log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), t.getMessage());

                    responseFuture.complete(new JoinGroupResponseData()
                        .setErrorCode(GroupMetadataManager.appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));
                }
            });

            records.add(
                GroupCoordinatorRecordHelpers.newEmptyGroupMetadataRecord(group, groupStore.image().features().metadataVersion())
            );

            return new CoordinatorResult<>(records, appendFuture, false);
        }
        return result;
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
     * Deserialize the subscription in JoinGroupRequestProtocolCollection.
     * All the protocols have the same subscription, so the method picks a random one.
     *
     * @param protocols The JoinGroupRequestProtocolCollection.
     * @return The ConsumerProtocolSubscription.
     */
    private static ConsumerProtocolSubscription deserializeSubscription(
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols
    ) {
        try {
            return ConsumerProtocol.deserializeConsumerProtocolSubscription(
                ByteBuffer.wrap(protocols.iterator().next().metadata())
            );
        } catch (SchemaException e) {
            throw new IllegalStateException("Malformed embedded consumer protocol in subscription deserialization.");
        }
    }

    /**
     * Validates whether the group id is eligible for an online downgrade if an existing
     * static member is replaced by another new one uses the classic protocol.
     *
     * @param consumerGroup     The group to downgrade.
     * @param replacedMemberId  The replaced member id.
     *
     * @return A boolean indicating whether it's valid to online downgrade the consumer group.
     */
    private boolean validateOnlineDowngradeWithReplacedMemberId(
        ConsumerGroup consumerGroup,
        String replacedMemberId
    ) {
        if (!consumerGroup.allMembersUseClassicProtocolExcept(replacedMemberId)) {
            return false;
        } else if (!consumerGroupMigrationPolicy.isDowngradeEnabled()) {
            log.info("Cannot downgrade consumer group {} to classic group because the online downgrade is disabled.",
                consumerGroup.groupId());
            return false;
        } else if (consumerGroup.numMembers() > classicGroupMaxSize) {
            log.info("Cannot downgrade consumer group {} to classic group because its group size is greater than classic group max size.",
                consumerGroup.groupId());
            return false;
        }
        return true;
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

    /**
     * Schedules a sync timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupSyncTimeout(
        String groupId,
        String memberId,
        int rebalanceTimeoutMs
    ) {
        timer.schedule(
            GroupMetaManagerHelper.consumerGroupSyncKey(groupId, memberId),
            rebalanceTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the member failed to sync within timeout")
        );
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
            //todo
            removeClassicGroup(groupId);
        }
    }

    /**
     * Checks whether the group can accept a joining member.
     *
     * @param group      The group.
     * @param memberId   The member.
     *
     * @return whether the group can accept a joining member.
     */
    private boolean acceptJoiningMember(ClassicGroup group, String memberId) {
        switch (group.currentState()) {
            case EMPTY:
            case DEAD:
                // Always accept the request when the group is empty or dead
                return true;
            case PREPARING_REBALANCE:
                // An existing member is accepted if it is already awaiting. New members are accepted
                // up to the max group size. Note that the number of awaiting members is used here
                // for two reasons:
                // 1) the group size is not reliable as it could already be above the max group size
                //    if the max group size was reduced.
                // 2) using the number of awaiting members allows to kick out the last rejoining
                //    members of the group.
                return (group.hasMember(memberId) && group.member(memberId).isAwaitingJoin()) ||
                    group.numAwaitingJoinResponse() < classicGroupMaxSize;
            case COMPLETING_REBALANCE:
            case STABLE:
                // An existing member is accepted. New members are accepted up to the max group size.
                // Note that the group size is used here. When the group transitions to CompletingRebalance,
                // members who haven't rejoined are removed.
                return group.hasMember(memberId) || group.numMembers() < classicGroupMaxSize;
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Handle a new member classic group join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewMember(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else {
            Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
            String newMemberId = group.generateMemberId(context.clientId(), groupInstanceId);

            if (groupInstanceId.isPresent()) {
                return classicGroupJoinNewStaticMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            } else {
                return classicGroupJoinNewDynamicMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle new static member join. If there was an existing member id for the group instance id,
     * replace that member. Otherwise, add the member and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewStaticMember(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String groupInstanceId = request.groupInstanceId();
        String existingMemberId = group.staticMemberId(groupInstanceId);
        if (existingMemberId != null) {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Replacing previously mapped member {} with this groupInstanceId.",
                groupInstanceId, group.groupId(), group.currentState(), existingMemberId);

            return updateStaticMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                existingMemberId,
                newMemberId,
                responseFuture
            );
        } else {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Created a new member id {} for this member and added to the group.",
                groupInstanceId, group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }
    }

    /**
     * Handle a join group request for an existing member.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinExistingMember(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String memberId = request.memberId();
        String groupInstanceId = request.groupInstanceId();

        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means the group was recently removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else if (group.isPendingMember(memberId)) {
            // A rejoining pending member will be accepted. Note that pending member cannot be a static member.
            if (groupInstanceId != null) {
                throw new IllegalStateException("Received unexpected JoinGroup with groupInstanceId=" +
                    groupInstanceId + " for pending member with memberId=" + memberId);
            }

            log.info("Pending dynamic member with id {} joins group {} in {} state. Adding to the group now.",
                memberId, group.groupId(), group.currentState());

            return addMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                memberId,
                responseFuture
            );
        } else {
            try {
                group.validateMember(
                    memberId,
                    groupInstanceId,
                    "join-group"
                );
            } catch (KafkaException ex) {
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.forException(ex).code())
                    .setProtocolType(null)
                    .setProtocolName(null)
                );
                return EMPTY_RESULT;
            }

            ClassicGroupMember member = group.member(memberId);
            if (group.isInState(PREPARING_REBALANCE)) {
                return updateMemberThenRebalanceOrCompleteJoin(
                    request,
                    group,
                    member,
                    "Member " + member.memberId() + " is joining group during " + group.stateAsString() +
                        "; client reason: " + JoinGroupRequest.joinReason(request),
                    responseFuture
                );
            } else if (group.isInState(COMPLETING_REBALANCE)) {
                if (member.matches(request.protocols())) {
                    // Member is joining with the same metadata (which could be because it failed to
                    // receive the initial JoinGroup response), so just return current group information
                    // for the current generation.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(group.isLeader(memberId) ?
                            group.currentClassicGroupMembers() : Collections.emptyList())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                } else {
                    // Member has changed metadata, so force a rebalance
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                }
            } else if (group.isInState(STABLE)) {
                if (group.isLeader(memberId)) {
                    // Force a rebalance if the leader sends JoinGroup;
                    // This allows the leader to trigger rebalances for changes affecting assignment
                    // which do not affect the member metadata (such as topic metadata changes for the consumer)
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Leader " + memberId + " re-joining group during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else if (!member.matches(request.protocols())) {
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else {
                    // For followers with no actual change to their metadata, just return group information
                    // for the current generation which will allow them to issue SyncGroup.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(Collections.emptyList())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                }
            } else {
                // Group reached unexpected (Empty) state. Let the joining member reset their generation and rejoin.
                log.warn("Attempt to add rejoining member {} of group {} in unexpected group state {}",
                    memberId, group.groupId(), group.stateAsString());

                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );
            }
        }

        return EMPTY_RESULT;
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
    private void throwIfConsumerGroupMemberEpochIsInvalid(
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
     * @return true if the group is an empty consumer group.
     */
    private static boolean isEmptyConsumerGroup(Group group) {
        return group != null && group.type() == CONSUMER && group.isEmpty();
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

    /**
     * Handles a DeleteGroups request.
     * Populates the record list passed in with record to update the state machine.
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(RequestContext, List)} by
     * calling {@link GroupMetadataManager#validateDeleteGroup(String)}.
     *
     * @param groupId The id of the group to be deleted. It has been checked in {@link GroupMetadataManager#validateDeleteGroup}.
     * @param records The record list to populate.
     */
    public void createGroupTombstoneRecords(
        String groupId,
        List<CoordinatorRecord> records
    ) {
        // At this point, we have already validated the group id, so we know that the group exists and that no exception will be thrown.
        createGroupTombstoneRecords(groupStore.group(groupId), records);
    }

    /**
     * Handle a new dynamic member join. If the member id field is required, the group metadata manager
     * will add the new member id to the pending members and respond with MEMBER_ID_REQUIRED along with
     * the new member id for the client to join with.
     *
     * Otherwise, add the new member to the group and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> classicGroupJoinNewDynamicMember(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (JoinGroupRequest.requiresKnownMemberId(context.apiVersion())) {
            // If member id required, register the member in the pending member list and send
            // back a response to call for another join group request with allocated member id.
            log.info("Dynamic member with unknown member id joins group {} in {} state. " +
                    "Created a new member id {} and requesting the member to rejoin with this id.",
                group.groupId(), group.currentState(), newMemberId);

            group.addPendingMember(newMemberId);
            String classicGroupHeartbeatKey = GroupMetaManagerHelper.classicGroupHeartbeatKey(group.groupId(), newMemberId);

            timer.schedule(
                classicGroupHeartbeatKey,
                request.sessionTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> helper.expireClassicGroupMemberHeartbeat(group.groupId(), newMemberId)
            );

            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setErrorCode(Errors.MEMBER_ID_REQUIRED.code())
            );
        } else {
            log.info("Dynamic member with unknown member id joins group {} in state {}. " +
                    "Created a new member id {} and added the member to the group.",
                group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }

        return EMPTY_RESULT;
    }

    /**
     * Update a static member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group of the static member.
     * @param oldMemberId     The existing static member id.
     * @param newMemberId     The new joining static member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> updateStaticMemberThenRebalanceOrCompleteJoin(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String oldMemberId,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String currentLeader = group.leaderOrNull();
        ClassicGroupMember newMember = group.replaceStaticMember(request.groupInstanceId(), oldMemberId, newMemberId);

        // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
        // New heartbeat shall be scheduled with new member id.
        helper.rescheduleClassicGroupMemberHeartbeat(group, newMember);

        int oldRebalanceTimeoutMs = newMember.rebalanceTimeoutMs();
        int oldSessionTimeoutMs = newMember.sessionTimeoutMs();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection oldProtocols = newMember.supportedProtocols();

        group.updateMember(
            newMember,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        if (group.isInState(STABLE)) {
            // Check if group's selected protocol of next generation will change, if not, simply store group to persist
            // the updated static member, if yes, rebalance should be triggered to keep the group's assignment
            // and selected protocol consistent
            String groupInstanceId = request.groupInstanceId();
            String selectedProtocolForNextGeneration = group.selectProtocol();
            if (group.protocolName().orElse("").equals(selectedProtocolForNextGeneration)) {
                log.info("Static member which joins during Stable stage and doesn't affect " +
                    "the selected protocol will not trigger a rebalance.");

                Map<String, byte[]> groupAssignment = group.groupAssignment();
                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        log.warn("Failed to persist metadata for group {} static member {} with " +
                                "group instance id {} due to {}. Reverting to old member id {}.",
                            group.groupId(), newMemberId, groupInstanceId, t.getMessage(), oldMemberId);

                        // Failed to persist the member id of the given static member, revert the update of the static member in the group.
                        group.updateMember(newMember, oldProtocols, oldRebalanceTimeoutMs, oldSessionTimeoutMs, null);
                        ClassicGroupMember oldMember = group.replaceStaticMember(groupInstanceId, newMemberId, oldMemberId);
                        helper.rescheduleClassicGroupMemberHeartbeat(group, oldMember);

                        responseFuture.complete(
                            new JoinGroupResponseData()
                                .setMemberId(UNKNOWN_MEMBER_ID)
                                .setGenerationId(group.generationId())
                                .setProtocolName(group.protocolName().orElse(null))
                                .setProtocolType(group.protocolType().orElse(null))
                                .setLeader(currentLeader)
                                .setSkipAssignment(false)
                                .setErrorCode(GroupMetaManagerHelper.appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));

                    } else if (JoinGroupRequest.supportsSkippingAssignment(context.apiVersion())) {
                        boolean isLeader = group.isLeader(newMemberId);

                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMembers(isLeader ? group.currentClassicGroupMembers() : Collections.emptyList())
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(group.leaderOrNull())
                            .setSkipAssignment(isLeader)
                        );
                    } else {
                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(currentLeader)
                            .setSkipAssignment(false)
                        );
                    }
                });

                List<CoordinatorRecord> records = Collections.singletonList(
                    GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, groupAssignment, groupStore.image().features().metadataVersion())
                );

                return new CoordinatorResult<>(records, appendFuture, false);
            } else {
                return helper.maybePrepareRebalanceOrCompleteJoin(
                    group,
                    "Group's selectedProtocol will change because static member " +
                        newMember.memberId() + " with instance id " + groupInstanceId +
                        " joined with change of protocol; client reason: " + JoinGroupRequest.joinReason(request)
                );
            }
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            // if the group is in after-sync stage, upon getting a new join-group of a known static member
            // we should still trigger a new rebalance, since the old member may already be sent to the leader
            // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
            // with the new replaced member id. As a result the new member id would not get any assignment.
            return helper.prepareRebalance(
                group,
                "Updating metadata for static member " + newMember.memberId() + " with instance id " +
                    request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request)
            );
        } else if (group.isInState(EMPTY) || group.isInState(DEAD)) {
            throw new IllegalStateException("Group " + group.groupId() + " was not supposed to be in the state " +
                group.stateAsString() + " when the unknown static member " + request.groupInstanceId() + " rejoins.");

        }
        return helper.maybeCompleteJoinPhase(group);
    }

    /**
     * Add a member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param memberId        The member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> addMemberThenRebalanceOrCompleteJoin(
        RequestContext context,
        JoinGroupRequestData request,
        ClassicGroup group,
        String memberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            groupInstanceId,
            context.clientId(),
            context.clientAddress().toString(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            request.protocolType(),
            request.protocols()
        );

        member.setIsNew(true);

        // Update the newMemberAdded flag to indicate that the initial rebalance can be further delayed
        if (group.isInState(PREPARING_REBALANCE) && group.previousState() == EMPTY) {
            group.setNewMemberAdded(true);
        }

        group.add(member, responseFuture);

        // The session timeout does not affect new members since they do not have their memberId and
        // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
        // while the JoinGroup request is parked. If the client does disconnect (e.g. because of a request
        // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
        // members in the rebalance. To prevent this going on indefinitely, we time out JoinGroup requests
        // for new members. If the new member is still there, we expect it to retry.
        helper.rescheduleClassicGroupMemberHeartbeat(group, member, classicGroupNewMemberJoinTimeoutMs);

        return helper.maybePrepareRebalanceOrCompleteJoin(group, "Adding new member " + memberId + " with group instance id " +
            request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request));
    }

    /**
     * Update an existing member. Then begin a rebalance or complete the join phase.
     *
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param member          The member.
     * @param joinReason      The client reason for the join request.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, CoordinatorRecord> updateMemberThenRebalanceOrCompleteJoin(
        JoinGroupRequestData request,
        ClassicGroup group,
        ClassicGroupMember member,
        String joinReason,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        group.updateMember(
            member,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        return helper.maybePrepareRebalanceOrCompleteJoin(group, joinReason);
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
    private boolean isSubset(
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

    public static String consumerGroupRebalanceTimeoutKey(String groupId, String memberId) {
        return "rebalance-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Validates if the consumer group member uses the classic protocol.
     *
     * @param member The ConsumerGroupMember.
     */
    private void throwIfMemberDoesNotUseClassicProtocol(ConsumerGroupMember member) {
        if (!member.useClassicProtocol()) {
            throw new UnknownMemberIdException(
                String.format("Member %s does not use the classic protocol.", member.memberId())
            );
        }
    }

    /**
     * Validates if the generation id from the request matches the member epoch.
     *
     * @param memberId              The member id.
     * @param memberEpoch           The member epoch.
     * @param requestGenerationId   The generation id from the request.
     */
    private void throwIfGenerationIdUnmatched(
        String memberId,
        int memberEpoch,
        int requestGenerationId
    ) {
        if (memberEpoch != requestGenerationId) {
            throw Errors.ILLEGAL_GENERATION.exception(
                String.format("The request generation id %s is not equal to the member epoch %d of member %s.",
                    requestGenerationId, memberEpoch, memberId)
            );
        }
    }

    /**
     * Validates if the protocol type and the protocol name from the request matches those of the consumer group.
     *
     * @param member                The ConsumerGroupMember.
     * @param requestProtocolType   The protocol type from the request.
     * @param requestProtocolName   The protocol name from the request.
     */
    private void throwIfClassicProtocolUnmatched(
        ConsumerGroupMember member,
        String requestProtocolType,
        String requestProtocolName
    ) {
        String protocolName = member.supportedClassicProtocols().get().iterator().next().name();
        if (requestProtocolType != null && !ConsumerProtocol.PROTOCOL_TYPE.equals(requestProtocolType)) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception(
                String.format("The protocol type %s from member %s request is not equal to the group protocol type %s.",
                    requestProtocolType, member.memberId(), ConsumerProtocol.PROTOCOL_TYPE)
            );
        } else if (requestProtocolName != null && !protocolName.equals(requestProtocolName)) {
            throw Errors.INCONSISTENT_GROUP_PROTOCOL.exception(
                String.format("The protocol name %s from member %s request is not equal to the protocol name %s returned in the join response.",
                    requestProtocolName, member.memberId(), protocolName)
            );
        }
    }

    /**
     * Validates if a new rebalance has been triggered and the member should rejoin to catch up.
     *
     * @param group     The ConsumerGroup.
     * @param member    The ConsumerGroupMember.
     */
    private void throwIfRebalanceInProgress(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        // If the group epoch is greater than the member epoch, there is a new rebalance triggered and the member
        // needs to rejoin to catch up. However, if the member is in UNREVOKED_PARTITIONS state, it means the
        // member has already rejoined, so it needs to first finish revoking the partitions and the reconciliation,
        // and then the next rejoin will be triggered automatically if needed.
        if (group.groupEpoch() > member.memberEpoch() && !member.state().equals(MemberState.UNREVOKED_PARTITIONS)) {
            scheduleConsumerGroupJoinTimeoutIfAbsent(group.groupId(), member.memberId(), member.rebalanceTimeoutMs());
            throw Errors.REBALANCE_IN_PROGRESS.exception(
                String.format("A new rebalance is triggered in group %s and member %s should rejoin to catch up.",
                    group.groupId(), member.memberId())
            );
        }
    }

    /**
     * Schedules a join timeout for the member if there's not a join timeout.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param rebalanceTimeoutMs    The rebalance timeout.
     */
    private void scheduleConsumerGroupJoinTimeoutIfAbsent(
        String groupId,
        String memberId,
        int rebalanceTimeoutMs
    ) {
        timer.scheduleIfAbsent(
            GroupMetaManagerHelper.consumerGroupJoinKey(groupId, memberId),
            rebalanceTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,
            () -> consumerGroupFenceMemberOperation(groupId, memberId, "the classic member failed to join within the rebalance timeout")
        );
    }

    /**
     * Serializes the member's assigned partitions with ConsumerProtocol.
     *
     * @param member The ConsumerGroupMember.
     * @return The serialized assigned partitions.
     */
    private byte[] prepareAssignment(ConsumerGroupMember member) {
        try {
            return ConsumerProtocol.serializeAssignment(
                toConsumerProtocolAssignment(
                    member.assignedPartitions(),
                    groupStore.image().topics()
                ),
                ConsumerProtocol.deserializeVersion(
                    ByteBuffer.wrap(member.classicMemberMetadata().get().supportedProtocols().iterator().next().metadata())
                )
            ).array();
        } catch (SchemaException e) {
            throw new IllegalStateException("Malformed embedded consumer protocol in version deserialization.");
        }
    }

    private Optional<Errors> validateSyncGroup(
        ClassicGroup group,
        SyncGroupRequestData request
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            return Optional.of(COORDINATOR_NOT_AVAILABLE);
        } else {
            try {
                group.validateMember(
                    request.memberId(),
                    request.groupInstanceId(),
                    "sync-group"
                );
            } catch (KafkaException ex) {
                return Optional.of(Errors.forException(ex));
            }

            if (request.generationId() != group.generationId()) {
                return Optional.of(Errors.ILLEGAL_GENERATION);
            } else if (isProtocolInconsistent(request.protocolType(), group.protocolType().orElse(null)) ||
                isProtocolInconsistent(request.protocolName(), group.protocolName().orElse(null))) {
                return Optional.of(Errors.INCONSISTENT_GROUP_PROTOCOL);
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Checks whether the given protocol type or name in the request is inconsistent with the group's.
     *
     * @param protocolTypeOrName       The request's protocol type or name.
     * @param groupProtocolTypeOrName  The group's protoocl type or name.
     *
     * @return  True if protocol is inconsistent, false otherwise.
     */
    private boolean isProtocolInconsistent(
        String protocolTypeOrName,
        String groupProtocolTypeOrName
    ) {
        return protocolTypeOrName != null
            && groupProtocolTypeOrName != null
            && !groupProtocolTypeOrName.equals(protocolTypeOrName);
    }

    private void removePendingSyncMember(
        ClassicGroup group,
        String memberId
    ) {
        group.removePendingSyncMember(memberId);
        String syncKey = GroupMetaManagerHelper.classicGroupSyncKey(group.groupId());
        switch (group.currentState()) {
            case DEAD:
            case EMPTY:
            case PREPARING_REBALANCE:
                timer.cancel(syncKey);
                break;
            case COMPLETING_REBALANCE:
            case STABLE:
                if (group.hasReceivedSyncFromAllMembers()) {
                    timer.cancel(syncKey);
                }
                break;
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Sets assignment for group and propagate assignment and error to all members.
     *
     * @param group      The group.
     * @param assignment The assignment for all members.
     */
    private void setAndPropagateAssignment(ClassicGroup group, Map<String, byte[]> assignment) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("The group must be in CompletingRebalance state " +
                "to set and propagate assignment.");
        }

        group.allMembers().forEach(member ->
            member.setAssignment(assignment.getOrDefault(member.memberId(), EMPTY_ASSIGNMENT)));

        helper.propagateAssignment(group, Errors.NONE);
    }

    /**
     * Replays GroupMetadataKey/Value to update the soft state of
     * the classic group.
     *
     * @param key   A GroupMetadataKey key.
     * @param value A GroupMetadataValue record.
     */
    public void replay(
        GroupMetadataKey key,
        GroupMetadataValue value
    ) {
        String groupId = key.group();

        if (value == null)  {
            // Tombstone. Group should be removed.
            removeClassicGroup(groupId);
        } else {
            List<ClassicGroupMember> loadedMembers = new ArrayList<>();
            for (GroupMetadataValue.MemberMetadata member : value.members()) {
                int rebalanceTimeout = member.rebalanceTimeout() == -1 ?
                    member.sessionTimeout() : member.rebalanceTimeout();

                JoinGroupRequestData.JoinGroupRequestProtocolCollection supportedProtocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
                supportedProtocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(value.protocol())
                    .setMetadata(member.subscription()));

                ClassicGroupMember loadedMember = new ClassicGroupMember(
                    member.memberId(),
                    Optional.ofNullable(member.groupInstanceId()),
                    member.clientId(),
                    member.clientHost(),
                    rebalanceTimeout,
                    member.sessionTimeout(),
                    value.protocolType(),
                    supportedProtocols,
                    member.assignment()
                );

                loadedMembers.add(loadedMember);
            }

            String protocolType = value.protocolType();

            ClassicGroup classicGroup = new ClassicGroup(
                this.logContext,
                groupId,
                loadedMembers.isEmpty() ? EMPTY : STABLE,
                time,
                value.generation(),
                protocolType == null || protocolType.isEmpty() ? Optional.empty() : Optional.of(protocolType),
                Optional.ofNullable(value.protocol()),
                Optional.ofNullable(value.leader()),
                value.currentStateTimestamp() == -1 ? Optional.empty() : Optional.of(value.currentStateTimestamp())
            );

            loadedMembers.forEach(member -> classicGroup.add(member, null));
            groupStore.addGroup(groupId, classicGroup);

            classicGroup.setSubscribedTopics(
                classicGroup.computeSubscribedTopics()
            );
        }
    }

    /**
     * Counts and updates the number of classic groups in different states.
     */
    public void updateClassicGroupSizeCounter() {
        Map<ClassicGroupState, Long> groupSizeCounter = new HashMap<>();
        List<ListGroupsResponseData.ListedGroup> groups = groupStore.listGroups(Collections.emptySet(), Collections.singleton(GroupType.CLASSIC.toString()), 0);
        groups.forEach((group) -> {
            ClassicGroup classicGroup = (ClassicGroup) groupStore.group(group.groupId());
            groupSizeCounter.compute(classicGroup.currentState(), Utils::incValue);
        });
        metrics.setClassicGroupGauges(groupSizeCounter);
    }

    /**
     * Handle a classic LeaveGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the records to append.
     */
    public CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeave(
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        Group group;
        try {
            group = groupStore.group(request.groupId());
        } catch (GroupIdNotFoundException e) {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }

        if (group.type() == CLASSIC) {
            return classicGroupLeaveToClassicGroup((ClassicGroup) group, context, request);
        } else if (group.type() == CONSUMER) {
            return classicGroupLeaveToConsumerGroup((ConsumerGroup) group, context, request);
        } else {
            throw new UnknownMemberIdException(String.format("Group %s not found.", request.groupId()));
        }
    }

    /**
     * Handle a classic LeaveGroupRequest to a ConsumerGroup.
     *
     * @param group          The ConsumerGroup.
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the records to append.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        String groupId = group.groupId();
        List<LeaveGroupResponseData.MemberResponse> memberResponses = new ArrayList<>();
        Set<ConsumerGroupMember> validLeaveGroupMembers = new HashSet<>();
        List<CoordinatorRecord> records = new ArrayList<>();

        for (LeaveGroupRequestData.MemberIdentity memberIdentity : request.members()) {
            String memberId = memberIdentity.memberId();
            String instanceId = memberIdentity.groupInstanceId();
            String reason = memberIdentity.reason() != null ? memberIdentity.reason() : "not provided";

            ConsumerGroupMember member;
            try {
                if (instanceId == null) {
                    member = group.getOrMaybeCreateMember(memberId, false);
                    throwIfMemberDoesNotUseClassicProtocol(member);

                    log.info("[Group {}] Dynamic member {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberId, reason);
                } else {
                    member = group.staticMember(instanceId);
                    throwIfStaticMemberIsUnknown(member, instanceId);
                    // The LeaveGroup API allows administrative removal of members by GroupInstanceId
                    // in which case we expect the MemberId to be undefined.
                    if (!UNKNOWN_MEMBER_ID.equals(memberId)) {
                        throwIfInstanceIdIsFenced(member, groupId, memberId, instanceId);
                    }
                    throwIfMemberDoesNotUseClassicProtocol(member);

                    memberId = member.memberId();
                    log.info("[Group {}] Static member {} with instance id {} has left group " +
                            "through explicit `LeaveGroup` request; client reason: {}",
                        groupId, memberId, instanceId, reason);
                }

                GroupMetaManagerHelper.removeMember(records, groupId, memberId);
                helper.cancelTimers(groupId, memberId);
                memberResponses.add(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId)
                        .setGroupInstanceId(instanceId)
                );
                validLeaveGroupMembers.add(member);
            } catch (KafkaException e) {
                memberResponses.add(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId)
                        .setGroupInstanceId(instanceId)
                        .setErrorCode(Errors.forException(e).code())
                );
            }
        }

        if (!records.isEmpty()) {
            // Maybe update the subscription metadata.
            Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
                group.computeSubscribedTopicNames(validLeaveGroupMembers),
                groupStore.image().topics(),
                groupStore.image().cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    group.groupId(), subscriptionMetadata);
                records.add(newConsumerGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
            }

            // Bump the group epoch.
            records.add(newConsumerGroupEpochRecord(groupId, group.groupEpoch() + 1));
        }

        return new CoordinatorResult<>(records, new LeaveGroupResponseData().setMembers(memberResponses));
    }

    /**
     * Handle a classic LeaveGroupRequest to a ClassicGroup.
     *
     * @param group          The ClassicGroup.
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the GroupMetadata record to append if the group
     *         no longer has any members.
     */
    private CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> classicGroupLeaveToClassicGroup(
        ClassicGroup group,
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException {
        if (group.isInState(DEAD)) {
            return new CoordinatorResult<>(
                Collections.emptyList(),
                new LeaveGroupResponseData()
                    .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        List<LeaveGroupResponseData.MemberResponse> memberResponses = new ArrayList<>();

        for (LeaveGroupRequestData.MemberIdentity member: request.members()) {
            String reason = member.reason() != null ? member.reason() : "not provided";
            // The LeaveGroup API allows administrative removal of members by GroupInstanceId
            // in which case we expect the MemberId to be undefined.
            if (UNKNOWN_MEMBER_ID.equals(member.memberId())) {
                if (member.groupInstanceId() != null && group.hasStaticMember(member.groupInstanceId())) {
                    removeCurrentMemberFromClassicGroup(
                        group,
                        group.staticMemberId(member.groupInstanceId()),
                        reason
                    );
                    memberResponses.add(
                        new LeaveGroupResponseData.MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } else {
                    memberResponses.add(
                        new LeaveGroupResponseData.MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                    );
                }
            } else if (group.isPendingMember(member.memberId())) {
                group.remove(member.memberId());
                timer.cancel(GroupMetaManagerHelper.classicGroupHeartbeatKey(group.groupId(), member.memberId()));
                log.info("[Group {}] Pending member {} has left group through explicit `LeaveGroup` request; client reason: {}",
                    group.groupId(), member.memberId(), reason);

                memberResponses.add(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(member.memberId())
                        .setGroupInstanceId(member.groupInstanceId())
                );
            } else {
                try {
                    group.validateMember(member.memberId(), member.groupInstanceId(), "leave-group");
                    removeCurrentMemberFromClassicGroup(
                        group,
                        member.memberId(),
                        reason
                    );
                    memberResponses.add(
                        new LeaveGroupResponseData.MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } catch (KafkaException e) {
                    memberResponses.add(
                        new LeaveGroupResponseData.MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.forException(e).code())
                    );
                }
            }
        }

        List<String> validLeaveGroupMembers = memberResponses.stream()
            .filter(response -> response.errorCode() == Errors.NONE.code())
            .map(LeaveGroupResponseData.MemberResponse::memberId)
            .collect(Collectors.toList());

        String reason = "explicit `LeaveGroup` request for (" + String.join(", ", validLeaveGroupMembers) + ") members.";
        CoordinatorResult<Void, CoordinatorRecord> coordinatorResult = EMPTY_RESULT;

        if (!validLeaveGroupMembers.isEmpty()) {
            switch (group.currentState()) {
                case STABLE:
                case COMPLETING_REBALANCE:
                    coordinatorResult = helper.maybePrepareRebalanceOrCompleteJoin(group, reason);
                    break;
                case PREPARING_REBALANCE:
                    coordinatorResult = helper.maybeCompleteJoinPhase(group);
                    break;
                default:
            }
        }

        return new CoordinatorResult<>(
            coordinatorResult.records(),
            new LeaveGroupResponseData()
                .setMembers(memberResponses),
            coordinatorResult.appendFuture(),
            coordinatorResult.replayRecords()
        );
    }

    /**
     * Remove a member from the group. Cancel member's heartbeat, and prepare rebalance
     * or complete the join phase if necessary.
     *
     * @param group     The classic group.
     * @param memberId  The member id.
     * @param reason    The reason for the LeaveGroup request.
     *
     */
    private void removeCurrentMemberFromClassicGroup(
        ClassicGroup group,
        String memberId,
        String reason
    ) {
        ClassicGroupMember member = group.member(memberId);
        timer.cancel(GroupMetadataManager.classicGroupHeartbeatKey(group.groupId(), memberId));
        log.info("[Group {}] Member {} has left group through explicit `LeaveGroup` request; client reason: {}",
            group.groupId(), memberId, reason);

        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
        // will retry the JoinGroup request if is still active.
        group.completeJoinFuture(
            member,
            new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());
    }
}
