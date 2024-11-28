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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class GroupStoreTest {
    private GroupStore groupStore;
    private long lastCommittedOffset;
    private long lastWrittenOffset;

    @BeforeEach
    public void setUp() {
        lastWrittenOffset = 0;
        lastCommittedOffset = 0;
        groupStore = new GroupStore(new SnapshotRegistry(new LogContext()), MetadataImage.EMPTY);
        groupStore.snapshotRegistry().idempotentCreateSnapshot(lastWrittenOffset);
    }

    @Test
    public void testListGroups() {
        String consumerGroupId = "consumer-group-id";
        String classicGroupId = "classic-group-id";
        String shareGroupId = "share-group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";

        setUp(new ConsumerGroupBuilder(consumerGroupId, 10));

        // Create one classic group record.
        replay(newGroupMetadataRecord(
            classicGroupId,
            new GroupMetadataValue()
                .setMembers(Collections.emptyList())
                .setGeneration(2)
                .setLeader(null)
                .setProtocolType("classic")
                .setProtocol("range")
                .setCurrentStateTimestamp(System.currentTimeMillis()),
            MetadataVersion.latestTesting()));
        // Create one share group record.
        replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(shareGroupId, 6));
        commit();
        // Create consumer group records.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .build()));
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, 11));

        // Test list group response without a group state or group type filter.
        Map<String, ListGroupsResponseData.ListedGroup> actualAllGroupMap = groupStore.listGroups(Collections.emptySet(), Collections.emptySet(), lastCommittedOffset)
            .stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        Map<String, ListGroupsResponseData.ListedGroup> expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(classicGroupId)
                    .setProtocolType("classic")
                    .setGroupState(EMPTY.toString())
                    .setGroupType(Group.GroupType.CLASSIC.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(shareGroupId)
                    .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                    .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.SHARE.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group with case-insensitive ‘empty’.
        actualAllGroupMap = groupStore.listGroups(Collections.singleton("empty"), Collections.emptySet(), lastCommittedOffset)
            .stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        commit();

        // Test list group response to check assigning state in the consumer group.
        actualAllGroupMap = groupStore.listGroups(Collections.singleton("assigning"), Collections.emptySet(), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with group state filter and no group type filter.
        actualAllGroupMap = groupStore.listGroups(Collections.singleton("Empty"), Collections.emptySet(), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroupId)
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter.
        actualAllGroupMap = groupStore.listGroups(Collections.emptySet(), Collections.singleton(Group.GroupType.CLASSIC.toString()), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroupId)
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter in a different case.
        actualAllGroupMap = groupStore.listGroups(Collections.emptySet(), Collections.singleton("Consumer"), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = groupStore.listGroups(Collections.emptySet(), Collections.singleton("Share"), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = groupStore.listGroups(Set.of("empty", "Assigning"), Collections.emptySet(), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroupId)
                .setProtocolType(Group.GroupType.CLASSIC.toString())
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with invalid group type filter .
        actualAllGroupMap = groupStore.listGroups(Collections.emptySet(), Collections.singleton("Invalid"), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Collections.emptyMap();

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with invalid group state filter and with no group type filter .
        actualAllGroupMap = groupStore.listGroups(Collections.singleton("Invalid"), Collections.emptySet(), lastCommittedOffset).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Collections.emptyMap();

        assertEquals(expectAllGroupMap, actualAllGroupMap);
    }

    @Test
    public void testGroupIdsByTopics() {
        String groupId1 = "group1";
        String groupId2 = "group2";

        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("zar"));

        // M1 in group 1 subscribes to foo and bar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to foo, bar and zar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId2), groupStore.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to bar and zar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(List.of("bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo and bar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("zar"));

        // M1 in group 1 is removed.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId1, "group1-m1"));
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId1, "group1-m1"));

        assertEquals(Set.of(groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Set.of(groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .build()));

        assertEquals(Set.of(groupId2), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupStore.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupStore.groupsSubscribedToTopic("zar"));
    }

    @Test
    public void testOnNewMetadataImageWithEmptyDelta() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        groupStore.onNewMetadataImage(image, delta);
        assertEquals(image, groupStore.image());
    }

    @Test
    public void testOnNewMetadataImage() {
        // M1 in group 1 subscribes to a and b.
        CoordinatorRecord record1 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group1",
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("a", "b"))
                .build());
        replay((ConsumerGroupMemberMetadataKey) record1.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record1.value()));

        // M1 in group 2 subscribes to b and c.
        CoordinatorRecord record2 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group2",
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("b", "c"))
                .build());
        replay((ConsumerGroupMemberMetadataKey) record2.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record2.value()));

        // M1 in group 3 subscribes to d.
        CoordinatorRecord record3 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group3",
            new ConsumerGroupMember.Builder("group3-m1")
                .setSubscribedTopicNames(Collections.singletonList("d"))
                .build());
        replay((ConsumerGroupMemberMetadataKey) record3.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record3.value()));

        // M1 in group 4 subscribes to e.
        CoordinatorRecord record4 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group4",
            new ConsumerGroupMember.Builder("group4-m1")
                .setSubscribedTopicNames(Collections.singletonList("e"))
                .build());
        replay((ConsumerGroupMemberMetadataKey) record4.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record4.value()));

        // M1 in group 5 subscribes to f.
        CoordinatorRecord record5 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group5",
            new ConsumerGroupMember.Builder("group5-m1")
                .setSubscribedTopicNames(Collections.singletonList("f"))
                .build());
        replay((ConsumerGroupMemberMetadataKey) record5.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record5.value()));

        // Ensures that all refresh flags are set to the future.
        List.of("group1", "group2", "group3", "group4", "group5").forEach(groupId -> {
            ConsumerGroup group = consumerGroup(groupId);
            group.setMetadataRefreshDeadline(System.currentTimeMillis() + 5000L, 0);
            assertFalse(group.hasMetadataExpired(System.currentTimeMillis()));
        });

        // Update the metadata image.
        Uuid topicA = Uuid.randomUuid();
        Uuid topicB = Uuid.randomUuid();
        Uuid topicC = Uuid.randomUuid();
        Uuid topicD = Uuid.randomUuid();
        Uuid topicE = Uuid.randomUuid();

        // Create a first base image with topic a, b, c and d.
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicA).setName("a"));
        delta.replay(new PartitionRecord().setTopicId(topicA).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicB).setName("b"));
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicC).setName("c"));
        delta.replay(new PartitionRecord().setTopicId(topicC).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicD).setName("d"));
        delta.replay(new PartitionRecord().setTopicId(topicD).setPartitionId(0));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        // Create a delta which updates topic B, deletes topic D and creates topic E.
        delta = new MetadataDelta(image);
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(2));
        delta.replay(new RemoveTopicRecord().setTopicId(topicD));
        delta.replay(new TopicRecord().setTopicId(topicE).setName("e"));
        delta.replay(new PartitionRecord().setTopicId(topicE).setPartitionId(1));
        image = delta.apply(MetadataProvenance.EMPTY);

        // Update metadata image with the delta.
        groupStore.onNewMetadataImage(image, delta);

        // Verify the groups.
        List.of("group1", "group2", "group3", "group4").forEach(groupId -> {
            ConsumerGroup group = consumerGroup(groupId);
            assertTrue(group.hasMetadataExpired(System.currentTimeMillis()));
        });

        Collections.singletonList("group5").forEach(groupId -> {
            ConsumerGroup group = consumerGroup(groupId);
            assertFalse(group.hasMetadataExpired(System.currentTimeMillis()));
        });

        // Verify image.
        assertEquals(image, groupStore.image());
    }

    @Test
    public void testClassicGroupMaybeDelete() {
        ClassicGroup group = getOrMaybeCreateClassicGroup("group-id");

        List<CoordinatorRecord> expectedRecords = Collections.singletonList(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id"));
        List<CoordinatorRecord> records = new ArrayList<>();
        groupStore.maybeDeleteGroup("group-id", records);
        assertEquals(expectedRecords, records);

        records = new ArrayList<>();
        group.transitionTo(PREPARING_REBALANCE);
        groupStore.maybeDeleteGroup("group-id", records);
        assertEquals(Collections.emptyList(), records);

        records = new ArrayList<>();
        groupStore.maybeDeleteGroup("invalid-group-id", records);
        assertEquals(Collections.emptyList(), records);
    }

    @Test
    public void testConsumerGroupMaybeDelete() {
        String groupId = "group-id";
        setUp(new ConsumerGroupBuilder(groupId, 10));
        commit();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        groupStore.maybeDeleteGroup(groupId, records);
        assertEquals(expectedRecords, records);

        records = new ArrayList<>();
        CoordinatorRecord record1 = GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build());
        replay((ConsumerGroupMemberMetadataKey) record1.key().message(), (ConsumerGroupMemberMetadataValue) messageOrNull(record1.value()));
        groupStore.maybeDeleteGroup(groupId, records);
        assertEquals(Collections.emptyList(), records);
    }

    private void setUp(ConsumerGroupBuilder consumerGroupBuilder) {
        List<CoordinatorRecord> records = consumerGroupBuilder.build(groupStore.image().topics());
        for (CoordinatorRecord record : records) {
            replay(record);
        }
        commit();
    }

    private void commit() {
        long lastCommittedOffset = this.lastCommittedOffset;
        this.lastCommittedOffset = lastWrittenOffset;
        groupStore.snapshotRegistry().deleteSnapshotsUpTo(lastCommittedOffset);
    }

    private void replay(
        CoordinatorRecord record) {
        ApiMessageAndVersion key = record.key();
        ApiMessageAndVersion value = record.value();

        if (key == null) {
            throw new IllegalStateException("Received a null key in " + record);
        }

        switch (key.version()) {
            case GroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (GroupMetadataKey) key.message(),
                    (GroupMetadataValue) messageOrNull(value)
                );
                break;

            case ConsumerGroupMemberMetadataKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (ConsumerGroupMemberMetadataKey) key.message(),
                    (ConsumerGroupMemberMetadataValue) messageOrNull(value)
                );
                break;

            case ConsumerGroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (ConsumerGroupMetadataKey) key.message(),
                    (ConsumerGroupMetadataValue) messageOrNull(value)
                );
                break;

            case ConsumerGroupTargetAssignmentMetadataKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (ConsumerGroupTargetAssignmentMetadataKey) key.message(),
                    (ConsumerGroupTargetAssignmentMetadataValue) messageOrNull(value)
                );
                break;

            case ShareGroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (ShareGroupMetadataKey) key.message(),
                    (ShareGroupMetadataValue) messageOrNull(value)
                );
                break;

            case ConsumerGroupCurrentMemberAssignmentKey.HIGHEST_SUPPORTED_VERSION:
                replay(
                    (ConsumerGroupCurrentMemberAssignmentKey) key.message(),
                    (ConsumerGroupCurrentMemberAssignmentValue) messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + key.version()
                    + " in " + record);
        }
        lastWrittenOffset++;
        groupStore.snapshotRegistry().idempotentCreateSnapshot(lastWrittenOffset);
    }

    private void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value) {

        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup;

        try {
            consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, value != null);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist and a tombstone is replayed, we can ignore it.
            return;
        }

        assert consumerGroup != null;
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

    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        ConsumerGroup consumerGroup = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
        assert consumerGroup != null;
        consumerGroup.setGroupEpoch(value.epoch());
    }

    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroup group;

        try {
            group = getOrMaybeCreatePersistedConsumerGroup(groupId, false);
        } catch (GroupIdNotFoundException ex) {
            // If the group does not exist, we can ignore the tombstone.
            return;
        }
        ConsumerGroupMember oldMember;
        try {
            assert group != null;
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

    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();


        ConsumerGroup group = getOrMaybeCreatePersistedConsumerGroup(groupId, true);
        assert group != null;
        group.setTargetAssignmentEpoch(value.assignmentEpoch());
    }

    private void replay(
        ShareGroupMetadataKey key,
        ShareGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        ShareGroup shareGroup = createPersistedShareGroup(groupStore, groupId);
        shareGroup.setGroupEpoch(value.epoch());
    }

    private void replay(
        GroupMetadataKey key,
        GroupMetadataValue value
    ) {
        String groupId = key.group();
        Time time = Time.SYSTEM;


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
            mock(LogContext.class),
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

    private ConsumerGroup getOrMaybeCreatePersistedConsumerGroup(String groupId, boolean createIfNotExist) {
        Group group;
        try {
            group = groupStore.group(groupId);
        } catch (GroupIdNotFoundException e) {
            if (createIfNotExist) {
                ConsumerGroup consumerGroup = new ConsumerGroup(groupStore.snapshotRegistry(), groupId, mock(GroupCoordinatorMetricsShard.class));
                groupStore.addGroup(groupId, consumerGroup);
                return consumerGroup;
            }
            return null;
        }
        return (ConsumerGroup) group;
    }

    ClassicGroup getOrMaybeCreateClassicGroup(
        String groupId
    ) {
        Group group;
        try {
            group = groupStore.group(groupId);
        } catch (GroupIdNotFoundException e) {
            ClassicGroup classicGroup = new ClassicGroup(mock(LogContext.class), groupId, ClassicGroupState.EMPTY, Time.SYSTEM);
            groupStore.addGroup(groupId, classicGroup);
            return classicGroup;
        }
        return (ClassicGroup) group;
    }

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

    private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    private ConsumerGroup consumerGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        Group group = groupStore.group(groupId, Long.MAX_VALUE);

        if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
        }
    }

    private ShareGroup createPersistedShareGroup(GroupStore groupStore, String groupId) {
        ShareGroup shareGroup = new ShareGroup(groupStore.snapshotRegistry(), groupId);
        groupStore.addGroup(groupId, shareGroup);
        return shareGroup;
    }

    private static CoordinatorRecord newGroupMetadataRecord(
        String groupId,
        GroupMetadataValue value,
        MetadataVersion metadataVersion
    ) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new GroupMetadataKey()
                    .setGroup(groupId),
                (short) 2
            ),
            new ApiMessageAndVersion(
                value,
                metadataVersion.groupMetadataValueVersion()
            )
        );
    }
}