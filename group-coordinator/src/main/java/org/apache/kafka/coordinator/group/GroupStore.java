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

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.SHARE;

public class GroupStore {

    /**
     * The snapshot registry.
     */
    private SnapshotRegistry snapshotRegistry;

    /**
     * The classic and consumer groups keyed by their name.
     */
    private final TimelineHashMap<String, Group> groups;

    /**
     * The group ids keyed by topic names.
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> groupsByTopics;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    public GroupStore(SnapshotRegistry snapshotRegistry, MetadataImage metadataImage) {
        this.snapshotRegistry = snapshotRegistry;
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupsByTopics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataImage = metadataImage;
    }

    /**
     * @return The current metadata image used by the group metadata manager.
     */
    public MetadataImage image() {
        return metadataImage;
    }

    /**
     * Returns the snapshot registry.
     *
     * @return The snapshot registry.
     */
    public SnapshotRegistry snapshotRegistry() {
        return snapshotRegistry;
    }

    /**
     * Get the Group List.
     *
     * @param statesFilter      The states of the groups we want to list.
     *                          If empty, all groups are returned with their state.
     *                          If invalid, no groups are returned.
     * @param typesFilter       The types of the groups we want to list.
     *                          If empty, all groups are returned with their type.
     *                          If invalid, no groups are returned.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the ListGroupsResponseData.ListedGroup
     */
    public List<ListGroupsResponseData.ListedGroup> listGroups(
        Set<String> statesFilter,
        Set<String> typesFilter,
        long committedOffset
    ) {
        // Converts each state filter string to lower case for a case-insensitive comparison.
        Set<String> caseInsensitiveFilterSet = statesFilter.stream()
            .map(String::toLowerCase)
            .map(String::trim)
            .collect(Collectors.toSet());

        // Converts each type filter string to a value in the GroupType enum while being case-insensitive.
        Set<Group.GroupType> enumTypesFilter = typesFilter.stream()
            .map(Group.GroupType::parse)
            .collect(Collectors.toSet());

        Predicate<Group> combinedFilter = group -> {
            boolean stateCheck = statesFilter.isEmpty() || group.isInStates(caseInsensitiveFilterSet, committedOffset);
            boolean typeCheck = enumTypesFilter.isEmpty() || enumTypesFilter.contains(group.type());

            return stateCheck && typeCheck;
        };

        Stream<Group> groupStream = groups.values(committedOffset).stream();

        return groupStream
            .filter(combinedFilter)
            .map(group -> group.asListedGroup(committedOffset))
            .collect(Collectors.toList());
    }

    /**
     * @return The group corresponding to the group id or throw GroupIdNotFoundException.
     */
    public Group group(String groupId) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The group corresponding to the group id at the given committed offset
     *         or throw GroupIdNotFoundException.
     */
    public Group group(String groupId, long committedOffset) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, committedOffset);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The set of groups subscribed to the topic.
     */
    public Set<String> groupsSubscribedToTopic(String topicName) {
        Set<String> groups = groupsByTopics.get(topicName);
        return groups != null ? groups : Collections.emptySet();
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;

        // Notify all the groups subscribed to the created, updated or
        // deleted topics.
        Optional.ofNullable(delta.topicsDelta()).ifPresent(topicsDelta -> {
            Set<String> allGroupIds = new HashSet<>();
            topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
                String topicName = topicDelta.name();
                allGroupIds.addAll(groupsSubscribedToTopic(topicName));
            });
            topicsDelta.deletedTopicIds().forEach(topicId -> {
                TopicImage topicImage = delta.image().topics().getTopic(topicId);
                allGroupIds.addAll(groupsSubscribedToTopic(topicImage.name()));
            });
            allGroupIds.forEach(groupId -> {
                Group group = groups.get(groupId);
                if (group != null && (group.type() == CONSUMER || group.type() == SHARE)) {
                    ((ModernGroup<?>) group).requestMetadataRefresh();
                }
            });
        });
    }

    /**
     * Delete the group if it exists and is in Empty state.
     *
     * @param groupId The group id.
     * @param records The list of records to append the group metadata tombstone records.
     */
    public void maybeDeleteGroup(String groupId, List<CoordinatorRecord> records) {
        Group group = groups.get(groupId);
        if (group != null && group.isEmpty()) {
            createGroupTombstoneRecords(groupId, records);
        }
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
        createGroupTombstoneRecords(group(groupId), records);
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
     * @return The set of all groups' ids.
     */
    public Set<String> groupIds() {
        return Collections.unmodifiableSet(this.groups.keySet());
    }

    /**
     * Adds a new group to the group store.
     *
     * @param groupId The groupId of the group to be added.
     * @param group The group to be added.
     */
    public void addGroup(String groupId, Group group) {
        groups.put(groupId, group);
    }

    /**
     * Subscribes a group to a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    public void subscribeGroupToTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics
            .computeIfAbsent(topicName, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
            .add(groupId);
    }

    /**
     * Unsubscribes a group from a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    public void unsubscribeGroupFromTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics.computeIfPresent(topicName, (__, groupIds) -> {
            groupIds.remove(groupId);
            return groupIds.isEmpty() ? null : groupIds;
        });
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     *
     * @return The type of the removed group.
     */
    public Group removeGroup(
        String groupId
    ) {
        return groups.remove(groupId);
    }

    /**
     * Validates the DeleteGroups request.
     *
     * @param groupId The id of the group to be deleted.
     */
    void validateDeleteGroup(String groupId) throws ApiException {
        Group group = group(groupId);
        group.validateDeleteGroup();
    }
}
