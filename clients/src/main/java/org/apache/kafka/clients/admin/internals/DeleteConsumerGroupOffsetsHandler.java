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
package org.apache.kafka.clients.admin.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class DeleteConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, Errors>> {

    private final CoordinatorKey groupId;
    private final Set<TopicPartition> partitions;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public DeleteConsumerGroupOffsetsHandler(
        String groupId,
        Set<TopicPartition> partitions,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.partitions = partitions;
        this.log = logContext.logger(DeleteConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "offsetDelete";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> newFuture(
            String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    @Override
    public OffsetDeleteRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        final OffsetDeleteRequestTopicCollection topics = new OffsetDeleteRequestTopicCollection();
        partitions.stream().collect(Collectors.groupingBy(TopicPartition::topic)).forEach((topic, topicPartitions) -> topics.add(
            new OffsetDeleteRequestTopic()
            .setName(topic)
            .setPartitions(topicPartitions.stream()
                .map(tp -> new OffsetDeleteRequestPartition().setPartitionIndex(tp.partition()))
                .collect(Collectors.toList())
            )
        ));

        return new OffsetDeleteRequest.Builder(
            new OffsetDeleteRequestData()
                .setGroupId(groupId.idValue)
                .setTopics(topics)
        );
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final OffsetDeleteResponse response = (OffsetDeleteResponse) abstractResponse;
        Map<CoordinatorKey, Map<TopicPartition, Errors>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();

        final Errors error = Errors.forCode(response.data().errorCode());
        if (error != Errors.NONE) {
            handleGroupError(groupId, error, failed, groupsToUnmap, groupsToRetry);
        } else {
            final Map<TopicPartition, Errors> partitionResults = new HashMap<>();
            response.data().topics().forEach(topic ->
                topic.partitions().forEach(partitionoffsetDeleteResponse -> {
                    Errors partitionError = Errors.forCode(partitionoffsetDeleteResponse.errorCode());
                    TopicPartition topicPartition = new TopicPartition(topic.name(), partitionoffsetDeleteResponse.partitionIndex());
                    if (partitionError != Errors.NONE) {
                        handlePartitionError(groupId, partitionError, topicPartition, groupsToUnmap, groupsToRetry);
                    }

                    partitionResults.put(new TopicPartition(topic.name(), partitionoffsetDeleteResponse.partitionIndex()), partitionError);
                })
            );

            completed.put(groupId, partitionResults);
        }

        if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty()) {
            return new ApiResult<>(
                completed,
                failed,
                Collections.emptyList()
            );
        } else {
            // retry the request, so don't send completed/failed results back
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                new ArrayList<>(groupsToUnmap)
            );
        }
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
            case GROUP_ID_NOT_FOUND:
            case INVALID_GROUP_ID:
            case NON_EMPTY_GROUP:
                log.error("Received non retriable error for group {} in `{}` response", groupId,
                    apiName(), error.exception());
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group {} failed because the coordinator" +
                    " is still in the process of loading state. Will retry.", apiName(), groupId);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName(), groupId, error);
                groupsToUnmap.add(groupId);
                break;
            default:
                final String unexpectedErrorMsg = String.format("Received unexpected error for group %s in `%s` response",
                    groupId, apiName());
                log.error(unexpectedErrorMsg, error.exception());
                failed.put(groupId, error.exception());
                break;
        }
    }

    private void handlePartitionError(
        CoordinatorKey groupId,
        Errors error,
        TopicPartition topicPartition,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group {} in partition {} failed because the coordinator" +
                    " is still in the process of loading state. Will retry.", apiName(), groupId, topicPartition);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group {} in partition {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName(), groupId, topicPartition, error);
                groupsToUnmap.add(groupId);
                break;
            case GROUP_SUBSCRIBED_TO_TOPIC:
            case TOPIC_AUTHORIZATION_FAILED:
            case UNKNOWN_TOPIC_OR_PARTITION:
                log.debug("`{}` request for group {} in partition {} returned error {}.", apiName(), groupId, topicPartition, error);
                break;
            default:
                log.error("`{}` request for group {} in partition {} returned unexpected error {}.",
                    apiName(), groupId, topicPartition, error);
                break;
        }
    }

}
