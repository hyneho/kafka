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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 * <p>
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogMetadataTopicPartitioner#metadataPartition(TopicIdPartition)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that are deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);

    static long pollIntervalMs = 100L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final Consumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final Time time = new SystemTime();

    // TODO - Update comments below
    // It indicates whether the closing process has been started or not. If it is set as true,
    // consumer will stop consuming messages, and it will not allow partition assignments to be updated.
    private volatile boolean isClosed = false;
    // It indicates whether the consumer needs to assign the partitions or not. This is set when it is
    // determined that the consumer needs to be assigned with the updated partitions.
    private volatile boolean isAssignmentChanged = true;

    // It represents a lock for any operations related to the assignedTopicPartitions.
    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetadataPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private volatile Map<TopicIdPartition, UserTopicIdPartition> assignedUserTopicIdPartitions = Collections.emptyMap();
    private volatile Set<TopicIdPartition> processedAssignmentOfUserTopicIdPartitions = Collections.emptySet();

    private long uninitializedAt = time.milliseconds();
    private boolean isAllUserTopicPartitionsInitialized;

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> readOffsetsByMetadataPartition = new ConcurrentHashMap<>();
    private final Map<TopicIdPartition, Long> readOffsetsByUserTopicPartition = new HashMap<>();

    private Map<TopicPartition, BeginAndEndOffsetHolder> offsetHolderByMetadataPartition = new HashMap<>();
    private boolean isOffsetsFetchFailed = false;
    private long lastFailedFetchOffsetsTimestamp;

    public ConsumerTask(RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                        RemoteLogMetadataTopicPartitioner topicPartitioner,
                        Function<Optional<String>, Consumer<byte[], byte[]>> consumerSupplier) {
        this.consumer = consumerSupplier.apply(Optional.empty());
        this.remotePartitionMetadataEventHandler = Objects.requireNonNull(remotePartitionMetadataEventHandler);
        this.topicPartitioner = Objects.requireNonNull(topicPartitioner);
    }

    @Override
    public void run() {
        log.info("Starting consumer task thread.");
        while (!isClosed) {
            try {
                if (isAssignmentChanged) {
                    maybeWaitForPartitionsAssignment();
                }

                log.trace("Polling consumer to receive remote log metadata topic records");
                final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(pollIntervalMs));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    processConsumerRecord(record);
                }
                maybeMarkUserPartitionsAsReady();
            } catch (final WakeupException ex) {
                // ignore logging the error
                isClosed = true;
            } catch (final RetriableException ex) {
                log.warn("Retriable error occurred while processing the records. Retrying...", ex);
            }  catch (final Exception ex) {
                isClosed = true;
                log.error("Error occurred while processing the records", ex);
            }
        }
        try {
            consumer.close(Duration.ofSeconds(30));
        } catch (final Exception e) {
            log.error("Error encountered while closing the consumer", e);
        }
        log.info("Exited from consumer task thread");
    }

    private void processConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
        final RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
        if (canProcess(remoteLogMetadata, record.offset())) {
            remotePartitionMetadataEventHandler.handleRemoteLogMetadata(remoteLogMetadata);
            readOffsetsByUserTopicPartition.put(remoteLogMetadata.topicIdPartition(), record.offset());
        } else {
            log.debug("The event {} is skipped because it is either already processed or not assigned to this consumer", remoteLogMetadata);
        }
        log.debug("Updating consumed offset: [{}] for partition [{}]", record.offset(), record.partition());
        readOffsetsByMetadataPartition.put(record.partition(), record.offset());
    }

    private boolean canProcess(final RemoteLogMetadata metadata, final long recordOffset) {
        final TopicIdPartition tpId = metadata.topicIdPartition();
        final Long readOffset = readOffsetsByUserTopicPartition.get(tpId);
        return processedAssignmentOfUserTopicIdPartitions.contains(tpId) && (readOffset == null || readOffset < recordOffset);
    }

    private void maybeMarkUserPartitionsAsReady() {
        if (isAllUserTopicPartitionsInitialized) {
            return;
        }
        maybeFetchBeginAndEndOffsets();
        boolean isAllInitialized = true;
        for (final UserTopicIdPartition utp : assignedUserTopicIdPartitions.values()) {
            if (utp.isAssigned && !utp.isInitialized) {
                final Integer metadataPartition = utp.metadataPartition;
                final BeginAndEndOffsetHolder holder = offsetHolderByMetadataPartition.get(toRemoteLogPartition(metadataPartition));
                // The offset-holder can be null, when the recent assignment wasn't picked up by the consumer.
                if (holder != null) {
                    final Long readOffset = readOffsetsByMetadataPartition.getOrDefault(metadataPartition, -1L);
                    // 1) The end-offset was fetched only once during reassignment. The metadata-partition can receive
                    // new stream of records, so the consumer can read records more than the last-fetched end-offset.
                    // 2) When the internal topic becomes empty due to breach by size/time/start-offset, then there
                    // are no records to read.
                    if (readOffset + 1 >= holder.endOffset || holder.endOffset.equals(holder.beginOffset)) {
                        markInitialized(utp);
                    }
                }
            }
            isAllInitialized = isAllInitialized && utp.isInitialized;
        }
        if (isAllInitialized) {
            log.info("Initialized for all the {} assigned user-partitions mapped to the {} meta-partitions in {} ms",
                assignedUserTopicIdPartitions.size(), assignedMetadataPartitions.size(),
                time.milliseconds() - uninitializedAt);
        }
        isAllUserTopicPartitionsInitialized = isAllInitialized;
    }

    void maybeWaitForPartitionsAssignment() throws InterruptedException {
        // Snapshots of the metadata-partition and user-topic-partition are used to reduce the scope of the
        // synchronization block.
        // 1) LEADER_AND_ISR and STOP_REPLICA requests adds / removes the user-topic-partitions from the request
        //    handler threads. Those threads should not be blocked for a long time, therefore scope of the
        //    synchronization block is reduced to bare minimum.
        // 2) Note that the consumer#position, consumer#seekToBeginning, consumer#seekToEnd and the other consumer APIs
        //    response times are un-predictable. Those should not be kept in the synchronization block.
        final Set<Integer> metadataPartitionSnapshot = new HashSet<>();
        final Set<UserTopicIdPartition> assignedUserTopicIdPartitionsSnapshot = new HashSet<>();
        synchronized (assignPartitionsLock) {
            while (!isClosed && assignedUserTopicIdPartitions.isEmpty()) {
                log.debug("Waiting for remote log metadata partitions to be assigned");
                assignPartitionsLock.wait();
            }
            if (!isClosed && isAssignmentChanged) {
                assignedUserTopicIdPartitions.values().forEach(utp -> {
                    metadataPartitionSnapshot.add(utp.metadataPartition);
                    assignedUserTopicIdPartitionsSnapshot.add(utp);
                });
                isAssignmentChanged = false;
            }
        }
        if (!metadataPartitionSnapshot.isEmpty()) {
            final Set<TopicPartition> remoteLogPartitions = toRemoteLogPartitions(metadataPartitionSnapshot);
            consumer.assign(remoteLogPartitions);
            this.assignedMetadataPartitions = Collections.unmodifiableSet(metadataPartitionSnapshot);
            // for newly assigned user-partitions, read from the beginning of the corresponding metadata partition
            final Set<TopicPartition> seekToBeginOffsetPartitions = assignedUserTopicIdPartitionsSnapshot
                .stream()
                .filter(utp -> !utp.isAssigned)
                .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                .collect(Collectors.toSet());
            consumer.seekToBeginning(seekToBeginOffsetPartitions);
            // for other metadata partitions, read from the offset where the processing left last time.
            remoteLogPartitions.stream()
                .filter(tp -> !seekToBeginOffsetPartitions.contains(tp) &&
                    readOffsetsByMetadataPartition.containsKey(tp.partition()))
                .forEach(tp -> consumer.seek(tp, readOffsetsByMetadataPartition.get(tp.partition())));
            // mark all the user-topic-partitions as assigned to the consumer.
            assignedUserTopicIdPartitionsSnapshot.forEach(utp -> {
                if (!utp.isAssigned) {
                    // Note that there can be a race between `remove` and `add` partition assignment. Calling the
                    // `maybeLoadPartition` here again to be sure that the partition gets loaded on the handler.
                    remotePartitionMetadataEventHandler.maybeLoadPartition(utp.topicIdPartition);
                    utp.isAssigned = true;
                }
            });
            processedAssignmentOfUserTopicIdPartitions = assignedUserTopicIdPartitionsSnapshot.stream()
                .map(utp -> utp.topicIdPartition).collect(Collectors.toSet());
            clearResourcesForUnassignedUserTopicPartitions(assignedUserTopicIdPartitionsSnapshot);
            isAllUserTopicPartitionsInitialized = false;
            uninitializedAt = time.milliseconds();
            fetchBeginAndEndOffsets();
        }
    }

    private void clearResourcesForUnassignedUserTopicPartitions(Set<UserTopicIdPartition> assignedUTPs) {
        Set<TopicIdPartition> assignedPartitions = assignedUTPs.stream()
            .map(utp -> utp.topicIdPartition).collect(Collectors.toSet());
        // Note that there can be previously assigned user-topic-partitions where no records are there to read
        // (eg) none of the segments for a partition were uploaded. Those partition resources won't be cleared.
        // It can be fixed later when required since they are empty resources.
        Set<TopicIdPartition> unassignedPartitions = readOffsetsByUserTopicPartition.keySet()
            .stream()
            .filter(e -> !assignedPartitions.contains(e))
            .collect(Collectors.toSet());
        unassignedPartitions.forEach(unassignedPartition -> {
            remotePartitionMetadataEventHandler.clearTopicPartition(unassignedPartition);
            readOffsetsByUserTopicPartition.remove(unassignedPartition);
        });
        log.info("Unassigned user-topic-partitions: {}", unassignedPartitions.size());
    }

    public void addAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Objects.requireNonNull(partitions), Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Collections.emptySet(), Objects.requireNonNull(partitions));
    }

    private void updateAssignments(final Set<TopicIdPartition> addedPartitions,
                                   final Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for partitions added: {} and removed: {}", addedPartitions, removedPartitions);
        if (!addedPartitions.isEmpty() || !removedPartitions.isEmpty()) {
            synchronized (assignPartitionsLock) {
                final Map<TopicIdPartition, UserTopicIdPartition> idealUserPartitions = new HashMap<>(assignedUserTopicIdPartitions);
                addedPartitions.forEach(tpId -> idealUserPartitions.putIfAbsent(tpId, newUserTopicIdPartition(tpId)));
                removedPartitions.forEach(idealUserPartitions::remove);
                if (!idealUserPartitions.equals(assignedUserTopicIdPartitions)) {
                    assignedUserTopicIdPartitions = Collections.unmodifiableMap(idealUserPartitions);
                    isAssignmentChanged = true;
                }
                if (isAssignmentChanged) {
                    log.debug("Assigned user-topic-partitions: {}", assignedUserTopicIdPartitions);
                    assignPartitionsLock.notifyAll();
                }
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(final int partition) {
        return Optional.ofNullable(readOffsetsByMetadataPartition.get(partition));
    }

    public boolean isMetadataPartitionAssigned(final int partition) {
        return assignedMetadataPartitions.contains(partition);
    }

    public boolean isUserPartitionAssigned(final TopicIdPartition partition) {
        final UserTopicIdPartition utp = assignedUserTopicIdPartitions.get(partition);
        return utp != null && utp.isAssigned;
    }

    @Override
    public void close() {
        if (!isClosed) {
            log.info("Closing the instance");
            synchronized (assignPartitionsLock) {
                isClosed = true;
                assignedUserTopicIdPartitions.values().forEach(this::markInitialized);
                consumer.wakeup();
                assignPartitionsLock.notifyAll();
            }
        }
    }

    public Set<Integer> metadataPartitionsAssigned() {
        return Collections.unmodifiableSet(assignedMetadataPartitions);
    }

    private void fetchBeginAndEndOffsets() {
        try {
            final Set<TopicPartition> unInitializedPartitions = assignedUserTopicIdPartitions.values().stream()
                .filter(utp -> utp.isAssigned && !utp.isInitialized)
                .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                .collect(Collectors.toSet());
            // Removing the previous offset holder if it exists. During reassignment, if the list-offset
            // call to `earliest` and `latest` offset fails, then we should not use the previous values.
            unInitializedPartitions.forEach(x -> offsetHolderByMetadataPartition.remove(x));
            if (!unInitializedPartitions.isEmpty()) {
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(unInitializedPartitions);
                Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(unInitializedPartitions);
                offsetHolderByMetadataPartition = endOffsets.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new BeginAndEndOffsetHolder(beginOffsets.get(e.getKey()), e.getValue())));

            }
            isOffsetsFetchFailed = false;
        } catch (final RetriableException ex) {
            // ignore LEADER_NOT_AVAILABLE error, this can happen when the partition leader is not yet assigned.
            isOffsetsFetchFailed = true;
            lastFailedFetchOffsetsTimestamp = time.milliseconds();
        }
    }

    private void maybeFetchBeginAndEndOffsets() {
        // If the leader for a `__remote_log_metadata` partition is not available, then the call to `ListOffsets`
        // will fail after the default timeout of 1 min. Added a delay of 5 min in between the retries to prevent the
        // thread from aggressively fetching the list offsets. During this time, the recently reassigned
        // user-topic-partitions won't be marked as initialized.
        if (isOffsetsFetchFailed && lastFailedFetchOffsetsTimestamp + 300_000 < time.milliseconds()) {
            fetchBeginAndEndOffsets();
        }
    }

    private UserTopicIdPartition newUserTopicIdPartition(final TopicIdPartition tpId) {
        return new UserTopicIdPartition(tpId, topicPartitioner.metadataPartition(tpId));
    }

    private void markInitialized(final UserTopicIdPartition utp) {
        // Silently not initialize the utp
        if (!utp.isAssigned) {
            log.warn("Tried to initialize a UTP: {} that was not yet assigned!", utp);
            return;
        }
        if (!utp.isInitialized) {
            remotePartitionMetadataEventHandler.markInitialized(utp.topicIdPartition);
            utp.isInitialized = true;
        }
    }

    static Set<TopicPartition> toRemoteLogPartitions(final Set<Integer> partitions) {
        return partitions.stream()
            .map(ConsumerTask::toRemoteLogPartition)
            .collect(Collectors.toSet());
    }

    static TopicPartition toRemoteLogPartition(int partition) {
        return new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition);
    }

    static class UserTopicIdPartition {
        private final TopicIdPartition topicIdPartition;
        private final Integer metadataPartition;
        // The `utp` will be initialized once it reads all the existing events from the remote log metadata topic.
        boolean isInitialized;
        // denotes whether this `utp` is assigned to the consumer
        boolean isAssigned;

        /**
         * UserTopicIdPartition denotes the user topic-partitions for which this broker acts as a leader/follower of.
         *
         * @param tpId               the unique topic partition identifier
         * @param metadataPartition  the remote log metadata partition mapped for this user-topic-partition.
         */
        public UserTopicIdPartition(final TopicIdPartition tpId, final Integer metadataPartition) {
            this.topicIdPartition = Objects.requireNonNull(tpId);
            this.metadataPartition = Objects.requireNonNull(metadataPartition);
            this.isInitialized = false;
            this.isAssigned = false;
        }

        @Override
        public String toString() {
            return "UserTopicIdPartition{" +
                "topicIdPartition=" + topicIdPartition +
                ", metadataPartition=" + metadataPartition +
                ", isInitialized=" + isInitialized +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserTopicIdPartition that = (UserTopicIdPartition) o;
            return topicIdPartition.equals(that.topicIdPartition) && metadataPartition.equals(that.metadataPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, metadataPartition);
        }
    }

    static class BeginAndEndOffsetHolder {
        Long beginOffset;
        Long endOffset;

        public BeginAndEndOffsetHolder(Long beginOffset, Long endOffset) {
            this.beginOffset = beginOffset;
            this.endOffset = endOffset;
        }

        @Override
        public String toString() {
            return "BeginAndEndOffsetHolder{" +
                "beginOffset=" + beginOffset +
                ", endOffset=" + endOffset +
                '}';
        }
    }
}