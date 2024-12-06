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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);

    /**
    * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
    */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
    * @see KafkaConsumer#subscribe(Pattern)
    */
    void subscribe(Pattern pattern);

    /**
     * @see KafkaConsumer#subscribe(SubscriptionPattern, ConsumerRebalanceListener)
     */
    void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#subscribe(SubscriptionPattern)
     */
    void subscribe(SubscriptionPattern pattern);

    /**
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * @see KafkaConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * @see KafkaConsumer#commitSync(Duration)
     */
    void commitSync(Duration timeout);

    /**
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see KafkaConsumer#commitSync(Map, Duration)
     */
    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
    /**
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#registerMetricForSubscription(KafkaMetric)
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * @see KafkaConsumer#unregisterMetricFromSubscription(KafkaMetric)
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);
    /**
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata)
     */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);
    
    /**
     * @see KafkaConsumer#position(TopicPartition, Duration)
     */
    long position(TopicPartition partition, final Duration timeout);

    /**
     * @see KafkaConsumer#committed(Set)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#committed(Set, Duration)
     */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout);

    /**
     * See {@link KafkaConsumer#clientInstanceId(Duration)}}
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#partitionsFor(String, Duration)
     */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    /**
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#listTopics(Duration)
     */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    /**
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#offsetsForTimes(Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * @see KafkaConsumer#offsetsForTimes(Map, Duration)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * @see KafkaConsumer#endOffsets(Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#endOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * @see KafkaConsumer#currentLag(TopicPartition)
     */
    OptionalLong currentLag(TopicPartition topicPartition);

    /**
     * @see KafkaConsumer#groupMetadata()
     */
    ConsumerGroupMetadata groupMetadata();

    /**
     * @see KafkaConsumer#enforceRebalance()
     */
    void enforceRebalance();

    /**
     * @see KafkaConsumer#enforceRebalance(String)
     */
    void enforceRebalance(final String reason);

    /**
     * @see KafkaConsumer#close()
     */
    void close();

    /**
     * This method has been deprecated since Kafka 4.0 and should use {@link Consumer#close(CloseOption)} instead.
     *
     * @see KafkaConsumer#close(Duration)
     */
    @Deprecated
    void close(Duration timeout);

    /**
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();

    /**
     * @see KafkaConsumer#close(CloseOption)
     */
    void close(final CloseOption option);

    class CloseOption {

        /**
         * Specifies the group membership operation upon shutdown.
         * By default, {@code GroupMembershipOperation.DEFAULT} will be applied, which follows the consumer's default behavior.
         */
        protected GroupMembershipOperation operation = GroupMembershipOperation.DEFAULT;

        /**
         * Specifies the maximum amount of time to wait for the close process to complete.
         * This allows users to define a custom timeout for gracefully stopping the consumer.
         * If no value is set, the default timeout {@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS} will be applied.
         */
        protected Optional<Duration> timeout = Optional.empty();

        private CloseOption() {
        }

        protected CloseOption(final CloseOption option) {
            this.operation = option.operation;
            this.timeout = option.timeout;
        }

        /**
         * Static method to create a {@code CloseOption} with a custom timeout.
         *
         * @param timeout the maximum time to wait for the consumer to close.
         * @return a new {@code CloseOption} instance with the specified timeout.
         */
        public static CloseOption timeout(final Duration timeout) {
            CloseOption option = new CloseOption();
            option.timeout = Optional.ofNullable(timeout);
            return option;
        }

        /**
         * Static method to create a {@code CloseOption} with a specified group membership operation.
         *
         * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP},
         *                 or {@code DEFAULT}.
         * @return a new {@code CloseOption} instance with the specified group membership operation.
         */
        public static CloseOption groupMembershipOperation(final GroupMembershipOperation operation) {
            CloseOption option = new CloseOption();
            option.operation = operation;
            return option;
        }

        /**
         * Fluent method to set the timeout for the close process.
         *
         * @param timeout the maximum time to wait for the consumer to close. If {@code null}, the default timeout will be used.
         * @return this {@code CloseOption} instance.
         */
        public CloseOption withTimeout(final Duration timeout) {
            this.timeout = Optional.ofNullable(timeout);
            return this;
        }

        /**
         * Fluent method to set the group membership operation upon shutdown.
         *
         * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP}, or {@code DEFAULT}.
         * @return this {@code CloseOption} instance.
         */
        public CloseOption withGroupMembershipOperation(final GroupMembershipOperation operation) {
            this.operation = operation;
            return this;
        }
    }
}
