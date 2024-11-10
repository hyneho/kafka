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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.TOPIC1;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.TOPIC2;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumeRecords;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumerPositions;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createDefaultReadCommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createDefaultTransactionalProducer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createReadCommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createReadUncommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTopics;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTransactionalProducer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.maybeWaitForAtLeastOneSegmentUpload;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.producerRecordWithExpectedTransactionStatus;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.recordValueAsString;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.sendOffset;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testFailureToFenceEpoch;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testTimeout;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.verifyLogStartOffsets;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createPropsForRemoteStorage;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createTopicConfigForRemoteStorage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ClusterTestExtensions.class)
public class TransactionsWithTieredStoreTest {

    private static List<ClusterConfig> generator() {
        String randomString = TestUtils.randomString(5);
        String storageDirPath = TestUtils.tempDirectory("kafka-remote-tier-" + randomString).getAbsolutePath();
        return TransactionTestUtils.generator(overridingProps(randomString, storageDirPath));
    }

    private static List<ClusterConfig> generateTV2Disabled() {
        String randomString = TestUtils.randomString(5);
        String storageDirPath = TestUtils.tempDirectory("kafka-remote-tier-" + randomString).getAbsolutePath();
        return TransactionTestUtils.generateTV2Disabled(overridingProps(randomString, storageDirPath));
    }

    private static List<ClusterConfig> generateTV2Enabled() {
        String randomString = TestUtils.randomString(5);
        String storageDirPath = TestUtils.tempDirectory("kafka-remote-tier-" + randomString).getAbsolutePath();
        return TransactionTestUtils.generateTV2Enabled(overridingProps(randomString, storageDirPath));
    }

    private static Properties overridingProps(String randomString, String storageDirPath) {
        int numRemoteLogMetadataPartitions = 3;
        return createPropsForRemoteStorage(randomString, storageDirPath, 3,
                numRemoteLogMetadataPartitions, new Properties());
    }
    
    // FIXME
    @ClusterTemplate("generator")
    public void testBasicTransactionsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        testBasicTransactions(cluster, GroupProtocol.CLASSIC);
    }

    // FIXME
    @ClusterTemplate("generator")
    public void testBasicTransactionsWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        testBasicTransactions(cluster, GroupProtocol.CONSUMER);
    }

    @ClusterTemplate("generator")
    public void testReadCommittedConsumerShouldNotSeeUndecidedDataWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testReadCommittedConsumerShouldNotSeeUndecidedData(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testReadCommittedConsumerShouldNotSeeUndecidedDataWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testReadCommittedConsumerShouldNotSeeUndecidedData(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generator")
    public void testDelayedFetchIncludesAbortedTransactionWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        testDelayedFetchIncludesAbortedTransaction(cluster, GroupProtocol.CLASSIC);
    }

    // FIXME
    @ClusterTemplate("generator")
    public void testDelayedFetchIncludesAbortedTransactionWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        testDelayedFetchIncludesAbortedTransaction(cluster, GroupProtocol.CONSUMER);
    }

    @SuppressWarnings("deprecation")
    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupIdWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), groupId), true, GroupProtocol.CLASSIC);
    }

    @SuppressWarnings("deprecation")
    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupIdWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), groupId), true, GroupProtocol.CONSUMER);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupMetadataWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata()), true, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupMetadataWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata()), true, GroupProtocol.CONSUMER);
    }

    @ClusterTemplate("generator")
    public void testFencingOnCommitWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnCommit(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnCommitWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnCommit(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendOffsetsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSendOffsets(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendOffsetsWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSendOffsets(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testOffsetMetadataInSendOffsetsToTransactionWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testOffsetMetadataInSendOffsetsToTransaction(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testOffsetMetadataInSendOffsetsToTransactionWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testOffsetMetadataInSendOffsetsToTransaction(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testInitTransactionsTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, false, KafkaProducer::initTransactions);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsToTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, producer -> producer.sendOffsetsToTransaction(
                Map.of(new TopicPartition(TOPIC1, 0), new OffsetAndMetadata(0)),
                new ConsumerGroupMetadata("test-group")
        ));
    }

    @ClusterTemplate("generator")
    public void testCommitTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, KafkaProducer::commitTransaction);
    }

    @ClusterTemplate("generator")
    public void testAbortTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, KafkaProducer::abortTransaction);
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSend(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSend(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnAddPartitionsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnAddPartitions(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnAddPartitionsWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnAddPartitions(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2EnabledWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Enabled(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2EnabledWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Enabled(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generator")
    public void testFencingOnTransactionExpirationWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnTransactionExpiration(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnTransactionExpirationWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnTransactionExpiration(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testMultipleMarkersOneLeaderWithClassicGroupProtocol(ClusterInstance cluster) throws InterruptedException {
        TransactionTestUtils.testMultipleMarkersOneLeader(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testMultipleMarkersOneLeaderWithClassicConsumerProtocol(ClusterInstance cluster) throws InterruptedException {
        TransactionTestUtils.testMultipleMarkersOneLeader(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testConsecutivelyRunInitTransactions(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        try (var producer = createTransactionalProducer(cluster, "normalProducer",
                100, 2000, 4000, 1000)) {
            producer.initTransactions();
            assertThrows(IllegalStateException.class, producer::initTransactions);
        }
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2DisabledWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Disabled(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2DisabledWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Disabled(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFailureToFenceEpochTV2Disable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, false, topicConfig());
    }

    @ClusterTemplate("generateTV2Enabled")
    public void testFailureToFenceEpochTV2Enable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, true, topicConfig());
    }

    private Map<String, String> topicConfig() {
        boolean enableRemoteStorage = true;
        int maxBatchCountPerSegment = 1;
        Map<String, String> config = new HashMap<>();
        config.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        config.putAll(createTopicConfigForRemoteStorage(enableRemoteStorage, maxBatchCountPerSegment));
        return config;
    }

    private void testBasicTransactions(ClusterInstance cluster, GroupProtocol groupProtocol) throws InterruptedException {
        createTopics(cluster, topicConfig());

        try (var producer = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol);
             var unCommittedConsumer = createReadUncommittedConsumer(cluster, groupProtocol)
        ) {
            var t1p1 = new TopicPartition(TOPIC1, 1);
            var t2p2 = new TopicPartition(TOPIC2, 2);

            producer.initTransactions();
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "2", "2", false));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "4", "4", false));
            producer.flush();

            // Since we haven't committed/aborted any records, the last stable offset is still 0,
            // no segments should be offloaded to remote storage
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p1, 0L, t2p2, 0L));
            producer.abortTransaction();

            maybeWaitForAtLeastOneSegmentUpload(cluster, Set.of(t1p1, t2p2), false);

            // We've sent 1 record + 1 abort mark = 2 (segments) to each topic partition,
            // so 1 segment should be offloaded, the local log start offset should be 1
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p1, 1L, t2p2, 1L));

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "3", "3", true));

            // Before records are committed, these records won't be offloaded.
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p1, 1L, t2p2, 1L));

            producer.commitTransaction();

            // We've sent 2 records + 1 abort mark + 1 commit mark = 4 (segments) to each topic partition,
            // so 3 segments should be offloaded, the local log start offset should be 3
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p1, 3L, t2p2, 3L));

            consumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));
            unCommittedConsumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));

            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            var allRecords = consumeRecords(unCommittedConsumer, 4);
            var expectedValues = Set.of("1", "2", "3", "4");
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    private void testDelayedFetchIncludesAbortedTransaction(ClusterInstance cluster, GroupProtocol groupProtocol) throws InterruptedException {
        createTopics(cluster, topicConfig());
        // ensure that the consumer's fetch will sit in purgatory
        var consumerProps = new Properties();
        consumerProps.put(FETCH_MIN_BYTES_CONFIG, "100000");
        consumerProps.put(FETCH_MAX_WAIT_MS_CONFIG, "100");

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "other", 2000, 2000, 4000, 1000);
             var readCommittedConsumer = createReadCommittedConsumer(cluster, "group", 500, groupProtocol, consumerProps)
        ) {
            var t1p0 = new TopicPartition(TOPIC1, 0);

            producer1.initTransactions();
            producer2.initTransactions();

            producer1.beginTransaction();
            producer2.beginTransaction();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "1".getBytes()));
            producer2.flush();

            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "1".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "2".getBytes()));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "2".getBytes()));
            producer2.flush();

            // Since we haven't committed/aborted any records, the last stable offset is still 0,
            // no segments should be offloaded to remote storage
            verifyLogStartOffsets(cluster, Map.of(t1p0, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p0, 0L));

            producer1.abortTransaction();
            producer2.commitTransaction();

            maybeWaitForAtLeastOneSegmentUpload(cluster, Set.of(t1p0), false);
            // We've sent 4 records + 1 abort mark + 1 commit mark = 6 (segments),
            // so 5 segments should be offloaded, the local log start offset should be 5
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p0, 0));
            maybeVerifyLocalLogStartOffsets(cluster, Map.of(t1p0, 5L));

            readCommittedConsumer.assign(Set.of(t1p0));
            var records = consumeRecords(readCommittedConsumer, 2);
            assertEquals(2, records.size());

            var first = records.get(0);
            assertEquals("x", new String(first.key()));
            assertEquals("1", new String(first.value()));
            assertEquals(0L, first.offset());

            var second = records.get(1);
            assertEquals("x", new String(second.key()));
            assertEquals("2", new String(second.value()));
            assertEquals(3L, second.offset());
        }
    }

    private void maybeVerifyLocalLogStartOffsets(ClusterInstance cluster, Map<TopicPartition, Long> partitionLocalStartOffsets) throws InterruptedException {
        Map<Integer, Long> offsets = new HashMap<>();
        TestUtils.waitForCondition(() ->
                cluster.brokers().values().stream().allMatch(broker ->
                        partitionLocalStartOffsets
                                .entrySet().stream().allMatch(entry -> {
                                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                                    offsets.put(broker.config().brokerId(), offset);
                                    return entry.getValue() == offset;
                                })
                ), () -> "local log start offset doesn't change to the expected position:" + partitionLocalStartOffsets + ", current position:" + offsets);
    }
}
