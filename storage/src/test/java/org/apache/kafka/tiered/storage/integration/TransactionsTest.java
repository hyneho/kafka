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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.NUM_PARTITIONS;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.TOPIC1;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.TOPIC2;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumeRecords;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumeRecordsFor;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumerPositions;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createDefaultReadCommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createDefaultTransactionalProducer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createReadCommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createReadUncommittedConsumer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTopic;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTopics;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTransactionalProducer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.producerRecordWithExpectedTransactionStatus;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.recordValueAsString;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.restartDeadBrokers;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.sendOffset;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.sendTransactionalMessagesWithValueRange;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testFailureToFenceEpoch;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testTimeout;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.verifyLogStartOffsets;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.waitUntilLeaderIsKnown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(ClusterTestExtensions.class)
public class TransactionsTest {

    private static List<ClusterConfig> generator() {
        return TransactionTestUtils.generator(new Properties());
    }

    private static List<ClusterConfig> generateTV2Disabled() {
        return TransactionTestUtils.generateTV2Disabled(new Properties());
    }

    private static List<ClusterConfig> generateTV2Enabled() {
        return TransactionTestUtils.generateTV2Enabled(new Properties());
    }

    private Map<String, String> topicConfig() {
        return Collections.singletonMap(MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }

    @ClusterTemplate("generator")
    public void testBasicTransactions(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());

        try (var producer = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster);
             var unCommittedConsumer = createReadUncommittedConsumer(cluster)
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
            producer.abortTransaction();


            // We've sent 1 record + 1 abort mark = 2 (segments) to each topic partition,
            // so 1 segment should be offloaded, the local log start offset should be 1
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "3", "3", true));

            // Before records are committed, these records won't be offloaded.
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            producer.commitTransaction();

            // We've sent 2 records + 1 abort mark + 1 commit mark = 4 (segments) to each topic partition,
            // so 3 segments should be offloaded, the local log start offset should be 3
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            consumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));
            unCommittedConsumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));

            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            var allRecords = consumeRecords(unCommittedConsumer, 4);
            var expectedValues = Set.of("1", "2", "3", "4");
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    @ClusterTemplate("generator")
    public void testReadCommittedConsumerShouldNotSeeUndecidedData(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "other", 2000, 2000, 4000, 1000);
             var readCommittedConsumer = createDefaultReadCommittedConsumer(cluster);
             var readUncommittedConsumer = createReadUncommittedConsumer(cluster)
        ) {

            producer1.initTransactions();
            producer2.initTransactions();
            producer1.beginTransaction();
            producer2.beginTransaction();

            var latestVisibleTimestamp = System.currentTimeMillis();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestVisibleTimestamp, "x".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestVisibleTimestamp, "x".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer2.flush();

            var latestWrittenTimestamp = latestVisibleTimestamp + 1;
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "a".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "b".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "c".getBytes(UTF_8), "3".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "d".getBytes(UTF_8), "4".getBytes(UTF_8)));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "x".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "x".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer2.commitTransaction();

            var tp1 = new TopicPartition(TOPIC1, 0);
            var tp2 = new TopicPartition(TOPIC2, 0);
            readUncommittedConsumer.assign(Set.of(tp1, tp2));
            consumeRecords(readUncommittedConsumer, 8);

            var readUncommittedOffsetsForTimes =
                    readUncommittedConsumer.offsetsForTimes(Map.of(tp1, latestWrittenTimestamp, tp2, latestWrittenTimestamp));
            assertEquals(2, readUncommittedOffsetsForTimes.size());
            assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp1).timestamp());
            assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp2).timestamp());
            readUncommittedConsumer.unsubscribe();

            // we should only see the first two records which come before the undecided second transaction
            readCommittedConsumer.assign(Set.of(tp1, tp2));
            consumeRecords(readCommittedConsumer, 2).forEach(record -> {
                assertEquals("x", new String(record.key()));
                assertEquals("1", new String(record.value()));
            });

            // even if we seek to the end, we should not be able to see the undecided data
            assertEquals(2, readCommittedConsumer.assignment().size());
            readCommittedConsumer.seekToEnd(readCommittedConsumer.assignment());
            readCommittedConsumer.assignment().forEach(tp -> assertEquals(1L, readCommittedConsumer.position(tp)));

            // undecided timestamps should not be searchable either
            var readCommittedOffsetsForTimes =
                    readCommittedConsumer.offsetsForTimes(Map.of(tp1, latestWrittenTimestamp, tp2, latestWrittenTimestamp));
            assertNull(readCommittedOffsetsForTimes.get(tp1));
            assertNull(readCommittedOffsetsForTimes.get(tp2));
        }
    }

    @ClusterTemplate("generator")
    public void testDelayedFetchIncludesAbortedTransaction(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        // ensure that the consumer's fetch will sit in purgatory
        var consumerProps = new Properties();
        consumerProps.put(FETCH_MIN_BYTES_CONFIG, "100000");
        consumerProps.put(FETCH_MAX_WAIT_MS_CONFIG, "100");

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "other", 2000, 2000, 4000, 1000);
             var readCommittedConsumer = createReadCommittedConsumer(cluster, "group", 500, consumerProps)
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

            producer1.abortTransaction();
            producer2.commitTransaction();

            // We've sent 4 records + 1 abort mark + 1 commit mark = 6 (segments),
            // so 5 segments should be offloaded, the local log start offset should be 5
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p0, 0));

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

    @SuppressWarnings("deprecation")
    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupId(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), groupId), true);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupMetadata(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata()), true);
    }

    @ClusterTemplate("generator")
    public void testFencingOnCommit(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster)
        ) {

            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class, producer1::commitTransaction);

            producer2.commitTransaction();  // ok

            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendOffsets(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster)
        ) {

            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.flush();

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class,
                    () -> producer1.sendOffsetsToTransaction(Map.of(
                                    new TopicPartition("foobartopic", 0), new OffsetAndMetadata(110L)
                            ), new ConsumerGroupMetadata("foobarGroup")
                    )
            );
            producer2.commitTransaction();  // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    @ClusterTemplate("generator")
    public void testOffsetMetadataInSendOffsetsToTransaction(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        var tp = new TopicPartition(TOPIC1, 0);
        var groupId = "group";

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createReadCommittedConsumer(cluster, groupId, 100, new Properties())
        ) {

            consumer.subscribe(List.of(TOPIC1));

            producer1.initTransactions();
            producer1.beginTransaction();

            var offsetAndMetadata = new OffsetAndMetadata(110L, Optional.of(15), "some metadata");
            producer1.sendOffsetsToTransaction(Map.of(tp, offsetAndMetadata), new ConsumerGroupMetadata(groupId));
            producer1.commitTransaction();  // ok

            // The call to commit the transaction may return before all markers are visible, so we initialize a second
            // producer to ensure the transaction completes and the committed offsets are visible.
            producer2.initTransactions();

            waitForCondition(() -> offsetAndMetadata.equals(consumer.committed(Set.of(tp)).get(tp)), "cannot read committed offset");
        }
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
    public void testFencingOnSend(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster)
        ) {
            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();
            producer1.beginTransaction();

            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            try {
                producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false)).get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (ProducerFencedException e) {
                producer1.close();
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidProducerEpochException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }

            producer2.commitTransaction(); // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    @ClusterTemplate("generator")
    public void testFencingOnAddPartitions(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster)
        ) {
            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();
            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.abortTransaction();

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true))
                    .get(20, SECONDS);
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true))
                    .get(20, SECONDS);

            try {
                producer1.beginTransaction();
                producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false)).get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (ProducerFencedException __) {

            } catch (ExecutionException e) {
                assertInstanceOf(ProducerFencedException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }

            producer2.commitTransaction(); // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2Enabled(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer = createTransactionalProducer(cluster, "transactionalProducer", 2000, 2000, 5000, 5000);
             var consumer = createDefaultReadCommittedConsumer(cluster);
        ) {
            // Create a topic with RF=1 so that a single broker failure will render it unavailable
            var testTopic = "test-topic";
            createTopic(cluster, testTopic, NUM_PARTITIONS, 1, Map.of());
            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(testTopic, 0));

            producer.initTransactions();
            // First transaction: commit
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            var kafkaBroker = cluster.brokers().get(partitionLeader);
            // Get producerId and epoch after first commit
            var log = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get();
            var producerStateManager = log.producerStateManager();
            var activeProducersIter = producerStateManager.activeProducers()
                    .entrySet().iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var previousProducerEpoch = producerStateEntry.producerEpoch();

            // Second transaction: abort
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));

            kafkaBroker.shutdown(); // kill the partition leader to prevent the batch from being submitted
            var failedFuture = producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(6000); // Wait for the record to time out
            restartDeadBrokers(cluster);

            assertFutureThrows(failedFuture, TimeoutException.class);
            producer.abortTransaction();

            // Get producer epoch after abortTransaction and verify it has increased.
            producerStateEntry = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get()
                    .producerStateManager()
                    .activeProducers().get(producerId);
            // Assert that producerStateEntry is not null
            assertNotNull(producerStateEntry, "Producer state entry should not be null after abortTransaction");

            var currentProducerEpoch = producerStateEntry.producerEpoch();
            assertTrue(currentProducerEpoch > previousProducerEpoch,
                    "Producer epoch after abortTransaction ($currentProducerEpoch) should be greater than after first commit ($previousProducerEpoch)"
            );
            
            // Update previousProducerEpoch
            var producerEpoch = producerStateEntry.producerEpoch();

            // Third transaction: commit
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            TestUtils.waitForCondition(() -> {
                var logOption = kafkaBroker.logManager().getLog(new TopicPartition(testTopic, 0), false);
                return logOption.exists(l -> {
                    var producerStateEntry1 = l.producerStateManager().activeProducers().get(producerId);
                    return producerStateEntry1 != null && producerStateEntry1.producerEpoch() > producerEpoch;
                });
            }, String.format("Timed out waiting for producer epoch to be incremented for topic %s after second commit", testTopic));

            consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));

            consumeRecords(consumer, 5).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    @ClusterTemplate("generator")
    public void testFencingOnTransactionExpiration(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer = createTransactionalProducer(cluster, "expiringProducer", 100, 2000, 4000, 1000);
             var nonTransactionalConsumer = createReadUncommittedConsumer(cluster);
             var transactionalConsumer = createDefaultReadCommittedConsumer(cluster)
        ) {
            producer.initTransactions();
            producer.beginTransaction();

            // The first message and hence the first AddPartitions request should be successfully sent.
            var firstMessageResult = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false)
            ).get();
            assertTrue(firstMessageResult.hasOffset());

            // Wait for the expiration cycle to kick in.
            Thread.sleep(600);

            // Now that the transaction has expired, the second send should fail with a ProducerFencedException.
            var exception = assertThrows(ExecutionException.class,
                    () -> producer.send(producerRecordWithExpectedTransactionStatus(
                            TOPIC1, null, "2", "2", false)).get());
            assertInstanceOf(ProducerFencedException.class, exception.getCause());

            // Verify that the first message was aborted and the second one was never written at all.
            nonTransactionalConsumer.subscribe(List.of(TOPIC1));

            // Attempt to consume the one written record. We should not see the second. The
            // assertion does not strictly guarantee that the record wasn't written, but the
            // data is small enough that had it been written, it would have been in the first fetch.
            var records = consumeRecords(nonTransactionalConsumer, 1);
            assertEquals(1, records.size());
            assertEquals("1", recordValueAsString(records.get(0)));

            transactionalConsumer.subscribe(List.of(TOPIC1));

            var transactionalRecords = consumeRecordsFor(transactionalConsumer);
            assertTrue(transactionalRecords.isEmpty());
        }
    }

    @ClusterTemplate("generator")
    public void testMultipleMarkersOneLeader(ClusterInstance cluster) throws InterruptedException {
        createTopics(cluster, topicConfig());
        try (var firstProducer = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster);
             var unCommittedConsumer = createReadUncommittedConsumer(cluster)
        ) {
            var topicWith10Partitions = "largeTopic";
            var topicWith10PartitionsAndOneReplica = "largeTopicOneReplica";

            createTopic(cluster, topicWith10Partitions, 10, cluster.brokers().size(), topicConfig());
            createTopic(cluster, topicWith10PartitionsAndOneReplica, 10, 1, Map.of());

            firstProducer.initTransactions();

            firstProducer.beginTransaction();
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 0, 5000, false);
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10PartitionsAndOneReplica, 5000, 10000, false);
            firstProducer.abortTransaction();

            firstProducer.beginTransaction();
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 10000, 11000, true);
            firstProducer.commitTransaction();

            consumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));
            unCommittedConsumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));

            consumeRecords(consumer, 1000).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            var allRecords = consumeRecords(unCommittedConsumer, 11000);
            var expectedValues = IntStream.range(0, 11000).mapToObj(Integer::toString).collect(Collectors.toSet());
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
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
    public void testBumpTransactionalEpochWithTV2Disabled(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        try (var producer = createTransactionalProducer(cluster, "transactionalProducer", 2000, 2000,
                5000, 5000);
             var consumer = createDefaultReadCommittedConsumer(cluster)
        ) {
            // Create a topic with RF=1 so that a single broker failure will render it unavailable
            var testTopic = "test-topic";
            createTopic(cluster, testTopic, NUM_PARTITIONS, 1, Map.of());
            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(testTopic, 0));

            producer.initTransactions();
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            var kafkaBroker = cluster.brokers().get(partitionLeader);

            var activeProducersIter = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get()
                    .producerStateManager().activeProducers().entrySet().iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var initialProducerEpoch = producerStateEntry.producerEpoch();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));

            kafkaBroker.shutdown(); // kill the partition leader to prevent the batch from being submitted
            var failedFuture = producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(600); // Wait for the record to time out
            restartDeadBrokers(cluster);

            assertFutureThrows(failedFuture, TimeoutException.class);
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));

            consumeRecords(consumer, 5).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            // Producers can safely abort and continue after the last record of a transaction timing out, so it's possible to
            // get here without having bumped the epoch. If bumping the epoch is possible, the producer will attempt to, so
            // check there that the epoch has actually increased
            producerStateEntry = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get().producerStateManager().activeProducers().get(producerId);
            assertNotNull(producerStateEntry);
            assertTrue(producerStateEntry.producerEpoch() > initialProducerEpoch);
        }
    }

    @ClusterTemplate("generator")
    public void testFailureToFenceEpochTV2Disable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, false, topicConfig());
    }

    @ClusterTemplate("generateTV2Enabled")
    public void testFailureToFenceEpochTV2Enable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, true, topicConfig());
    }
}
