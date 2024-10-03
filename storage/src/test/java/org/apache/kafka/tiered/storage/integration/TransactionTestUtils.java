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

import kafka.server.HostedPartition;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.test.api.Type.CO_KRAFT;
import static org.apache.kafka.common.test.api.Type.KRAFT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG;
import static org.apache.kafka.server.common.Features.TRANSACTION_VERSION;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionTestUtils {

    public static final String TOPIC1 = "topic1";
    public static final String TOPIC2 = "topic2";
    public static final int NUM_PARTITIONS = 4;

    private static final String TRANSACTION_STATUS_KEY = "transactionStatus";
    private static final String COMMITTED_KEY = "committed";

    static List<ClusterConfig> generator(Properties prop) {
        return List.of(ClusterConfig.defaultBuilder()
                .setBrokers(3)
                .setAutoStart(true)
                .setTypes(Set.of(KRAFT, CO_KRAFT))
                .setServerProperties(serverConfig(prop))
                .setTags(List.of("kraftGroupCoordinator"))
                .build());
    }

    static List<ClusterConfig> generateTV2Disabled(Properties prop) {
        return generator(prop, false);
    }
    
    static List<ClusterConfig> generateTV2Enabled(Properties prop) {
        return generator(prop, true);
    }

    static void createTopic(
            ClusterInstance cluster,
            String topName,
            int numPartitions,
            int size,
            Map<String, String> config
    ) {
        try (Admin adminClient = cluster.createAdminClient()) {
            adminClient.createTopics(List.of(new NewTopic(topName, numPartitions, (short) size).configs(config)));
        }
    }

    static void createTopics(ClusterInstance cluster, Map<String, String> topicConfig) {
        try (Admin adminClient = cluster.createAdminClient()) {
            adminClient.createTopics(List.of(
                    new NewTopic(TOPIC1, NUM_PARTITIONS, (short) 3).configs(topicConfig),
                    new NewTopic(TOPIC2, NUM_PARTITIONS, (short) 3).configs(topicConfig))
            );
        }
    }

    static KafkaProducer<byte[], byte[]> createDefaultTransactionalProducer(ClusterInstance cluster) {
        return createTransactionalProducer(cluster, "transactional-producer",
                2000, 2000, 4000, 1000);
    }

    static KafkaProducer<byte[], byte[]> createTransactionalProducer(
            ClusterInstance cluster,
            String transactionalId,
            int transactionTimeoutMs,
            int maxBlockMs,
            int deliveryTimeoutMs,
            int requestTimeoutMs
    ) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ACKS_CONFIG, "all");
        props.put(BATCH_SIZE_CONFIG, 16384);
        props.put(TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        props.put(MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.put(DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        return new KafkaProducer<>(props);
    }

    static KafkaConsumer<byte[], byte[]> createDefaultReadCommittedConsumer(ClusterInstance cluster) {
        return createReadCommittedConsumer(cluster, "transactional-group", 100, new Properties());
    }

    static KafkaConsumer<byte[], byte[]> createReadCommittedConsumer(
            ClusterInstance cluster,
            String group,
            int maxPollRecords,
            Properties props
    ) {
        var prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        prop.put(GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);
        prop.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(GROUP_ID_CONFIG, group);
        prop.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        prop.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        prop.putAll(props);
        return new KafkaConsumer<>(prop, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    static KafkaConsumer<byte[], byte[]> createReadUncommittedConsumer(ClusterInstance cluster) {
        var prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        prop.put(GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);
        prop.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(GROUP_ID_CONFIG, "non-transactional-group");
        prop.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        prop.put(MAX_POLL_RECORDS_CONFIG, 500);
        prop.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        return new KafkaConsumer<>(prop);
    }

    static ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            Integer partition,
            String key,
            String value,
            boolean willBeCommitted
    ) {
        return producerRecordWithExpectedTransactionStatus(topic, partition, key, value, new AtomicBoolean(willBeCommitted));
    }

    static ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            Integer partition,
            String key,
            String value,
            AtomicBoolean willBeCommitted
    ) {
        var header = new Header() {
            @Override
            public String key() {
                return TRANSACTION_STATUS_KEY;
            }

            @Override
            public byte[] value() {
                return willBeCommitted.get() ? COMMITTED_KEY.getBytes(UTF_8) : "aborted".getBytes(UTF_8);
            }
        };
        return new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes(), singleton(header));
    }

    static void verifyLogStartOffsets(
            ClusterInstance cluster,
            Map<TopicPartition, Integer> partitionStartOffsets
    ) throws InterruptedException {
        Collection<KafkaBroker> brokers = cluster.brokers().values();
        Map<Integer, Long> offsets = new HashMap<>();
        waitForCondition(() -> {
            for (KafkaBroker broker : brokers) {
                for (Map.Entry<TopicPartition, Integer> entry : partitionStartOffsets.entrySet()) {
                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                    offsets.put(broker.config().brokerId(), offset);
                    return entry.getValue() == offset;
                }
            }
            return false;
        }, "log start offset doesn't change to the expected position: " + partitionStartOffsets + ", current position: " + offsets);
    }

    static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
            Consumer<K, V> consumer,
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = pollUntilAtLeastNumRecords(consumer, numRecords);
        assertEquals(numRecords, records.size(), "Consumed more records than expected");
        return records;
    }

    static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(
            Consumer<K, V> consumer,
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return records.size() >= numRecords;
        }, DEFAULT_MAX_WAIT_MS, format("Expected %d records will be consumed", numRecords));
        return records;
    }

    static String assertCommittedAndGetValue(ConsumerRecord<byte[], byte[]> record) {
        var header = record.headers().headers(TRANSACTION_STATUS_KEY).iterator().next();
        if (header != null) {
            assertEquals(COMMITTED_KEY, new String(header.value(), UTF_8), "Got " + new String(header.value(), UTF_8) + " but expected the value to indicate committed status.");
        } else {
            throw new RuntimeException("expected the record header to include an expected transaction status, but received nothing.");
        }
        return recordValueAsString(record);
    }

    static String recordValueAsString(ConsumerRecord<byte[], byte[]> record) {
        return new String(record.value(), UTF_8);
    }

    /**
     * The basic plan for the test is as follows:
     * 1. Seed topic1 with 500 unique, numbered, messages.
     * 2. Run a consume/process/produce loop to transactionally copy messages from topic1 to topic2 and commit
     * offsets as part of the transaction.
     * 3. Randomly abort transactions in step2.
     * 4. Validate that we have 500 unique committed messages in topic2. If the offsets were committed properly with the
     * transactions, we should not have any duplicates or missing messages since we should process in the input
     * messages exactly once.
     */
    static void sendOffset(ClusterInstance cluster, OffsetHandler handler, boolean isSkip) throws Exception {
        var consumerGroupId = "foobar-consumer-group";
        var numSeedMessages = 500;
        seedTopicWithNumberedRecords(numSeedMessages, cluster);

        try (var producer = createDefaultTransactionalProducer(cluster);
             var consumer = createReadCommittedConsumer(cluster, consumerGroupId, numSeedMessages / 4, new Properties())
        ) {
            consumer.subscribe(List.of(TOPIC1));
            producer.initTransactions();

            AtomicBoolean shouldCommit = new AtomicBoolean(false);
            int recordsProcessed = 0;

            while (recordsProcessed < numSeedMessages) {
                var records = pollUntilAtLeastNumRecords(consumer, Math.min(10, numSeedMessages - recordsProcessed));

                producer.beginTransaction();
                shouldCommit.set(!shouldCommit.get());

                records.forEach(record -> {
                    var key = new String(record.key(), UTF_8);
                    var value = new String(record.value(), UTF_8);
                    producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, key, value, shouldCommit));
                });

                handler.commit(producer, consumerGroupId, consumer);

                if (shouldCommit.get()) {
                    producer.commitTransaction();
                    recordsProcessed += records.size();
                } else {
                    producer.abortTransaction();
                    resetToCommittedPositions(consumer);
                }
            }
        }

        Set<TopicPartition> partitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitions.add(new TopicPartition(TOPIC2, i));
        }

        maybeWaitForAtLeastOneSegmentUpload(cluster, partitions, isSkip);

        // In spite of random aborts, we should still have exactly 500 messages in topic2. I.e. we should not
        // re-copy or miss any messages from topic1, since the consumed offsets were committed transactionally.
        try (KafkaConsumer<byte[], byte[]> verifyingConsumer = createReadCommittedConsumer(cluster,
                "transactional-group", 100, new Properties())) {
            verifyingConsumer.subscribe(Collections.singleton(TOPIC2));
            List<String> valueList = pollUntilAtLeastNumRecords(verifyingConsumer, numSeedMessages)
                    .stream()
                    .map(TransactionTestUtils::assertCommittedAndGetValue)
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            Set<String> valueSet = new HashSet<>(valueList);

            assertEquals(numSeedMessages, valueList.size(), String.format("Expected %d values in %s.", numSeedMessages, TOPIC2));
            assertEquals(valueList.size(), valueSet.size(), String.format("Expected %d unique messages in %s.", valueList.size(), TOPIC2));
        }
    }

    // Seeds the given topic with records with keys and values in the range [0..numRecords)
    static void seedTopicWithNumberedRecords(int numRecords, ClusterInstance instance) {
        var props = new Properties();
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, instance.bootstrapServers());
        try (var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(TOPIC1, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            }
            producer.flush();
        }
    }

    static void resetToCommittedPositions(Consumer<byte[], byte[]> consumer) {
        consumer.committed(consumer.assignment()).forEach((topicPartition, offsetAndMetadata) -> {
            if (offsetAndMetadata != null) {
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            }
        });
    }

    static Map<TopicPartition, OffsetAndMetadata> consumerPositions(Consumer<byte[], byte[]> consumer) {
        HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        consumer.assignment().forEach(
                topicPartition -> offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
        );
        return offsetsToCommit;
    }

    static void testTimeout(
            ClusterInstance cluster,
            boolean needInitAndSendMsg,
            java.util.function.Consumer<KafkaProducer<byte[], byte[]>> timeoutProcess
    ) {
        var producer = createTransactionalProducer(cluster, "transactionProducer",
                2000, 3000, 4000, 1000);
        if (needInitAndSendMsg) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC1, 0, "foo".getBytes(), "bar".getBytes()));
        }
        cluster.brokers().forEach((__, broker) -> broker.shutdown());
        assertThrows(TimeoutException.class, () -> timeoutProcess.accept(producer));
        producer.close(Duration.ZERO);
    }

    static List<ConsumerRecord<byte[], byte[]>> consumeRecordsFor(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        var duration = 1000;
        var startTime = System.currentTimeMillis();
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(50)).forEach(records::add);
            return System.currentTimeMillis() - startTime > duration;
        }, "The timeout $duration was greater than the maximum wait time.");
        return records;
    }

    static void sendTransactionalMessagesWithValueRange(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            int start,
            int end,
            boolean willBeCommitted) {
        for (int i = start; i < end; i++) {
            producer.send(producerRecordWithExpectedTransactionStatus(topic, null, Integer.toString(i), Integer.toString(i), willBeCommitted));
        }
        producer.flush();
    }

    static int waitUntilLeaderIsKnown(ClusterInstance cluster, TopicPartition tp) throws InterruptedException {
        AtomicReference<Integer> leaderIfExists = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            Optional<Integer> leader = cluster.brokers().values()
                    .stream()
                    .filter(broker -> broker.replicaManager()
                            .onlinePartition(tp)
                            .exists(partition -> partition.leaderIdIfLocal().isDefined())
                    )
                    .map(broker -> broker.config().brokerId())
                    .findFirst();
            leaderIfExists.set(leader.orElse(null));
            return leader.isPresent();
        }, String.format("Partition %s leaders not made yet after %d ms", tp, 15000));
        return leaderIfExists.get();
    }

    static void restartDeadBrokers(ClusterInstance cluster) {
        cluster.brokers().forEach((__, broker) -> {
            if (broker.isShutdown()) {
                broker.startup();
            }
        });
    }

    static void testFailureToFenceEpoch(ClusterInstance cluster, boolean isTV2Enabled, Map<String, String> topicConfig) throws Exception {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "transactional-producer", 2000, 1000, 4000, 1000);
             var producer3 = createTransactionalProducer(cluster, "transactional", 2000, 5000, 4000, 1000)
        ) {
            producer1.initTransactions();
            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer1.commitTransaction();

            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(TOPIC1, 0));
            var broker = cluster.brokers().get(partitionLeader);
            var activeProducersIter = broker.logManager()
                    .getLog(new TopicPartition(TOPIC1, 0), false)
                    .get()
                    .producerStateManager()
                    .activeProducers()
                    .entrySet()
                    .iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var initialProducerEpoch = producerStateEntry.producerEpoch();

            // Kill two brokers to bring the transaction log under min-ISR
            cluster.brokers().get(0).shutdown();
            cluster.brokers().get(1).shutdown();

            assertThrows(TimeoutException.class, producer2::initTransactions);

            try {
                producer2.initTransactions();
            } catch (TimeoutException e) {
                // good!
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from initTransactions", e);
            } finally {
                producer2.close();
            }

            restartDeadBrokers(cluster);

            // Because the epoch was bumped in memory, attempting to begin a transaction with producer 1 should fail
            try {
                producer1.beginTransaction();
            } catch (ProducerFencedException e) {
                // good!
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from commitTransaction", e);
            } finally {
                producer1.close();
            }

            producer3.initTransactions();
            producer3.beginTransaction();
            producer3.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer3.commitTransaction();

            if (isTV2Enabled) {
                // Wait until the producer epoch has been updated on the broker.
                TestUtils.waitForCondition(() -> {
                    ProducerStateEntry producerStateEntry1 = broker.logManager()
                            .getLog(new TopicPartition(TOPIC1, 0), false)
                            .get()
                            .producerStateManager()
                            .activeProducers()
                            .get(producerId);
                    return producerStateEntry1 != null && producerStateEntry1.producerEpoch() == initialProducerEpoch + 1;
                }, "Timed out waiting for producer epoch to be incremented after second commit");
            } else {
                // Check that the epoch only increased by 1
                producerStateEntry = broker.logManager()
                        .getLog(new TopicPartition(TOPIC1, 0), false)
                        .get()
                        .producerStateManager()
                        .activeProducers()
                        .get(producerId);
                assertNotNull(producerStateEntry);
                assertEquals(initialProducerEpoch + 1, producerStateEntry.producerEpoch());
            }
        }
    }

    @FunctionalInterface
    interface OffsetHandler {
        void commit(Producer<byte[], byte[]> producer, String groupId, Consumer<byte[], byte[]> consumer);
    }

    private static List<ClusterConfig> generator(Properties prop, boolean isTV2Enabled) {
        return List.of(ClusterConfig.defaultBuilder()
                .setBrokers(3)
                .setTypes(Set.of(KRAFT, CO_KRAFT))
                .setServerProperties(serverConfig(prop))
                .setTags(List.of("kraftGroupCoordinator"))
                .setFeatures(isTV2Enabled ?
                        Map.of(TRANSACTION_VERSION, (short) 2) :
                        Map.of(TRANSACTION_VERSION, (short) 0)
                )
                .build());
    }

    private static Map<String, String> serverConfig(Properties overridingProps) {
        Map<String, String> prop = new HashMap<>();
        prop.put(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false");
        prop.put(OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        prop.put(TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, "3");
        prop.put(TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
        prop.put(TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "2");
        prop.put(CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true");
        prop.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        prop.put(AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false");
        prop.put(GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        prop.put(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200");
        overridingProps.forEach((k, v) -> prop.put(k.toString(), v.toString()));
        return prop;
    }

    static void maybeWaitForAtLeastOneSegmentUpload(
            ClusterInstance cluster, 
            Set<TopicPartition> topicPartitions,
            boolean isSkip
    ) {
        if (!isSkip) {
            topicPartitions.forEach(topicPartition -> {
                List<BrokerLocalStorage> localStorages = cluster.brokers().values().stream()
                        .map(b -> new BrokerLocalStorage(b.config().brokerId(), CollectionConverters.asJava(b.config().logDirs().toSet()), STORAGE_WAIT_TIMEOUT_SEC))
                        .collect(Collectors.toList());
                localStorages
                        .stream()
                        // Select brokers which are assigned a replica of the topic-partition
                        .filter(s -> isAssignedReplica(cluster, topicPartition, s.getBrokerId()))
                        // Filter out inactive brokers, which may still contain log segments we would expect
                        // to be deleted based on the retention configuration.
                        .filter(s -> isAlive(cluster, s.getBrokerId()))
                        .forEach(localStorage ->
                                // Wait until the brokers local storage have been cleared from the inactive log segments.
                                localStorage.waitForAtLeastEarliestLocalOffset(topicPartition, 1L));
            });
        }
    }

    private static boolean isAssignedReplica(
            ClusterInstance cluster,
            TopicPartition topicPartition,
            Integer replicaId
    ) {
        Optional<KafkaBroker> brokerOpt = cluster.brokers().values()
                .stream()
                .filter(b -> b.config().brokerId() == replicaId).findFirst();
        boolean isAssigned = false;
        if (brokerOpt.isPresent()) {
            HostedPartition hostedPartition = brokerOpt.get().replicaManager().getPartition(topicPartition);
            if (hostedPartition instanceof HostedPartition.Online) {
                isAssigned = true;
            }
        }
        return isAssigned;
    }

    private static boolean isAlive(ClusterInstance cluster, Integer brokerId) {
        return cluster.brokers().values().stream().anyMatch(b -> b.config().brokerId() == brokerId);
    }

    private TransactionTestUtils() {
    }
}
