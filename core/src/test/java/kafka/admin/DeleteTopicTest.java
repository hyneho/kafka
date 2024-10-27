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
package kafka.admin;

import kafka.log.UnifiedLog;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.jdk.javaapi.OptionConverters;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"),
        @ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000")
    })
public class DeleteTopicTest {
    private static final String DEFAULT_TOPIC = "topic";
    private final Map<Integer, List<Integer>> expectedReplicaAssignment = Map.of(0, List.of(0, 1, 2));

    @ClusterTest
    public void testDeleteTopicWithAllAliveReplicas(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
        }
    }

    @ClusterTest
    public void testResumeDeleteTopicWithRecoveredFollower(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), topicPartition);
            KafkaBroker follower = findFollower(cluster.brokers().values(), leaderId);

            // shutdown one follower replica
            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            TestUtils.waitForCondition(() -> cluster.brokers().values()
                    .stream()
                    .filter(broker -> broker.config().brokerId() != follower.config().brokerId())
                    .allMatch(b -> b.logManager().getLog(topicPartition, false).isEmpty()),
                "Online replicas have not deleted log.");

            follower.startup();
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
        }
    }

    @ClusterTest(brokers = 4)
    public void testPartitionReassignmentDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            Map<Integer, KafkaBroker> servers = findPartitionHostingBrokers(cluster.brokers());
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), topicPartition);
            KafkaBroker follower = findFollower(servers.values(), leaderId);
            follower.shutdown();

            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                waitUtilTopicGone(otherAdmin);
                assertThrows(ExecutionException.class, () -> otherAdmin.alterPartitionReassignments(
                    Map.of(topicPartition, Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))))
                ).all().get());
            }

            follower.startup();
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
        }
    }

    @ClusterTest(brokers = 4)
    public void testIncreasePartitionCountDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            Map<Integer, KafkaBroker> partitionHostingBrokers = findPartitionHostingBrokers(cluster.brokers());
            waitForReplicaCreated(partitionHostingBrokers, topicPartition, "Replicas for topic test not created.");
            int leaderId = waitUtilLeaderIsKnown(partitionHostingBrokers, topicPartition);
            KafkaBroker follower = findFollower(partitionHostingBrokers.values(), leaderId);
            // shutdown a broker to make sure the following topic deletion will be suspended
            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            // increase the partition count for topic
            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                otherAdmin.createPartitions(Map.of(DEFAULT_TOPIC, NewPartitions.increaseTo(2))).all().get();
            } catch (ExecutionException ignored) {
                // do nothing
            }

            follower.startup();
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1, partitionHostingBrokers.values());
        }
    }

    @ClusterTest
    public void testDeleteTopicDuringAddPartition(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), new TopicPartition(DEFAULT_TOPIC, 0));
            TopicPartition newTopicPartition = new TopicPartition(DEFAULT_TOPIC, 1);
            KafkaBroker follower = findFollower(cluster.brokers().values(), leaderId);
            follower.shutdown();
            // wait until the broker is in shutting down state
            TestUtils.waitForCondition(() -> follower.brokerState().equals(BrokerState.SHUTTING_DOWN),
                "Follower " + follower.config().brokerId() + " was not shutdown");
            increasePartitions(admin, DEFAULT_TOPIC, 3, cluster.brokers().values().stream().filter(broker ->
                broker.config().brokerId() != follower.config().brokerId()).collect(Collectors.toList()));
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            follower.startup();
            // test if topic deletion is resumed
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
            waitForReplicaDeleted(cluster.brokers(), newTopicPartition, "Replica logs not for new partition [" + DEFAULT_TOPIC + ",1] not deleted after delete topic is complete.");
        }
    }

    @ClusterTest
    public void testAddPartitionDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            // partitions to be added to the topic later
            TopicPartition newTopicPartition = new TopicPartition(DEFAULT_TOPIC, 1);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            increasePartitions(admin, DEFAULT_TOPIC, 3, Collections.emptyList());
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
            waitForReplicaDeleted(cluster.brokers(), newTopicPartition, "Replica logs not deleted after delete topic is complete");
        }
    }

    @ClusterTest
    public void testRecreateTopicAfterDeletion(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
            // re-create topic on same replicas
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            waitForReplicaCreated(cluster.brokers(), topicPartition, "Replicas for topic " + DEFAULT_TOPIC + " not created.");
        }
    }
    @ClusterTest
    public void testDeleteNonExistingTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            String topic = "test2";
            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(topic)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause() instanceof UnknownTopicOrPartitionException;
                }
            }, "Topic test2 should not exist.");

            cluster.verifyTopicDeletion(topic, 1);

            waitForReplicaCreated(cluster.brokers(), topicPartition, "Replicas for topic test not created.");
            TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, DEFAULT_TOPIC, 0, 1000);
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "log.cleaner.enable", value = "true"),
        @ClusterConfigProperty(key = "log.cleanup.policy", value = "compact"),
        @ClusterConfigProperty(key = "log.segment.bytes", value = "100"),
        @ClusterConfigProperty(key = "log.cleaner.dedupe.buffer.size", value = "1048577")
    })
    public void testDeleteTopicWithCleaner(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            // for simplicity, we are validating cleaner offsets on a single broker
            KafkaBroker server = cluster.brokers().values().stream().findFirst().orElseThrow();
            TestUtils.waitForCondition(() -> server.logManager().getLog(topicPartition, false).isDefined(),
                "Replicas for topic test not created.");
            UnifiedLog log = server.logManager().getLog(topicPartition, false).get();
            writeDups(100, 3, log);
            // wait for cleaner to clean
            server.logManager().cleaner().awaitCleaned(topicPartition, 0, 60000);
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
        }
    }

    @ClusterTest
    public void testDeleteTopicAlreadyMarkedAsDeleted(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause() instanceof UnknownTopicOrPartitionException;
                }
            }, "Topic " + DEFAULT_TOPIC + " should be marked for deletion or already deleted.");

            cluster.verifyTopicDeletion(DEFAULT_TOPIC, 1);
        }
    }

    @ClusterTest(controllers = 1,
        serverProperties = {@ClusterConfigProperty(key = ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, value = "false")})
    public void testDisableDeleteTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(List.of(new NewTopic(DEFAULT_TOPIC, expectedReplicaAssignment))).all().get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            TestUtils.waitForCondition(() -> {
                try {
                    admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
                    return false;
                } catch (Exception exception) {
                    return exception.getCause() instanceof TopicDeletionDisabledException;
                }
            }, "TopicDeletionDisabledException should be returned when deleting " + DEFAULT_TOPIC);

            waitForReplicaCreated(cluster.brokers(), topicPartition, "TopicDeletionDisabledException should be returned when deleting " + DEFAULT_TOPIC);
            assertDoesNotThrow(() -> admin.describeTopics(List.of(DEFAULT_TOPIC)).allTopicNames().get());
            assertDoesNotThrow(() -> waitUtilLeaderIsKnown(cluster.brokers(), topicPartition));
        }
    }

    private int waitUtilLeaderIsKnown(Map<Integer, KafkaBroker> idToBroker,
                                      TopicPartition topicPartition) throws InterruptedException {
        TestUtils.waitForCondition(() -> isLeaderKnown(idToBroker, topicPartition).get().isPresent(), 15000,
            "Partition " + topicPartition + " not made yet" + " after 15 seconds");
        return isLeaderKnown(idToBroker, topicPartition).get().get();
    }

    private void waitForReplicaCreated(Map<Integer, KafkaBroker> clusters,
                                       TopicPartition topicPartition,
                                       String failMessage) throws InterruptedException {
        TestUtils.waitForCondition(() -> clusters.values().stream().allMatch(broker ->
                broker.logManager().getLog(topicPartition, false).isDefined()),
            failMessage);
    }

    private void waitForReplicaDeleted(Map<Integer, KafkaBroker> clusters,
                                       TopicPartition newTopicPartition,
                                       String failMessage) throws InterruptedException {
        TestUtils.waitForCondition(() -> clusters.values().stream().allMatch(broker ->
                broker.logManager().getLog(newTopicPartition, false).isEmpty()),
            failMessage);
    }

    private Supplier<Optional<Integer>> isLeaderKnown(Map<Integer, KafkaBroker> idToBroker, TopicPartition topicPartition) {
        return () -> idToBroker.values()
            .stream()
            .filter(broker -> OptionConverters.toJava(broker.replicaManager().onlinePartition(topicPartition))
                .stream().anyMatch(tp -> tp.leaderIdIfLocal().isDefined()))
            .map(broker -> broker.config().brokerId())
            .findFirst();
    }

    private KafkaBroker findFollower(Collection<KafkaBroker> idToBroker, int leaderId) {
        return idToBroker.stream()
            .filter(broker -> broker.config().brokerId() != leaderId)
            .findFirst()
            .orElseGet(() -> fail("Can't find any follower"));
    }

    private void waitUtilTopicGone(Admin admin) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                admin.describeTopics(List.of(DEFAULT_TOPIC)).allTopicNames().get();
                return false;
            } catch (Exception exception) {
                return exception.getCause() instanceof UnknownTopicOrPartitionException;
            }
        }, "Topic" + DEFAULT_TOPIC + " should be deleted");
    }

    private Map<Integer, KafkaBroker> findPartitionHostingBrokers(Map<Integer, KafkaBroker> brokers) {
        return brokers.entrySet()
                .stream()
                .filter(broker -> expectedReplicaAssignment.get(0).contains(broker.getValue().config().brokerId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public <B extends KafkaBroker> void increasePartitions(Admin admin,
                                                           String topic,
                                                           int totalPartitionCount,
                                                           List<B> brokersToValidate) throws Exception {
        Map<String, NewPartitions> newPartitionSet = Map.of(topic, NewPartitions.increaseTo(totalPartitionCount));
        admin.createPartitions(newPartitionSet);

        if (!brokersToValidate.isEmpty()) {
            // wait until we've propagated all partitions metadata to all brokers
            Map<TopicPartition, UpdateMetadataPartitionState> allPartitionsMetadata =
                TestUtils.waitForAllPartitionsMetadata(brokersToValidate, topic, totalPartitionCount);

            IntStream.range(0, totalPartitionCount - 1).forEach(i -> {
                Optional<UpdateMetadataPartitionState> partitionMetadata = Optional.ofNullable(allPartitionsMetadata.get(new TopicPartition(topic, i)));
                partitionMetadata.ifPresent(metadata -> assertEquals(totalPartitionCount, metadata.replicas().size()));
            });
        }
    }

    private List<int[]> writeDups(int numKeys, int numDups, UnifiedLog log) {
        int counter = 0;
        List<int[]> result = new ArrayList<>();

        for (int i = 0; i < numDups; i++) {
            for (int key = 0; key < numKeys; key++) {
                int count = counter;
                log.appendAsLeader(
                    TestUtils.singletonRecords(
                        String.valueOf(counter).getBytes(),
                        String.valueOf(key).getBytes()
                    ),
                    0,
                    AppendOrigin.CLIENT,
                    MetadataVersion.LATEST_PRODUCTION,
                    RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL
                );
                counter++;
                result.add(new int[] {key, count});
            }
        }
        return result;
    }
}