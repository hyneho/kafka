/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterInstance, ClusterTest, ClusterTestDefaults, ClusterTestExtensions, Type}
import kafka.server.GroupCoordinatorBaseRequestTest
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.extension.ExtendWith

import java.util.Collections
import java.util.Collections.singleton

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT))
class TxnOffsetCommitRequestTest(cluster:ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    )
  )
  def testTxnOffsetCommitWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testTxnOffsetCommit(true)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    )
  )
  def testTxnOffsetCommitWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testTxnOffsetCommit(false)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "false"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  ))
  def testTxnOffsetCommitWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testTxnOffsetCommit(false)
  }

  private def testTxnOffsetCommit(useNewProtocol: Boolean): Unit = {
    if (useNewProtocol && !isNewGroupCoordinatorEnabled) {
      fail("Cannot use the new protocol with the old group coordinator.")
    }

    val topic = "topic"
    val partition = 0
    val transactionalId = "txn"
    val groupId = "group"

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    joinConsumerGroup(groupId, useNewProtocol)

    var adminClient: Admin = null
    var producer: Producer[String, String] = null

    try {
      adminClient = cluster.admin()
      adminClient.createTopics(singleton(new NewTopic(topic, 1, 1.toShort)))

      producer = cluster.producer(Collections.singletonMap(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId))

      producer.initTransactions()
      producer.beginTransaction()
      // add this line to avoid error INVALID_TXN_STATE
      producer.sendOffsetsToTransaction(
        Collections.singletonMap(new TopicPartition("topic", 0), new OffsetAndMetadata(5)),
        new ConsumerGroupMetadata(groupId))

      // verify that the TXN_OFFSET_COMMIT request is processed correctly when member id is UNKNOWN_MEMBER_ID
      // and generation id is UNKNOWN_GENERATION_ID under all api versions
      for (version <- 0 to ApiKeys.TXN_OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
        commitTxnOffset(
          groupId = groupId,
          memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
          generationId = JoinGroupRequest.UNKNOWN_GENERATION_ID,
          producerId = 0,
          producerEpoch = 0.toShort,
          transactionalId = transactionalId,
          topic = topic,
          partition = partition,
          offset = 100L,
          expectedError = Errors.NONE,
          version = version.toShort,
        )
      }
    } finally {
      adminClient.close()
      producer.close()
    }
  }
}
