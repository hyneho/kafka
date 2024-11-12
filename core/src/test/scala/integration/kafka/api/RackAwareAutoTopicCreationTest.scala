/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import java.util.Properties
import kafka.admin.RackAwareTest
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createAdminClient
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.server.config.{ReplicationConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.concurrent.TimeUnit
import scala.collection.Map
import scala.util.Using
import scala.jdk.CollectionConverters._

class RackAwareAutoTopicCreationTest extends KafkaServerTestHarness with RackAwareTest {
  val numServers = 4
  val numPartitions = 8
  val replicationFactor = 2
  val overridingProps = new Properties()
  overridingProps.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numPartitions.toString)
  overridingProps.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, replicationFactor.toString)

  def generateConfigs =
    (0 until numServers) map { node =>
      TestUtils.createBrokerConfig(node, null, enableControlledShutdown = false, rack = Some((node / 2).toString))
    } map (KafkaConfig.fromProps(_, overridingProps))

  private val topic = "topic"

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAutoCreateTopic(quorum: String): Unit = {
    val producer = TestUtils.createProducer(bootstrapServers())
    try {
      // Wait for all brokers to be unfenced.
      TestUtils.retry(50_000) {
        assertEquals(numServers, brokers.head.metadataCache.getAliveBrokers().size)
      }
      TestUtils.ensureConsistentKRaftMetadata(brokers, controllerServer)

      // Send a message to auto-create the topic
      val record = new ProducerRecord(topic, null, "key".getBytes, "value".getBytes)
      assertEquals(0L, producer.send(record).get.offset, "Should have offset 0")

      var assignment: Map[Int, Seq[Int]] = null
      TestUtils.retry(50_000) {
        var allTopics: java.util.Map[String, TopicDescription] = null
        Using(createAdminClient(brokers, listenerName)) { admin =>
          allTopics = admin.describeTopics(java.util.Arrays.asList(topic)).
            allTopicNames().get(2, TimeUnit.MINUTES)
        }
        val topicDescription = allTopics.get(topic)
        assertNotNull(topicDescription)
        assertEquals(numPartitions, topicDescription.partitions().size())
        assignment = topicDescription.partitions().asScala.map(p => {
          p.partition() -> p.replicas.asScala.map(_.id()).toSeq
        }).toMap
      }
      val brokerMetadatas = brokers.head.metadataCache.getAliveBrokers()
      val expectedMap = Map(0 -> "0", 1 -> "0", 2 -> "1", 3 -> "1")
      assertEquals(expectedMap, brokerMetadatas.map(b => b.id -> b.rack.get).toMap)
      checkReplicaDistribution(assignment, expectedMap, numServers, numPartitions, replicationFactor, verifyLeaderDistribution = false)
    } finally producer.close()
  }
}

