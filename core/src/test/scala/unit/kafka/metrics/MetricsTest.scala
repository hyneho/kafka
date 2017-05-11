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

package kafka.metrics

import java.util.Properties

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Meter, Metric, MetricName, MetricPredicate}
import org.junit.{After, Test}
import org.junit.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.serializer._
import kafka.utils._
import kafka.admin.AdminUtils
import kafka.utils.TestUtils._

import scala.collection._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import kafka.consumer.{ConsumerConfig, ZookeeperConsumerConnector}

class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs() =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic=true).map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @After
  override def tearDown() {
    super.tearDown()
  }

  @Test
  @deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
  def testMetricsLeak() {
    // create topic topic1 with 1 partition on broker 0
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = servers)
    // force creation not client's specific metrics.
    createAndShutdownStep("group0", "consumer0", "producer0")

    //this assertion is only used for creating the metrics for DelayedFetchMetrics, it should never fail, but should not be removed
    assertNotNull(DelayedFetchMetrics)

    val countOfStaticMetrics = Metrics.defaultRegistry().allMetrics().keySet().size

    for (i <- 0 to 5) {
      createAndShutdownStep("group" + i % 3, "consumer" + i % 2, "producer" + i % 2)
      assertEquals(countOfStaticMetrics, Metrics.defaultRegistry().allMetrics().keySet().size)
    }
  }

  @Test
  def testMetricsReporterAfterDeletingTopic() {
    val topic = "test-topic-metric"
    AdminUtils.createTopic(zkUtils, topic, 1, 1)
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    assertFalse("Topic metrics exists after deleteTopic", checkTopicMetricsExists(topic))
  }

  @Test
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic() {
    val topic = "test-broker-topic-metric"
    AdminUtils.createTopic(zkUtils, topic, 2, 1)
    createAndShutdownStep("group0", "consumer0", "producer0")
    assertNotNull(BrokerTopicStats.getBrokerTopicStats(topic))
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    assertFalse("Topic metrics exists after deleteTopic", checkTopicMetricsExists(topic))
  }

  @Test
  def testClusterIdMetric(): Unit ={
    // Check if clusterId metric exists.
    val metrics = Metrics.defaultRegistry().allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName().equals("kafka.server:type=KafkaServer,name=ClusterId")), 1)
  }

  @deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
  def createAndShutdownStep(group: String, consumerId: String, producerId: String): Unit = {
    sendMessages(servers, topic, nMessages)
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumerId))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    getMessages(topicMessageStreams1, nMessages)

    zkConsumerConnector1.shutdown()
  }

  @Test
  def testBrokerTopicMetrics() {
    val replicationBytesIn = "ReplicationBytesInPerSec"
    val replicationBytesOut = "ReplicationBytesOutPerSec"
    val bytesIn = "BytesInPerSec,topic=" + topic
    val bytesOut = "BytesOutPerSec,topic=" + topic

    createTopic(zkUtils, topic, 1, numNodes, servers)
    // Topic metrics can take a moment to be created
    TestUtils.waitUntilTrue(
        () => Metrics.defaultRegistry.allMetrics.asScala.filterKeys(k => k.getMBeanName.endsWith(topic)).size > 1,
        s"Timed out while waiting the metrics for ${topic} to be created",
        3000L)

    val initialReplicationBytesIn = getMeterCount(replicationBytesIn)
    val initialReplicationBytesOut = getMeterCount(replicationBytesOut)
    val initialBytesIn = getMeterCount(bytesIn)
    val initialBytesOut = getMeterCount(bytesOut)

    // Produce a few messages to make the metrics tick
    TestUtils.produceMessages(servers, topic, nMessages)

    assertTrue(getMeterCount(replicationBytesIn) > initialReplicationBytesIn)
    assertTrue(getMeterCount(replicationBytesOut) > initialReplicationBytesOut)
    assertTrue(getMeterCount(bytesIn) > initialBytesIn)
    // BytesOut doesn't include replication, so it shouldn't have changed
    assertEquals(initialBytesOut, getMeterCount(bytesOut))

    // Consume messages to make bytesOut tick
    TestUtils.consumeMessages(servers, topic, nMessages)

    assertTrue(getMeterCount(bytesOut) > initialBytesOut)
  }

  private def getMeterCount(metricName: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala
      .filterKeys(k => k.getMBeanName.endsWith(metricName))
      .headOption
      .getOrElse(fail(s"Unable to find metric ${metricName}"))
      ._2.asInstanceOf[Meter]
      .count
  }

  private def checkTopicMetricsExists(topic: String): Boolean = {
    val topicMetricRegex = new Regex(".*("+topic+")$")
    val metricGroups = Metrics.defaultRegistry().groupedMetrics(MetricPredicate.ALL).entrySet()
    for(metricGroup <- metricGroups.asScala) {
      if (topicMetricRegex.pattern.matcher(metricGroup.getKey()).matches)
        return true
    }
    false
  }
}
