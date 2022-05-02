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

package kafka.server

import kafka.api.ApiVersion
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.utils.TestUtils
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Properties

class KafkaServerTest extends QuorumTestHarness {

  @Test
  def testAlreadyRegisteredAdvertisedListeners(): Unit = {
    //start a server with a advertised listener
    val server1 = createServer(1, "myhost", TestUtils.RandomPort)

    //start a server with same advertised listener
    assertThrows(classOf[IllegalArgumentException], () => createServer(2, "myhost", TestUtils.boundPort(server1)))

    //start a server with same host but with different port
    val server2 = createServer(2, "myhost", TestUtils.RandomPort)

    TestUtils.shutdownServers(Seq(server1, server2))
  }

  @Test
  def testCreatesProperZkConfigWhenSaslDisabled(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    assertEquals("false", zkClientConfig.getProperty(JaasUtils.ZK_SASL_CLIENT))
  }

  @Test
  def testCreatesProperZkTlsConfigWhenDisabled(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    props.put(KafkaConfig.ZkSslClientEnableProp, "false")
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach { propName =>
      assertNull(zkClientConfig.getProperty(propName))
    }
  }

  @Test
  def testCreatesProperZkTlsConfigWithTrueValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "HTTPS"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "true"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  @Test
  def testCreatesProperZkTlsConfigWithFalseAndListValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => ""
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "false"
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  @Test
  def testZkIsrManager(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "2.7-IV1")

    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    server.replicaManager.alterPartitionManager match {
      case _: ZkAlterPartitionManager =>
      case _ => fail("Should use ZK for ISR manager in versions before 2.7-IV2")
    }
    server.shutdown()
  }

  @Test
  def testAlterIsrManager(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, ApiVersion.latestVersion.toString)

    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    server.replicaManager.alterPartitionManager match {
      case _: DefaultAlterPartitionManager =>
      case _ => fail("Should use AlterIsr for ISR manager in versions after 2.7-IV2")
    }
    server.shutdown()
  }

  @Test
  def testSocketServerMetricNames(): Unit = {
    val props = TestUtils.createBrokerConfig(1, zkConnect)
    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    TestUtils.waitUntilTrue(() => BrokerState.RUNNING == server.brokerState, "Timeout waiting for KafkaServer starting")
    checkMetricNames(server.metrics, server.socketServer)
    server.shutdown()
  }

  private def checkMetricNames(metrics: Metrics, server: SocketServer): Unit = {
    assertNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}NotExists", SocketServer.MetricsGroup)))

    // SocketServer metric
    assertNotNull(metrics.sensor(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolUtilization"))
    assertNotNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolAvgDepletedPercent", SocketServer.MetricsGroup)))
    assertNotNull(metrics.metric(metrics.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolDepletedTimeTotal", SocketServer.MetricsGroup)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}${DataPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolAvailable", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}MemoryPoolUsed", Map.empty)))
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}${DataPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", Map.empty)))

    // KafkaRequestHandlerPool metric
    assertNotNull(TestUtils.metric(server.metricName(s"${DataPlaneAcceptor.KafkaServerMetricPrefix}RequestHandlerAvgIdlePercent", Map.empty)))
  }

  def createServer(nodeId: Int, hostName: String, port: Int): KafkaServer = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(KafkaConfig.AdvertisedListenersProp, s"PLAINTEXT://$hostName:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

}
