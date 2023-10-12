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
package kafka.server

import org.apache.kafka.common.{DirectoryId, Node, TopicPartition, Uuid}

import java.util
import java.util.Arrays.asList
import java.util.Collections
import kafka.api.LeaderAndIsr
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState, UpdateMetadataTopicState}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{AbstractControlRequest, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.metadata.{BrokerRegistrationChangeRecord, PartitionRecord, RegisterBrokerRecord, RemoveTopicRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.image.{ClusterImage, MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.api.Test

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

object MetadataCacheTest {
  def zkCacheProvider(): util.stream.Stream[MetadataCache] =
    util.stream.Stream.of[MetadataCache](
      MetadataCache.zkMetadataCache(1, MetadataVersion.latest())
    )

  def cacheProvider(): util.stream.Stream[MetadataCache] =
    util.stream.Stream.of[MetadataCache](
      MetadataCache.zkMetadataCache(1, MetadataVersion.latest()),
      MetadataCache.kRaftMetadataCache(1)
    )

  def updateCache(cache: MetadataCache, request: UpdateMetadataRequest, records: Seq[ApiMessage] = List()): Unit = {
    cache match {
      case c: ZkMetadataCache => c.updateMetadata(0, request)
      case c: KRaftMetadataCache => {
        // UpdateMetadataRequest always contains a full list of brokers, but may contain
        // a partial list of partitions. Therefore, base our delta off a partial image that
        // contains no brokers, but which contains the previous partitions.
        val image = c.currentImage()
        val partialImage = new MetadataImage(
          new MetadataProvenance(100L, 10, 1000L),
          image.features(),
          ClusterImage.EMPTY,
          image.topics(),
          image.configs(),
          image.clientQuotas(),
          image.producerIds(),
          image.acls(),
          image.scram(),
          image.delegationTokens())
        val delta = new MetadataDelta.Builder().setImage(partialImage).build()

        def toRecord(broker: UpdateMetadataBroker): RegisterBrokerRecord = {
          val endpoints = new BrokerEndpointCollection()
          broker.endpoints().forEach { e =>
            endpoints.add(new BrokerEndpoint().
              setName(e.listener()).
              setHost(e.host()).
              setPort(e.port()).
              setSecurityProtocol(e.securityProtocol()))
          }
          val prevBroker = Option(image.cluster().broker(broker.id()))
          // UpdateMetadataRequest doesn't contain all the broker registration fields, so get
          // them from the previous registration if available.
          val (epoch, incarnationId, fenced) = prevBroker match {
            case None => (0L, Uuid.ZERO_UUID, false)
            case Some(b) => (b.epoch(), b.incarnationId(), b.fenced())
          }
          new RegisterBrokerRecord().
            setBrokerId(broker.id()).
            setBrokerEpoch(epoch).
            setIncarnationId(incarnationId).
            setEndPoints(endpoints).
            setRack(broker.rack()).
            setFenced(fenced)
        }
        request.liveBrokers().iterator().asScala.foreach { brokerInfo =>
          delta.replay(toRecord(brokerInfo))
        }

        def toRecords(topic: UpdateMetadataTopicState): Seq[ApiMessage] = {
          val results = new mutable.ArrayBuffer[ApiMessage]()
          results += new TopicRecord().setName(topic.topicName()).setTopicId(topic.topicId())
          topic.partitionStates().forEach { partition =>
            if (partition.leader() == LeaderAndIsr.LeaderDuringDelete) {
              results += new RemoveTopicRecord().setTopicId(topic.topicId())
            } else {
              results += new PartitionRecord().
                setPartitionId(partition.partitionIndex()).
                setTopicId(topic.topicId()).
                setReplicas(partition.replicas()).
                setIsr(partition.isr()).
                setRemovingReplicas(Collections.emptyList()).
                setAddingReplicas(Collections.emptyList()).
                setLeader(partition.leader()).
                setLeaderEpoch(partition.leaderEpoch()).
                setPartitionEpoch(partition.zkVersion())
            }
          }
          results
        }
        request.topicStates().forEach { topic =>
          toRecords(topic).foreach(delta.replay)
        }
        records.foreach(delta.replay)
        c.setImage(delta.apply(new MetadataProvenance(100L, 10, 1000L)))
      }
      case _ => throw new RuntimeException("Unsupported cache type")
    }
  }
}

class MetadataCacheTest {
  val brokerEpoch = 0L

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataNonExistingTopics(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertTrue(topicMetadata.isEmpty)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadata(cache: MetadataCache): Unit = {
    val topic0 = "topic-0"
    val topic1 = "topic-1"

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    def endpoints(brokerId: Int): Seq[UpdateMetadataEndpoint] = {
      val host = s"foo-$brokerId"
      Seq(
        new UpdateMetadataEndpoint()
          .setHost(host)
          .setPort(9092)
          .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
          .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value),
        new UpdateMetadataEndpoint()
          .setHost(host)
          .setPort(9093)
          .setSecurityProtocol(SecurityProtocol.SSL.id)
          .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.SSL).value)
      )
    }

    val brokers = (0 to 4).map { brokerId =>
      new UpdateMetadataBroker()
        .setId(brokerId)
        .setEndpoints(endpoints(brokerId).asJava)
        .setRack("rack1")
    }

    val partitionStates = Seq(
      new UpdateMetadataPartitionState()
        .setTopicName(topic0)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setIsr(asList(0, 1, 3))
        .setZkVersion(zkVersion)
        .setReplicas(asList(0, 1, 3)),
      new UpdateMetadataPartitionState()
        .setTopicName(topic0)
        .setPartitionIndex(1)
        .setControllerEpoch(controllerEpoch)
        .setLeader(1)
        .setLeaderEpoch(1)
        .setIsr(asList(1, 0))
        .setZkVersion(zkVersion)
        .setReplicas(asList(1, 2, 0, 4)),
      new UpdateMetadataPartitionState()
        .setTopicName(topic1)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(2)
        .setLeaderEpoch(2)
        .setIsr(asList(2, 1))
        .setZkVersion(zkVersion)
        .setReplicas(asList(2, 1, 3)))

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(topic0, Uuid.randomUuid())
    topicIds.put(topic1, Uuid.randomUuid())

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, topicIds).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    for (securityProtocol <- Seq(SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL)) {
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

      def checkTopicMetadata(topic: String): Unit = {
        val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName)
        assertEquals(1, topicMetadatas.size)

        val topicMetadata = topicMetadatas.head
        assertEquals(Errors.NONE.code, topicMetadata.errorCode)
        assertEquals(topic, topicMetadata.name)
        assertEquals(topicIds.get(topic), topicMetadata.topicId())

        val topicPartitionStates = partitionStates.filter { ps => ps.topicName == topic }
        val partitionMetadatas = topicMetadata.partitions.asScala.sortBy(_.partitionIndex)
        assertEquals(topicPartitionStates.size, partitionMetadatas.size, s"Unexpected partition count for topic $topic")

        partitionMetadatas.zipWithIndex.foreach { case (partitionMetadata, partitionId) =>
          assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
          assertEquals(partitionId, partitionMetadata.partitionIndex)
          val partitionState = topicPartitionStates.find(_.partitionIndex == partitionId).getOrElse(
            fail(s"Unable to find partition state for partition $partitionId"))
          assertEquals(partitionState.leader, partitionMetadata.leaderId)
          assertEquals(partitionState.leaderEpoch, partitionMetadata.leaderEpoch)
          assertEquals(partitionState.isr, partitionMetadata.isrNodes)
          assertEquals(partitionState.replicas, partitionMetadata.replicaNodes)
        }
      }

      checkTopicMetadata(topic0)
      checkTopicMetadata(topic1)
    }

  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataPartitionLeaderNotAvailable(cache: MetadataCache): Unit = {
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))
    val metadataCacheBrokerId = 0
    // leader is not available. expect LEADER_NOT_AVAILABLE for any metadata version.
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, metadataCacheBrokerId, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, metadataCacheBrokerId, brokers, listenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = true)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataPartitionListenerNotAvailableOnLeader(cache: MetadataCache): Unit = {
    // when listener name is not present in the metadata cache for a broker, getTopicMetadata should
    // return LEADER_NOT_AVAILABLE or LISTENER_NOT_FOUND errors for old and new versions respectively.
    val plaintextListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val sslListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL)
    val broker0Endpoints = Seq(
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setListener(plaintextListenerName.value),
      new UpdateMetadataEndpoint()
        .setHost("host0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.SSL.id)
        .setListener(sslListenerName.value))
    val broker1Endpoints = Seq(new UpdateMetadataEndpoint()
      .setHost("host1")
      .setPort(9092)
      .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
      .setListener(plaintextListenerName.value))
    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setEndpoints(broker0Endpoints.asJava),
      new UpdateMetadataBroker()
        .setId(1)
        .setEndpoints(broker1Endpoints.asJava))
    val metadataCacheBrokerId = 0
    // leader available in cache but listener name not present. expect LISTENER_NOT_FOUND error for new metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, metadataCacheBrokerId, brokers, sslListenerName,
      leader = 1, Errors.LISTENER_NOT_FOUND, errorUnavailableListeners = true)
    // leader available in cache but listener name not present. expect LEADER_NOT_AVAILABLE error for old metadata version
    verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache, metadataCacheBrokerId, brokers, sslListenerName,
      leader = 1, Errors.LEADER_NOT_AVAILABLE, errorUnavailableListeners = false)
  }

  private def verifyTopicMetadataPartitionLeaderOrEndpointNotAvailable(cache: MetadataCache,
                                                                       metadataCacheBrokerId: Int,
                                                                       brokers: Seq[UpdateMetadataBroker],
                                                                       listenerName: ListenerName,
                                                                       leader: Int,
                                                                       expectedError: Errors,
                                                                       errorUnavailableListeners: Boolean): Unit = {
    val topic = "topic"

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1

    val leaderEpoch = 1
    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(asList(0))
      .setZkVersion(zkVersion)
      .setReplicas(asList(0)))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, util.Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableListeners = errorUnavailableListeners)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code, topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(expectedError.code, partitionMetadata.errorCode)
    assertFalse(partitionMetadata.isrNodes.isEmpty)
    assertEquals(List(0), partitionMetadata.replicaNodes.asScala)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataReplicaNotAvailable(cache: MetadataCache): Unit = {
    val topic = "topic"

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0, 1)
    val isr = asList[Integer](0)

    val partitionStates = Seq(
      new UpdateMetadataPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(zkVersion)
        .setReplicas(replicas))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, util.Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code(), topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
    assertEquals(Set(0, 1), partitionMetadata.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadata.isrNodes.asScala.toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE.code, topicMetadataWithError.errorCode)

    val partitionMetadatasWithError = topicMetadataWithError.partitions()
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, partitionMetadataWithError.errorCode)
    assertEquals(Set(0), partitionMetadataWithError.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadataWithError.isrNodes.asScala.toSet)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataIsrNotAvailable(cache: MetadataCache): Unit = {
    val topic = "topic"

    val zkVersion = 3
    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setRack("rack1")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))

    // replica 1 is not available
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)

    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(zkVersion)
      .setReplicas(replicas))

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, util.Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    // Validate errorUnavailableEndpoints = false
    val topicMetadatas = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = false)
    assertEquals(1, topicMetadatas.size)

    val topicMetadata = topicMetadatas.head
    assertEquals(Errors.NONE.code(), topicMetadata.errorCode)

    val partitionMetadatas = topicMetadata.partitions
    assertEquals(1, partitionMetadatas.size)

    val partitionMetadata = partitionMetadatas.get(0)
    assertEquals(0, partitionMetadata.partitionIndex)
    assertEquals(Errors.NONE.code, partitionMetadata.errorCode)
    assertEquals(Set(0), partitionMetadata.replicaNodes.asScala.toSet)
    assertEquals(Set(0, 1), partitionMetadata.isrNodes.asScala.toSet)

    // Validate errorUnavailableEndpoints = true
    val topicMetadatasWithError = cache.getTopicMetadata(Set(topic), listenerName, errorUnavailableEndpoints = true)
    assertEquals(1, topicMetadatasWithError.size)

    val topicMetadataWithError = topicMetadatasWithError.head
    assertEquals(Errors.NONE.code, topicMetadataWithError.errorCode)

    val partitionMetadatasWithError = topicMetadataWithError.partitions
    assertEquals(1, partitionMetadatasWithError.size)

    val partitionMetadataWithError = partitionMetadatasWithError.get(0)
    assertEquals(0, partitionMetadataWithError.partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, partitionMetadataWithError.errorCode)
    assertEquals(Set(0), partitionMetadataWithError.replicaNodes.asScala.toSet)
    assertEquals(Set(0), partitionMetadataWithError.isrNodes.asScala.toSet)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getTopicMetadataWithNonSupportedSecurityProtocol(cache: MetadataCache): Unit = {
    val topic = "topic"
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setRack("")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(ListenerName.forSecurityProtocol(securityProtocol).value)).asJava))
    val controllerEpoch = 1
    val leader = 0
    val leaderEpoch = 0
    val replicas = asList[Integer](0)
    val isr = asList[Integer](0, 1)
    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(0)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(3)
      .setReplicas(replicas))
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, brokerEpoch, partitionStates.asJava,
      brokers.asJava, util.Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    val topicMetadata = cache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals(1, topicMetadata.size)
    assertEquals(1, topicMetadata.head.partitions.size)
    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, topicMetadata.head.partitions.get(0).leaderId)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def getAliveBrokersShouldNotBeMutatedByUpdateCache(cache: MetadataCache): Unit = {
    val topic = "topic"

    def updateCache(brokerIds: Seq[Int]): Unit = {
      val brokers = brokerIds.map { brokerId =>
        val securityProtocol = SecurityProtocol.PLAINTEXT
        new UpdateMetadataBroker()
          .setId(brokerId)
          .setRack("")
          .setEndpoints(Seq(new UpdateMetadataEndpoint()
            .setHost("foo")
            .setPort(9092)
            .setSecurityProtocol(securityProtocol.id)
            .setListener(ListenerName.forSecurityProtocol(securityProtocol).value)).asJava)
      }
      val controllerEpoch = 1
      val leader = 0
      val leaderEpoch = 0
      val replicas = asList[Integer](0)
      val isr = asList[Integer](0, 1)
      val partitionStates = Seq(new UpdateMetadataPartitionState()
        .setTopicName(topic)
        .setPartitionIndex(0)
        .setControllerEpoch(controllerEpoch)
        .setLeader(leader)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(3)
        .setReplicas(replicas))
      val version = ApiKeys.UPDATE_METADATA.latestVersion
      val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, brokerEpoch, partitionStates.asJava,
        brokers.asJava, util.Collections.emptyMap()).build()
      MetadataCacheTest.updateCache(cache, updateMetadataRequest)
    }

    val initialBrokerIds = (0 to 2)
    updateCache(initialBrokerIds)
    val aliveBrokersFromCache = cache.getAliveBrokers()
    // This should not change `aliveBrokersFromCache`
    updateCache((0 to 3))
    assertEquals(initialBrokerIds.toSet, aliveBrokersFromCache.map(_.id).toSet)
  }

  // This test runs only for the ZK cache, because KRaft mode doesn't support offline
  // replicas yet. TODO: implement KAFKA-13005.
  @ParameterizedTest
  @MethodSource(Array("zkCacheProvider"))
  def testGetClusterMetadataWithOfflineReplicas(cache: MetadataCache): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)

    val brokers = Seq(
      new UpdateMetadataBroker()
        .setId(0)
        .setRack("r")
        .setEndpoints(Seq(new UpdateMetadataEndpoint()
          .setHost("foo")
          .setPort(9092)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value)).asJava),
      new UpdateMetadataBroker()
        .setId(1)
        .setEndpoints(Seq.empty.asJava)
    )
    val controllerEpoch = 1
    val leader = 1
    val leaderEpoch = 0
    val replicas = asList[Integer](0, 1)
    val isr = asList[Integer](0, 1)
    val offline = asList[Integer](1)
    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(topicPartition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(3)
      .setReplicas(replicas)
      .setOfflineReplicas(offline))
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, 2, controllerEpoch, brokerEpoch, partitionStates.asJava,
      brokers.asJava, Collections.emptyMap()).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    val expectedNode0 = new Node(0, "foo", 9092, "r")
    val expectedNode1 = new Node(1, "", -1)

    val cluster = cache.getClusterMetadata("clusterId", listenerName)
    assertEquals(expectedNode0, cluster.nodeById(0))
    assertNull(cluster.nodeById(1))
    assertEquals(expectedNode1, cluster.leaderFor(topicPartition))

    val partitionInfo = cluster.partition(topicPartition)
    assertEquals(expectedNode1, partitionInfo.leader)
    assertEquals(Seq(expectedNode0, expectedNode1), partitionInfo.replicas.toSeq)
    assertEquals(Seq(expectedNode0, expectedNode1), partitionInfo.inSyncReplicas.toSeq)
    assertEquals(Seq(expectedNode1), partitionInfo.offlineReplicas.toSeq)
  }

  @Test
  def testIsBrokerFenced(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setFenced(false))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertFalse(metadataCache.isBrokerFenced(0))

    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(0)
      .setFenced(1.toByte))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertTrue(metadataCache.isBrokerFenced(0))
  }

  @Test
  def testGetAliveBrokersWithBrokerFenced(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0)
    val listenerName = "listener"
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(new BrokerEndpoint().
      setName(listenerName).
      setHost("foo").
      setPort(123).
      setSecurityProtocol(0))
    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setFenced(false)
      .setEndPoints(endpoints))
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(1)
      .setFenced(false)
      .setEndPoints(endpoints))
    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(1)
      .setFenced(1.toByte))

    val metadataImage = delta.apply(MetadataProvenance.EMPTY)

    metadataCache.setImage(metadataImage)
    assertFalse(metadataCache.isBrokerFenced(0))
    assertTrue(metadataCache.isBrokerFenced(1))

    val aliveBrokers = metadataCache.getAliveBrokers().map(_.id).toSet
    metadataImage.cluster().brokers().forEach { (brokerId, registration) =>
      assertEquals(!registration.fenced(), aliveBrokers.contains(brokerId))
      assertEquals(aliveBrokers.contains(brokerId), metadataCache.getAliveBrokerNode(brokerId, new ListenerName(listenerName)).isDefined)
    }
  }

  @Test
  def testIsBrokerInControlledShutdown(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setInControlledShutdown(false))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertFalse(metadataCache.isBrokerShuttingDown(0))

    delta.replay(new BrokerRegistrationChangeRecord()
      .setBrokerId(0)
      .setInControlledShutdown(1.toByte))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertTrue(metadataCache.isBrokerShuttingDown(0))
  }

  @Test
  def testGetLiveBrokerEpoch(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0)

    val delta = new MetadataDelta.Builder().build()
    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(0)
      .setBrokerEpoch(100)
      .setFenced(false))

    delta.replay(new RegisterBrokerRecord()
      .setBrokerId(1)
      .setBrokerEpoch(101)
      .setFenced(true))

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    assertEquals(100L, metadataCache.getAliveBrokerEpoch(0).getOrElse(-1L))
    assertEquals(-1L, metadataCache.getAliveBrokerEpoch(1).getOrElse(-1L))
  }

  @Test
  def testGetTopicMetadataForDescribeTopicResponse(): Unit = {
    val metadataCache = MetadataCache.kRaftMetadataCache(0)

    val controllerId = 2
    val controllerEpoch = 1
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val topic0 = "test0"
    val topic1 = "test1"

    val topicIds = new util.HashMap[String, Uuid]()
    topicIds.put(topic0, Uuid.randomUuid())
    topicIds.put(topic1, Uuid.randomUuid())

    val partitionMap = Map[(String, Int), PartitionRecord](
      (topic0, 0) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(0)
        .setReplicas(asList(0, 1, 2))
        .setLeader(0)
        .setIsr(asList(0))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownELR(asList(2))
        .setLeaderEpoch(0)
        .setPartitionEpoch(1)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic0, 1) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(1)
        .setReplicas(asList(0, 1, 3))
        .setLeader(0)
        .setIsr(asList(0))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownELR(asList(3))
        .setLeaderEpoch(0)
        .setPartitionEpoch(2)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic0, 2) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic0))
        .setPartitionId(2)
        .setReplicas(asList(0, 2, 3))
        .setLeader(3)
        .setIsr(asList(3))
        .setEligibleLeaderReplicas(asList(2))
        .setLastKnownELR(asList(0))
        .setLeaderEpoch(1)
        .setPartitionEpoch(2)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
      (topic1, 0) -> new PartitionRecord()
        .setTopicId(topicIds.get(topic1))
        .setPartitionId(0)
        .setReplicas(asList(0, 1, 2))
        .setLeader(2)
        .setIsr(asList(2))
        .setEligibleLeaderReplicas(asList(1))
        .setLastKnownELR(asList(0))
        .setLeaderEpoch(10)
        .setPartitionEpoch(11)
        .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()),
    )

    val brokers = Seq(
      new UpdateMetadataBroker().setId(0).setEndpoints(Seq(new UpdateMetadataEndpoint().setHost("foo0").setPort(9092).setSecurityProtocol(securityProtocol.id).setListener(listenerName.value)).asJava),
      new UpdateMetadataBroker().setId(1).setEndpoints(Seq(new UpdateMetadataEndpoint().setHost("foo1").setPort(9093).setSecurityProtocol(securityProtocol.id).setListener(listenerName.value)).asJava),
      new UpdateMetadataBroker().setId(2).setEndpoints(Seq(new UpdateMetadataEndpoint().setHost("foo2").setPort(9094).setSecurityProtocol(securityProtocol.id).setListener(listenerName.value)).asJava),
      new UpdateMetadataBroker().setId(3).setEndpoints(Seq(new UpdateMetadataEndpoint().setHost("foo3").setPort(9095).setSecurityProtocol(securityProtocol.id).setListener(listenerName.value)).asJava),
    )

    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      List[UpdateMetadataPartitionState]().asJava, brokers.asJava, topicIds).build()
    var recordSeq = Seq[ApiMessage](
      new TopicRecord().setName(topic0).setTopicId(topicIds.get(topic0)),
      new TopicRecord().setName(topic1).setTopicId(topicIds.get(topic1))
    )
    recordSeq = recordSeq.appendedAll(partitionMap.values.toSeq)
    MetadataCacheTest.updateCache(metadataCache, updateMetadataRequest, recordSeq)

    def checkTopicMetadata(topic: String, partitionIds: Set[Int], partitions: mutable.Buffer[DescribeTopicsResponsePartition]): Unit = {
      partitions.foreach(partition => {
        val partitionId = partition.partitionIndex()
        assertTrue(partitionIds.contains(partitionId))
        val expectedPartition = partitionMap.get((topic, partitionId)).get
        assertEquals(0, partition.errorCode())
        assertEquals(expectedPartition.leaderEpoch(), partition.leaderEpoch())
        assertEquals(expectedPartition.partitionId(), partition.partitionIndex())
        assertEquals(expectedPartition.eligibleLeaderReplicas(), partition.eligibleLeaderReplicas())
        assertEquals(expectedPartition.isr(), partition.isrNodes())
        assertEquals(expectedPartition.lastKnownELR(), partition.lastKnownELR())
        assertEquals(expectedPartition.leader(), partition.leaderId())
      })
    }

    // Basic test
    var result = metadataCache.getTopicMetadataForDescribeTopicResponse(Set((topic0, 0), (topic1, 0)), listenerName, 10).toList
    assertEquals(2, result.size)
    result.foreach(responseTopic => {
      if (responseTopic.name() == topic0) {
        assertEquals(0, responseTopic.errorCode())
        assertEquals(topicIds.get(topic0), responseTopic.topicId())
        assertEquals(3, responseTopic.partitions().size())
        checkTopicMetadata(topic0, Set(0, 1, 2), responseTopic.partitions().asScala)
      } else {
        assertEquals(0, responseTopic.errorCode())
        assertEquals(topicIds.get(topic1), responseTopic.topicId())
        assertEquals(1, responseTopic.partitions().size())
        checkTopicMetadata(topic1, Set(0), responseTopic.partitions().asScala)
      }
    })

    // Quota reached
    result = metadataCache.getTopicMetadataForDescribeTopicResponse(Set((topic0, 0), (topic1, 0)), listenerName, 2).toList
    assertEquals(2, result.size)
    result.foreach(responseTopic => {
      if (responseTopic.name() == topic0) {
        assertEquals(0, responseTopic.errorCode())
        assertEquals(topicIds.get(topic0), responseTopic.topicId())
        assertEquals(2, responseTopic.partitions().size())
        checkTopicMetadata(topic0, Set(0, 1), responseTopic.partitions().asScala)
      } else {
        assertEquals(117, responseTopic.errorCode())
        assertEquals(topicIds.get(topic1), responseTopic.topicId())
        assertEquals(0, responseTopic.partitions().size())
      }
    })

    // With start index
    result = metadataCache.getTopicMetadataForDescribeTopicResponse(Set((topic0, 1)), listenerName, 10).toList
    assertEquals(1, result.size)
    var responseTopic = result.apply(0)
    assertEquals(0, responseTopic.errorCode())
    assertEquals(topicIds.get(topic0), responseTopic.topicId())
    assertEquals(topic0, responseTopic.name())
    assertEquals(2, responseTopic.partitions().size())
    checkTopicMetadata(topic0, Set(1, 2), responseTopic.partitions().asScala)

    // With start index and quota reached
    result = metadataCache.getTopicMetadataForDescribeTopicResponse(Set((topic0, 1)), listenerName, 1).toList
    assertEquals(1, result.size)
    responseTopic = result.apply(0)
    assertEquals(0, responseTopic.errorCode())
    assertEquals(topicIds.get(topic0), responseTopic.topicId())
    assertEquals(topic0, responseTopic.name())
    assertEquals(1, responseTopic.partitions().size())
    assertEquals(2, responseTopic.nextPartition())
    checkTopicMetadata(topic0, Set(1), responseTopic.partitions().asScala)
  }

  @ParameterizedTest
  @MethodSource(Array("cacheProvider"))
  def testGetPartitionInfo(cache: MetadataCache): Unit = {
    val topic = "topic"
    val partitionIndex = 0
    val controllerEpoch = 1
    val leader = 0
    val leaderEpoch = 0
    val isr = asList[Integer](2, 3, 0)
    val zkVersion = 3
    val replicas = asList[Integer](2, 3, 0, 1, 4)
    val offlineReplicas = asList[Integer](0)

    val partitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partitionIndex)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(zkVersion)
      .setReplicas(replicas)
      .setOfflineReplicas(offlineReplicas))

    val version = ApiKeys.UPDATE_METADATA.latestVersion

    val controllerId = 2
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = Seq(new UpdateMetadataBroker()
      .setId(0)
      .setRack("rack1")
      .setEndpoints(Seq(new UpdateMetadataEndpoint()
        .setHost("foo")
        .setPort(9092)
        .setSecurityProtocol(securityProtocol.id)
        .setListener(listenerName.value)).asJava))
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitionStates.asJava, brokers.asJava, util.Collections.emptyMap(), false, AbstractControlRequest.Type.UNKNOWN).build()
    MetadataCacheTest.updateCache(cache, updateMetadataRequest)

    val partitionState = cache.getPartitionInfo(topic, partitionIndex).get
    assertEquals(topic, partitionState.topicName())
    assertEquals(partitionIndex, partitionState.partitionIndex())
    if (cache.isInstanceOf[ZkMetadataCache]) {
      assertEquals(controllerEpoch, partitionState.controllerEpoch())
    } else {
      assertEquals(-1, partitionState.controllerEpoch())
    }
    assertEquals(leader, partitionState.leader())
    assertEquals(leaderEpoch, partitionState.leaderEpoch())
    assertEquals(isr, partitionState.isr())
    assertEquals(zkVersion, partitionState.zkVersion())
    assertEquals(replicas, partitionState.replicas())
    if (cache.isInstanceOf[ZkMetadataCache]) {
      assertEquals(offlineReplicas, partitionState.offlineReplicas())
    }
  }

  def setupInitialAndFullMetadata(): (
    Map[String, Uuid], mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
    Map[String, Uuid], Seq[UpdateMetadataPartitionState]
  ) = {
    def addTopic(
      name: String,
      partitions: Int,
      topicStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]]
    ): Unit = {
      val partitionMap = mutable.LongMap.empty[UpdateMetadataPartitionState]
      for (i <- 0 until partitions) {
        partitionMap.put(i, new UpdateMetadataPartitionState()
          .setTopicName(name)
          .setPartitionIndex(i)
          .setControllerEpoch(2)
          .setLeader(0)
          .setLeaderEpoch(10)
          .setIsr(asList(0, 1))
          .setZkVersion(10)
          .setReplicas(asList(0, 1, 2)))
      }
      topicStates.put(name, partitionMap)
    }

    val initialTopicStates = mutable.AnyRefMap.empty[String, mutable.LongMap[UpdateMetadataPartitionState]]
    addTopic("test-topic-1", 3, initialTopicStates)
    addTopic("test-topic-2", 3, initialTopicStates)

    val initialTopicIds = Map(
      "test-topic-1" -> Uuid.fromString("IQ2F1tpCRoSbjfq4zBJwpg"),
      "test-topic-2" -> Uuid.fromString("4N8_J-q7SdWHPFkos275pQ")
    )

    val newTopicIds = Map(
      "different-topic" -> Uuid.fromString("DraFMNOJQOC5maTb1vtZ8Q")
    )

    val newPartitionStates = Seq(new UpdateMetadataPartitionState()
      .setTopicName("different-topic")
      .setPartitionIndex(0)
      .setControllerEpoch(42)
      .setLeader(0)
      .setLeaderEpoch(10)
      .setIsr(asList[Integer](0, 1, 2))
      .setZkVersion(1)
      .setReplicas(asList[Integer](0, 1, 2)))

    (initialTopicIds, initialTopicStates, newTopicIds, newPartitionStates)
  }

  /**
   * Verify that ZkMetadataCache#maybeInjectDeletedPartitionsFromFullMetadataRequest correctly
   * generates deleted topic partition state when deleted topics are detected. This does not check
   * any of the logic about when this method should be called, only that it does the correct thing
   * when called.
   */
  @Test
  def testMaybeInjectDeletedPartitionsFromFullMetadataRequest(): Unit = {
    val (initialTopicIds, initialTopicStates, newTopicIds, _) = setupInitialAndFullMetadata()

    val initialSnapshot = MetadataSnapshot(
      partitionStates = initialTopicStates,
      topicIds = initialTopicIds,
      controllerId = Some(KRaftCachedControllerId(3000)),
      aliveBrokers = mutable.LongMap.empty,
      aliveNodes = mutable.LongMap.empty)

    def verifyTopicStates(
      updateMetadataRequest: UpdateMetadataRequest
    )(
      verifier: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]] => Unit
    ): Unit  = {
      val finalTopicStates = mutable.AnyRefMap.empty[String, mutable.LongMap[UpdateMetadataPartitionState]]
      updateMetadataRequest.topicStates().forEach { topicState =>
        finalTopicStates.put(topicState.topicName(), mutable.LongMap.empty[UpdateMetadataPartitionState])
        topicState.partitionStates().forEach { partitionState =>
          finalTopicStates(topicState.topicName()).put(partitionState.partitionIndex(), partitionState)
        }
      }
      verifier.apply(finalTopicStates)
    }

    // Empty UMR, deletes everything
    var updateMetadataRequest = new UpdateMetadataRequest.Builder(8, 1, 42, brokerEpoch,
      Seq.empty.asJava, Seq.empty.asJava, Map.empty[String, Uuid].asJava, true, AbstractControlRequest.Type.FULL).build()
    assertEquals(
      Seq(Uuid.fromString("IQ2F1tpCRoSbjfq4zBJwpg"), Uuid.fromString("4N8_J-q7SdWHPFkos275pQ")),
      ZkMetadataCache.maybeInjectDeletedPartitionsFromFullMetadataRequest(
        initialSnapshot, 42, updateMetadataRequest.topicStates())
    )
    verifyTopicStates(updateMetadataRequest) { topicStates =>
      assertEquals(2, topicStates.size)
      assertEquals(3, topicStates("test-topic-1").values.toSeq.count(_.leader() == -2))
      assertEquals(3, topicStates("test-topic-2").values.toSeq.count(_.leader() == -2))
    }

    // One different topic, should remove other two
    val oneTopicPartitionState = Seq(new UpdateMetadataPartitionState()
      .setTopicName("different-topic")
      .setPartitionIndex(0)
      .setControllerEpoch(42)
      .setLeader(0)
      .setLeaderEpoch(10)
      .setIsr(asList[Integer](0, 1, 2))
      .setZkVersion(1)
      .setReplicas(asList[Integer](0, 1, 2)))
    updateMetadataRequest = new UpdateMetadataRequest.Builder(8, 1, 42, brokerEpoch,
      oneTopicPartitionState.asJava, Seq.empty.asJava, newTopicIds.asJava, true, AbstractControlRequest.Type.FULL).build()
    assertEquals(
      Seq(Uuid.fromString("IQ2F1tpCRoSbjfq4zBJwpg"), Uuid.fromString("4N8_J-q7SdWHPFkos275pQ")),
      ZkMetadataCache.maybeInjectDeletedPartitionsFromFullMetadataRequest(
        initialSnapshot, 42, updateMetadataRequest.topicStates())
    )
    verifyTopicStates(updateMetadataRequest) { topicStates =>
      assertEquals(3, topicStates.size)
      assertEquals(3, topicStates("test-topic-1").values.toSeq.count(_.leader() == -2))
      assertEquals(3, topicStates("test-topic-2").values.toSeq.count(_.leader() == -2))
    }

    // Existing two plus one new topic, nothing gets deleted, all topics should be present
    val allTopicStates = initialTopicStates.flatMap(_._2.values).toSeq ++ oneTopicPartitionState
    val allTopicIds = initialTopicIds ++ newTopicIds
    updateMetadataRequest = new UpdateMetadataRequest.Builder(8, 1, 42, brokerEpoch,
      allTopicStates.asJava, Seq.empty.asJava, allTopicIds.asJava, true, AbstractControlRequest.Type.FULL).build()
    assertEquals(
      Seq.empty,
      ZkMetadataCache.maybeInjectDeletedPartitionsFromFullMetadataRequest(
        initialSnapshot, 42, updateMetadataRequest.topicStates())
    )
    verifyTopicStates(updateMetadataRequest) { topicStates =>
      assertEquals(3, topicStates.size)
      // Ensure these two weren't deleted (leader = -2)
      assertEquals(0, topicStates("test-topic-1").values.toSeq.count(_.leader() == -2))
      assertEquals(0, topicStates("test-topic-2").values.toSeq.count(_.leader() == -2))
    }
  }

  /**
   * Verify the behavior of ZkMetadataCache when handling "Full" UpdateMetadataRequest
   */
  @Test
  def testHandleFullUpdateMetadataRequestInZkMigration(): Unit = {
    val (initialTopicIds, initialTopicStates, newTopicIds, newPartitionStates) = setupInitialAndFullMetadata()

    val updateMetadataRequestBuilder = () => new UpdateMetadataRequest.Builder(8, 1, 42, brokerEpoch,
      newPartitionStates.asJava, Seq.empty.asJava, newTopicIds.asJava, true, AbstractControlRequest.Type.FULL).build()

    def verifyMetadataCache(
      updateMetadataRequest: UpdateMetadataRequest,
      zkMigrationEnabled: Boolean = true
    )(
      verifier: ZkMetadataCache => Unit
    ): Unit = {
      val cache = MetadataCache.zkMetadataCache(1, MetadataVersion.latest(), zkMigrationEnabled = zkMigrationEnabled)
      cache.updateMetadata(1, new UpdateMetadataRequest.Builder(8, 1, 42, brokerEpoch,
        initialTopicStates.flatMap(_._2.values).toList.asJava, Seq.empty.asJava, initialTopicIds.asJava).build())
      cache.updateMetadata(1, updateMetadataRequest)
      verifier.apply(cache)
    }

    // KRaft=false Type=FULL, migration disabled
    var updateMetadataRequest = updateMetadataRequestBuilder.apply()
    updateMetadataRequest.data().setIsKRaftController(true)
    updateMetadataRequest.data().setType(AbstractControlRequest.Type.FULL.toByte)
    verifyMetadataCache(updateMetadataRequest, zkMigrationEnabled = false) { cache =>
      assertEquals(3, cache.getAllTopics().size)
      assertTrue(cache.contains("test-topic-1"))
      assertTrue(cache.contains("test-topic-1"))
    }

    // KRaft=true Type=FULL
    updateMetadataRequest = updateMetadataRequestBuilder.apply()
    verifyMetadataCache(updateMetadataRequest) { cache =>
      assertEquals(1, cache.getAllTopics().size)
      assertFalse(cache.contains("test-topic-1"))
      assertFalse(cache.contains("test-topic-1"))
    }

    // KRaft=false Type=FULL
    updateMetadataRequest = updateMetadataRequestBuilder.apply()
    updateMetadataRequest.data().setIsKRaftController(false)
    verifyMetadataCache(updateMetadataRequest) { cache =>
      assertEquals(3, cache.getAllTopics().size)
      assertTrue(cache.contains("test-topic-1"))
      assertTrue(cache.contains("test-topic-1"))
    }

    // KRaft=true Type=INCREMENTAL
    updateMetadataRequest = updateMetadataRequestBuilder.apply()
    updateMetadataRequest.data().setType(AbstractControlRequest.Type.INCREMENTAL.toByte)
    verifyMetadataCache(updateMetadataRequest) { cache =>
      assertEquals(3, cache.getAllTopics().size)
      assertTrue(cache.contains("test-topic-1"))
      assertTrue(cache.contains("test-topic-1"))
    }

    // KRaft=true Type=UNKNOWN
    updateMetadataRequest = updateMetadataRequestBuilder.apply()
    updateMetadataRequest.data().setType(AbstractControlRequest.Type.UNKNOWN.toByte)
    verifyMetadataCache(updateMetadataRequest) { cache =>
      assertEquals(3, cache.getAllTopics().size)
      assertTrue(cache.contains("test-topic-1"))
      assertTrue(cache.contains("test-topic-1"))
    }
  }

  @Test
  def testGetOfflineReplicasConsidersDirAssignment(): Unit = {
    case class Broker(id: Int, dirs: util.List[Uuid])
    case class Partition(id: Int, replicas: util.List[Integer], dirs: util.List[Uuid])

    def offlinePartitions(brokers: Seq[Broker], partitions: Seq[Partition]): Map[Int, util.List[Integer]] = {
      val delta = new MetadataDelta.Builder().build()
      brokers.foreach(broker => delta.replay(
        new RegisterBrokerRecord().setFenced(false).
          setBrokerId(broker.id).setLogDirs(broker.dirs).
          setEndPoints(new BrokerEndpointCollection(Collections.singleton(
            new RegisterBrokerRecord.BrokerEndpoint().setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
              setPort(9093.toShort).setName("PLAINTEXT").setHost(s"broker-${broker.id}")).iterator()))))
      val topicId = Uuid.fromString("95OVr1IPRYGrcNCLlpImCA")
      delta.replay(new TopicRecord().setTopicId(topicId).setName("foo"))
      partitions.foreach(partition => delta.replay(
        new PartitionRecord().setTopicId(topicId).setPartitionId(partition.id).
          setReplicas(partition.replicas).setDirectories(partition.dirs).
          setLeader(partition.replicas.get(0)).setIsr(partition.replicas)))
      val cache = MetadataCache.kRaftMetadataCache(1)
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      val topicMetadata = cache.getTopicMetadata(Set("foo"), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).head
      topicMetadata.partitions().asScala.map(p => (p.partitionIndex(), p.offlineReplicas())).toMap
    }

    val brokers = Seq(
      Broker(0, asList(Uuid.fromString("broker1logdirjEo71BG0w"))),
      Broker(1, asList(Uuid.fromString("broker2logdirRmQQgLxgw")))
    )
    val partitions = Seq(
      Partition(0, asList(0, 1), asList(Uuid.fromString("broker1logdirjEo71BG0w"), DirectoryId.LOST)),
      Partition(1, asList(0, 1), asList(Uuid.fromString("unknownlogdirjEo71BG0w"), DirectoryId.UNASSIGNED)),
      Partition(2, asList(0, 1), asList(DirectoryId.MIGRATING, Uuid.fromString("broker2logdirRmQQgLxgw")))
    )
    assertEquals(Map(
      0 -> asList(1),
      1 -> asList(0),
      2 -> asList(),
    ), offlinePartitions(brokers, partitions))
  }
}
