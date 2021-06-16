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

package kafka.admin

import java.util
import java.util.{Collections, Optional}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, RangeAssignor}
import org.apache.kafka.common.{ConsumerGroupState, KafkaFuture, Node, TopicPartition, TopicPartitionInfo}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import scala.jdk.CollectionConverters._

class ConsumerGroupServiceTest {

  private val group = "testGroup"
  private val topics = (0 until 5).map(i => s"testTopic$i")
  private val numPartitions = 10
  private val topicPartitions = topics.flatMap(topic => (0 until numPartitions).map(i => new TopicPartition(topic, i)))
  private val admin = mock(classOf[Admin])

  @Test
  def testAdminRequestsForDescribeOffsets(): Unit = {
    val args = Array("--bootstrap-server", "localhost:9092", "--group", group, "--describe", "--offsets")
    val groupService = consumerGroupService(args)

    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(describeGroupsResult(ConsumerGroupState.STABLE))
    when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(group), any()))
      .thenReturn(listGroupOffsetsResult)
    when(admin.listOffsets(offsetsArgMatcher, any()))
      .thenReturn(listOffsetsResult)

    val (state, assignments) = groupService.collectGroupOffsets(group)
    assertEquals(Some("Stable"), state)
    assertTrue(assignments.nonEmpty)
    assertEquals(topicPartitions.size, assignments.get.size)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(group), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcher, any())
  }

  @Test
  def testAdminRequestsForDescribeNegativeOffsets(): Unit = {
    val args = Array("--bootstrap-server", "localhost:9092", "--group", group, "--describe", "--offsets")
    val groupService = consumerGroupService(args)

    val testTopicPartition0 = new TopicPartition("testTopic1", 0);
    val testTopicPartition1 = new TopicPartition("testTopic1", 1);
    val testTopicPartition2 = new TopicPartition("testTopic1", 2);
    val testTopicPartition3 = new TopicPartition("testTopic2", 0);
    val testTopicPartition4 = new TopicPartition("testTopic2", 1);
    val testTopicPartition5 = new TopicPartition("testTopic2", 2);

    val offsets = Map(
      //testTopicPartition0 -> there is no offset information for an asssigned topic partition
      testTopicPartition1 -> new OffsetAndMetadata(100), // regular information for a assigned partition
      testTopicPartition2 -> null, //there is a null value for an asssigned topic partition
      // testTopicPartition3 ->  there is no offset information for an unasssigned topic partition
      testTopicPartition4 -> new OffsetAndMetadata(100), // regular information for a unassigned partition
      testTopicPartition5 -> null, //there is a null value for an unasssigned topic partition
    ).asJava

    val resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis, Optional.of(1))
    val endOffsets = Map(
      testTopicPartition0 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition1 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition2 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition3 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition4 -> KafkaFuture.completedFuture(resultInfo),
      testTopicPartition5 -> KafkaFuture.completedFuture(resultInfo),
    )
    val assignedTopicPartitions = Set(testTopicPartition0, testTopicPartition1, testTopicPartition2 )
    val unassignedTopicPartitions = offsets.asScala.filterNot { case (tp, _) => assignedTopicPartitions.contains(tp) }.toMap.keySet

    def describeGroupsResult(groupState: ConsumerGroupState): DescribeConsumerGroupsResult = {
      val member1 = new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", new MemberAssignment(assignedTopicPartitions.asJava))
      val description = new ConsumerGroupDescription(group,
        true,
        Collections.singleton(member1),
        classOf[RangeAssignor].getName,
        groupState,
        new Node(1, "localhost", 9092))
      new DescribeConsumerGroupsResult(Collections.singletonMap(group, KafkaFuture.completedFuture(description)))
    }

    def offsetsArgMatcherAssignedTopics: util.Map[TopicPartition, OffsetSpec] = {
      val expectedOffsets = endOffsets.filter{ case (tp, _) => assignedTopicPartitions.contains(tp) }.keySet.map(tp => tp -> OffsetSpec.latest).toMap
      ArgumentMatchers.argThat[util.Map[TopicPartition, OffsetSpec]] { map =>
        map.keySet.asScala == expectedOffsets.keySet && map.values.asScala.forall(_.isInstanceOf[OffsetSpec.LatestSpec])
      }
    }
    def offsetsArgMatcherUnassignedTopics: util.Map[TopicPartition, OffsetSpec] = {
      val expectedOffsets = offsets.asScala.filter{ case (tp, _) => unassignedTopicPartitions.contains(tp) }.keySet.map(tp => tp -> OffsetSpec.latest).toMap
      ArgumentMatchers.argThat[util.Map[TopicPartition, OffsetSpec]] { map =>
        map.keySet.asScala == expectedOffsets.keySet && map.values.asScala.forall(_.isInstanceOf[OffsetSpec.LatestSpec])
      }
    }
    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(describeGroupsResult(ConsumerGroupState.STABLE))
    when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(group), any()))
      .thenReturn(AdminClientTestUtils.listConsumerGroupOffsetsResult(offsets))
    doReturn(new ListOffsetsResult(endOffsets.asJava)).when(admin).listOffsets(offsetsArgMatcherAssignedTopics, any())
    doReturn(new ListOffsetsResult(endOffsets.asJava)).when(admin).listOffsets(offsetsArgMatcherUnassignedTopics, any())

    val (state, assignments) = groupService.collectGroupOffsets(group)
    assertEquals(Some("Stable"), state)
    assertTrue(assignments.nonEmpty)
    // Results should have information for all assigned topic partition (even if there is not Offset's information at all, because they get fills with None)
    // Results should have information only for unassigned topic partitions if and only if there is information about them (included with null values)
    assertEquals(assignedTopicPartitions.size + unassignedTopicPartitions.size , assignments.get.size)
    assignments.map( results =>
      results.map( partitionAssignmentState =>
        (partitionAssignmentState.topic, partitionAssignmentState.partition) match {
          case (Some("testTopic1"), Some(0)) => assertEquals(None, partitionAssignmentState.offset)
          case (Some("testTopic1"), Some(1)) => assertEquals(Some(100), partitionAssignmentState.offset)
          case (Some("testTopic1"), Some(2)) => assertEquals(None, partitionAssignmentState.offset)
          case (Some("testTopic2"), Some(1)) => assertEquals(Some(100), partitionAssignmentState.offset)
          case (Some("testTopic2"), Some(2)) => assertEquals(None, partitionAssignmentState.offset)
          case _ => assertTrue(false)
    }))

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(group), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcherAssignedTopics, any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcherUnassignedTopics, any())
  }

  @Test
  def testAdminRequestsForResetOffsets(): Unit = {
    val args = Seq("--bootstrap-server", "localhost:9092", "--group", group, "--reset-offsets", "--to-latest")
    val topicsWithoutPartitionsSpecified = topics.tail
    val topicArgs = Seq("--topic", s"${topics.head}:${(0 until numPartitions).mkString(",")}") ++
      topicsWithoutPartitionsSpecified.flatMap(topic => Seq("--topic", topic))
    val groupService = consumerGroupService((args ++ topicArgs).toArray)

    when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any()))
      .thenReturn(describeGroupsResult(ConsumerGroupState.DEAD))
    when(admin.describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified.asJava), any()))
      .thenReturn(describeTopicsResult(topicsWithoutPartitionsSpecified))
    when(admin.listOffsets(offsetsArgMatcher, any()))
      .thenReturn(listOffsetsResult)

    val resetResult = groupService.resetOffsets()
    assertEquals(Set(group), resetResult.keySet)
    assertEquals(topicPartitions.toSet, resetResult(group).keySet)

    verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(group)), any())
    verify(admin, times(1)).describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified.asJava), any())
    verify(admin, times(1)).listOffsets(offsetsArgMatcher, any())
  }

  private def consumerGroupService(args: Array[String]): ConsumerGroupService = {
    new ConsumerGroupService(new ConsumerGroupCommandOptions(args)) {
      override protected def createAdminClient(configOverrides: collection.Map[String, String]): Admin = {
        admin
      }
    }
  }

  private def describeGroupsResult(groupState: ConsumerGroupState): DescribeConsumerGroupsResult = {
    val member1 = new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", null)
    val description = new ConsumerGroupDescription(group,
      true,
      Collections.singleton(member1),
      classOf[RangeAssignor].getName,
      groupState,
      new Node(1, "localhost", 9092))
    new DescribeConsumerGroupsResult(Collections.singletonMap(group, KafkaFuture.completedFuture(description)))
  }

  private def listGroupOffsetsResult: ListConsumerGroupOffsetsResult = {
    val offsets = topicPartitions.map(_ -> new OffsetAndMetadata(100)).toMap.asJava
    AdminClientTestUtils.listConsumerGroupOffsetsResult(offsets)
  }

  private def offsetsArgMatcher: util.Map[TopicPartition, OffsetSpec] = {
    val expectedOffsets = topicPartitions.map(tp => tp -> OffsetSpec.latest).toMap
    ArgumentMatchers.argThat[util.Map[TopicPartition, OffsetSpec]] { map =>
      map.keySet.asScala == expectedOffsets.keySet && map.values.asScala.forall(_.isInstanceOf[OffsetSpec.LatestSpec])
    }
  }

  private def listOffsetsResult: ListOffsetsResult = {
    val resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis, Optional.of(1))
    val futures = topicPartitions.map(_ -> KafkaFuture.completedFuture(resultInfo)).toMap
    new ListOffsetsResult(futures.asJava)
  }

  private def describeTopicsResult(topics: Seq[String]): DescribeTopicsResult = {
   val topicDescriptions = topics.map { topic =>
      val partitions = (0 until numPartitions).map(i => new TopicPartitionInfo(i, null, Collections.emptyList[Node], Collections.emptyList[Node]))
      topic -> new TopicDescription(topic, false, partitions.asJava)
    }.toMap
    AdminClientTestUtils.describeTopicsResult(topicDescriptions.asJava)
  }
}
