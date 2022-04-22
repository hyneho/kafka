package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse, ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.util.Collections
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class DeleteRecordsRequestTest extends BaseRequestTest {
  private val TIMEOUT_MS = 1000
  private val MESSAGES_PRODUCED_PER_PARTITION = 10
  private var producer: KafkaProducer[String, String] = null

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    producer = TestUtils.createProducer(bootstrapServers(),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  @Test
  def testDeleteRecordsHappyCase(): Unit = {
    val (topicPartition: TopicPartition, leaderId: Int) = createTopicAndSendRecords

    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = Math.max(MESSAGES_PRODUCED_PER_PARTITION - 8, 0)
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(topicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request, leaderId)
    val partitionResult = response.data.topics.find(topicPartition.topic).partitions.find(topicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.NONE.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode).name()}")

    // Validate the expected lowWaterMark in the response
    assertEquals(offsetToDelete, partitionResult.lowWatermark(),
      s"Unexpected lowWatermark received: ${partitionResult.lowWatermark}")

    // Validate that the records have actually deleted
    validateRecordsAreDeleted(topicPartition, leaderId, offsetToDelete)
  }

  @Test
  def testErrorWhenDeletingRecordsWithInvalidOffset(): Unit = {
    val (topicPartition: TopicPartition, leaderId: Int) = createTopicAndSendRecords

    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = MESSAGES_PRODUCED_PER_PARTITION + 5
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(topicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request, leaderId)
    val partitionResult = response.data.topics.find(topicPartition.topic).partitions.find(topicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.OFFSET_OUT_OF_RANGE.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode()).name()}")
  }

  @Test
  def testErrorWhenDeletingRecordsWithInvalidTopic(): Unit = {
    val (_, leaderId: Int) = createTopicAndSendRecords

    val invalidTopicPartition = new TopicPartition("invalid-topic", 0)
    // Create the DeleteRecord request requesting deletion of offset which is not present
    val offsetToDelete = 1
    val request: DeleteRecordsRequest = createDeleteRecordsRequestForTopicPartition(invalidTopicPartition, offsetToDelete)

    // call the API
    val response = sendDeleteRecordsRequest(request, leaderId)
    val partitionResult = response.data.topics.find(invalidTopicPartition.topic).partitions.find(invalidTopicPartition.partition)

    // Validate the expected error code in the response
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), partitionResult.errorCode(),
      s"Unexpected error code received: ${Errors.forCode(partitionResult.errorCode()).name()}")
  }

  private def createTopicAndSendRecords = {
    // Single topic
    val topic1 = "topic-1"
    val topicPartition = new TopicPartition(topic1, 0)
    val partitionToLeader = createTopic(topic1)
    assertTrue(partitionToLeader.contains(topicPartition.partition), "Topic creation did not succeed")
    // Write records
    produceData(Seq(topicPartition), MESSAGES_PRODUCED_PER_PARTITION)
    (topicPartition, partitionToLeader(topicPartition.partition))
  }

  private def createDeleteRecordsRequestForTopicPartition(topicPartition: TopicPartition, offsetToDelete: Int) = {
    val requestData = new DeleteRecordsRequestData()
      .setTopics(Collections.singletonList(new DeleteRecordsTopic()
        .setName(topicPartition.topic())
        .setPartitions(Collections.singletonList(new DeleteRecordsPartition()
          .setOffset(offsetToDelete)
          .setPartitionIndex(topicPartition.partition())))))
      .setTimeoutMs(TIMEOUT_MS)
    val request = new DeleteRecordsRequest.Builder(requestData).build()
    request
  }

  private def sendDeleteRecordsRequest(request: DeleteRecordsRequest, leaderId: Int): DeleteRecordsResponse = {
    connectAndReceive[DeleteRecordsResponse](request, destination = brokerSocketServer(leaderId))
  }

  private def produceData(topicPartitions: Iterable[TopicPartition], numMessagesPerPartition: Int): Seq[RecordMetadata] = {
    val records = for {
      tp <- topicPartitions.toSeq
      messageIndex <- 0 until numMessagesPerPartition
    } yield {
      val suffix = s"$tp-$messageIndex"
      new ProducerRecord(tp.topic, tp.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
  }

  private def validateRecordsAreDeleted(topicPartition: TopicPartition, leaderId: Int, expectedOffset: Long): Unit = {
    val targetTimes = List(new ListOffsetsTopic()
      .setName(topicPartition.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(topicPartition.partition)
        .setTimestamp(0L)).asJava)).asJava

    val request = ListOffsetsRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false)
      .setTargetTimes(targetTimes).build()

    val listOffsetsPartitionResponse = connectAndReceive[ListOffsetsResponse](request, destination = brokerSocketServer(leaderId)).topics.asScala.find(_.name == topicPartition.topic).get
      .partitions.asScala.find(_.partitionIndex == topicPartition.partition).get

    assertEquals(expectedOffset, listOffsetsPartitionResponse.offset)
  }

}
