/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.nio.ByteBuffer
import java.util
import java.util.Properties

import kafka.log.{Log, LogConfig, LogManager, LogTest}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.tools.DumpLogSegments.TimeIndexDumpErrors
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metadata.{PartitionChangeRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, ObjectSerializationCache}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.ApiMessageAndVersion
import org.apache.kafka.raft.metadata.MetadataRecordSerde
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class BatchInfo(records: Seq[SimpleRecord], hasKeys: Boolean, hasValues: Boolean)

class DumpLogSegmentsTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val segmentName = "00000000000000000000"
  val logFilePath = s"$logDir/$segmentName.log"
  val indexFilePath = s"$logDir/$segmentName.index"
  val timeIndexFilePath = s"$logDir/$segmentName.timeindex"
  val time = new MockTime(0, 0)

  val batches = new ArrayBuffer[BatchInfo]
  var log: Log = _

  @BeforeEach
  def setUp(): Unit = {
    val props = new Properties
    props.setProperty(LogConfig.IndexIntervalBytesProp, "128")
    log = Log(logDir, LogConfig(props), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      time = time, brokerTopicStats = new BrokerTopicStats, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10), topicId = None, keepPartitionMetadataFile = true)
  }

  def addSimpleRecords(): Unit = {
    val now = System.currentTimeMillis()
    val firstBatchRecords = (0 until 10).map { i => new SimpleRecord(now + i * 2, s"message key $i".getBytes, s"message value $i".getBytes)}
    batches += BatchInfo(firstBatchRecords, true, true)
    val secondBatchRecords = (10 until 30).map { i => new SimpleRecord(now + i * 3, s"message key $i".getBytes, null)}
    batches += BatchInfo(secondBatchRecords, true, false)
    val thirdBatchRecords = (30 until 50).map { i => new SimpleRecord(now + i * 5, null, s"message value $i".getBytes)}
    batches += BatchInfo(thirdBatchRecords, false, true)
    val fourthBatchRecords = (50 until 60).map { i => new SimpleRecord(now + i * 7, null)}
    batches += BatchInfo(fourthBatchRecords, false, false)

    batches.foreach { batchInfo =>
      log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0, batchInfo.records: _*),
        leaderEpoch = 0)
    }
    // Flush, but don't close so that the indexes are not trimmed and contain some zero entries
    log.flush()
  }

  @AfterEach
  def tearDown(): Unit = {
    log.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testPrintDataLog(): Unit = {
    addSimpleRecords()
    def verifyRecordsInOutput(checkKeysAndValues: Boolean, args: Array[String]): Unit = {
      def isBatch(index: Int): Boolean = {
        var i = 0
        batches.zipWithIndex.foreach { case (batch, batchIndex) =>
          if (i == index)
            return true

          i += 1

          batch.records.indices.foreach { recordIndex =>
            if (i == index)
              return false
            i += 1
          }
        }
        throw new AssertionError(s"No match for index $index")
      }

      val output = runDumpLogSegments(args)
      val lines = output.split("\n")
      assertTrue(lines.length > 2, s"Data not printed: $output")
      val totalRecords = batches.map(_.records.size).sum
      var offset = 0
      val batchIterator = batches.iterator
      var batch : BatchInfo = null;
      (0 until totalRecords + batches.size).foreach { index =>
        val line = lines(lines.length - totalRecords - batches.size + index)
        // The base offset of the batch is the offset of the first record in the batch, so we
        // only increment the offset if it's not a batch
        if (isBatch(index)) {
          assertTrue(line.startsWith(s"baseOffset: $offset lastOffset: "), s"Not a valid batch-level message record: $line")
          batch = batchIterator.next()
        } else {
          assertTrue(line.startsWith(s"${DumpLogSegments.RecordIndent} offset: $offset"), s"Not a valid message record: $line")
          if (checkKeysAndValues) {
            var suffix = "headerKeys: []"
            if (batch.hasKeys)
              suffix += s" key: message key $offset"
            if (batch.hasValues)
              suffix += s" payload: message value $offset"
            assertTrue(line.endsWith(suffix), s"Message record missing key or value: $line")
          }
          offset += 1
        }
      }
    }

    def verifyNoRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      assertFalse(output.matches("(?s).*offset: [0-9]* isvalid.*"), s"Data should not have been printed: $output")
    }

    // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
    verifyRecordsInOutput(true, Array("--print-data-log", "--files", logFilePath))
    // Verify that records are printed with --print-data-log if --deep-iteration is also specified
    verifyRecordsInOutput(true, Array("--print-data-log", "--deep-iteration", "--files", logFilePath))
    // Verify that records are printed with --value-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(true, Array("--value-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --key-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(true, Array("--key-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
    verifyRecordsInOutput(false, Array("--deep-iteration", "--files", logFilePath))

    // Verify that records are not printed by default
    verifyNoRecordsInOutput(Array("--files", logFilePath))
  }

  @Test
  def testDumpIndexMismatches(): Unit = {
    addSimpleRecords()
    val offsetMismatches = mutable.Map[String, List[(Long, Long)]]()
    DumpLogSegments.dumpIndex(new File(indexFilePath), indexSanityOnly = false, verifyOnly = true, offsetMismatches,
      Int.MaxValue)
    assertEquals(Map.empty, offsetMismatches)
  }

  @Test
  def testDumpTimeIndexErrors(): Unit = {
    addSimpleRecords()
    val errors = new TimeIndexDumpErrors
    DumpLogSegments.dumpTimeIndex(new File(timeIndexFilePath), indexSanityOnly = false, verifyOnly = true, errors,
      Int.MaxValue)
    assertEquals(Map.empty, errors.misMatchesForTimeIndexFilesMap)
    assertEquals(Map.empty, errors.outOfOrderTimestamp)
    assertEquals(Map.empty, errors.shallowOffsetNotFound)
  }

  @Test
  def testDumpMetadataRecords(): Unit = {
    val mockTime = new MockTime
    val logConfig = LogTest.createLogConfig(segmentBytes = 1024 * 1024)
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime)

    val metadataRecords = Seq(
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(0).setBrokerEpoch(10), 0.toShort),
      new ApiMessageAndVersion(
        new RegisterBrokerRecord().setBrokerId(1).setBrokerEpoch(20), 0.toShort),
      new ApiMessageAndVersion(
        new TopicRecord().setName("test-topic").setTopicId(Uuid.randomUuid()), 0.toShort),
      new ApiMessageAndVersion(
        new PartitionChangeRecord().setTopicId(Uuid.randomUuid()).setLeader(1).
          setPartitionId(0).setIsr(util.Arrays.asList(0, 1, 2)), 0.toShort)
    )

    val records: Array[SimpleRecord] = metadataRecords.map(message => {
      val serde = new MetadataRecordSerde()
      val cache = new ObjectSerializationCache
      val size = serde.recordSize(message, cache)
      val buf = ByteBuffer.allocate(size)
      val writer = new ByteBufferAccessor(buf)
      serde.write(message, cache, writer)
      buf.flip()
      new SimpleRecord(null, buf.array)
    }).toArray
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, records:_*), leaderEpoch = 1)
    log.flush()

    var output = runDumpLogSegments(Array("--cluster-metadata-decoder", "false", "--files", logFilePath))
    assert(output.contains("TOPIC_RECORD"))
    assert(output.contains("BROKER_RECORD"))

    output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--skip-record-metadata", "false", "--files", logFilePath))
    assert(output.contains("TOPIC_RECORD"))
    assert(output.contains("BROKER_RECORD"))

    // Bogus metadata record
    val buf = ByteBuffer.allocate(4)
    val writer = new ByteBufferAccessor(buf)
    writer.writeUnsignedVarint(10000)
    writer.writeUnsignedVarint(10000)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(null, buf.array)), leaderEpoch = 2)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, records:_*), leaderEpoch = 2)

    output = runDumpLogSegments(Array("--cluster-metadata-decoder", "--skip-record-metadata", "false", "--files", logFilePath))
    assert(output.contains("TOPIC_RECORD"))
    assert(output.contains("BROKER_RECORD"))
    assert(output.contains("skipping"))
  }

  @Test
  def testDumpEmptyIndex(): Unit = {
    val indexFile = new File(indexFilePath)
    new PrintWriter(indexFile).close()
    val expectOutput = s"$indexFile is empty.\n"
    val outContent = new ByteArrayOutputStream()
    Console.withOut(outContent) {
      DumpLogSegments.dumpIndex(indexFile, indexSanityOnly = false, verifyOnly = true,
        misMatchesForIndexFilesMap = mutable.Map[String, List[(Long, Long)]](), Int.MaxValue)
    }
    assertEquals(expectOutput, outContent.toString)
  }

  private def runDumpLogSegments(args: Array[String]): String = {
    val outContent = new ByteArrayOutputStream
    Console.withOut(outContent) {
      DumpLogSegments.main(args)
    }
    outContent.toString
  }
}
