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
package kafka.coordinator.group

import kafka.server.{LogAppendResult, ReplicaManager, RequestLocal}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{NotLeaderOrFollowerException, RecordTooLargeException}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, RecordConversionStats}
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.coordinator.group.runtime.PartitionWriter
import org.apache.kafka.storage.internals.log.{AppendOrigin, LeaderHwChange, LogAppendInfo, LogConfig}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.nio.charset.Charset
import java.util.{Collections, Optional, OptionalInt, Properties}
import scala.jdk.CollectionConverters._

class StringKeyValueSerializer extends PartitionWriter.Serializer[(String, String)] {
  override def serializeKey(record: (String, String)): Array[Byte] = {
    record._1.getBytes(Charset.defaultCharset())
  }

  override def serializeValue(record: (String, String)): Array[Byte] = {
    record._2.getBytes(Charset.defaultCharset())
  }
}

class PartitionWriterImplTest {
  @Test
  def testRegisterDeregisterListener(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new PartitionWriterImpl(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    val listener = new PartitionWriter.Listener {
      override def onHighWatermarkUpdated(tp: TopicPartition, offset: Long): Unit = {}
    }

    partitionRecordWriter.registerListener(tp, listener)
    verify(replicaManager).maybeAddListener(tp, new ListenerAdaptor(listener))

    partitionRecordWriter.deregisterListener(tp, listener)
    verify(replicaManager).removeListener(tp, new ListenerAdaptor(listener))

    assertEquals(
      new ListenerAdaptor(listener),
      new ListenerAdaptor(listener)
    )
    assertEquals(
      new ListenerAdaptor(listener).hashCode(),
      new ListenerAdaptor(listener).hashCode()
    )
  }

  @Test
  def testWriteRecords(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val time = new MockTime()
    val partitionRecordWriter = new PartitionWriterImpl(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      time
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val recordsCapture: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    when(replicaManager.appendToLocalLog(
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      ArgumentMatchers.eq(1),
      ArgumentMatchers.eq(RequestLocal.NoCaching)
    )).thenReturn(Map(tp -> LogAppendResult(new LogAppendInfo(
      Optional.empty(),
      10,
      OptionalInt.empty(),
      RecordBatch.NO_TIMESTAMP,
      -1L,
      RecordBatch.NO_TIMESTAMP,
      -1L,
      RecordConversionStats.EMPTY,
      CompressionType.NONE,
      CompressionType.NONE,
      -1,
      -1,
      false,
      -1L,
      Collections.emptyList(),
      "",
      LeaderHwChange.INCREASED
    ))))

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertEquals(11, partitionRecordWriter.append(
      tp,
      records.asJava
    ))

    verify(replicaManager).maybeCompletePurgatories(
      tp,
      LeaderHwChange.INCREASED
    )

    val batch = recordsCapture.getValue.getOrElse(tp,
      throw new AssertionError(s"No records for $tp"))
    assertEquals(1, batch.batches().asScala.toList.size)

    val receivedRecords = batch.records.asScala.map { record =>
      (
        Charset.defaultCharset().decode(record.key).toString,
        Charset.defaultCharset().decode(record.value).toString,
      )
    }.toList

    assertEquals(records, receivedRecords)
  }

  @Test
  def testWriteRecordTooLarge(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new PartitionWriterImpl(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    val maxBatchSize = 16384
    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Map(TopicConfig.MAX_MESSAGE_BYTES_CONFIG -> maxBatchSize).asJava,
      new Properties()
    )))

    val randomBytes = TestUtils.randomBytes(maxBatchSize + 1)
    // We need more than one record here because the first record
    // is always allowed by the MemoryRecordsBuilder.
    val records = List(
      ("k0", new String(randomBytes)),
      ("k1", new String(randomBytes)),
    )

    assertThrows(classOf[RecordTooLargeException],
      () => partitionRecordWriter.append(tp, records.asJava))
  }

  @Test
  def testWriteEmptyRecordList(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new PartitionWriterImpl(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    assertThrows(classOf[IllegalStateException],
      () => partitionRecordWriter.append(tp, List.empty.asJava))
  }

  @Test
  def testNonexistentPartition(): Unit = {
    val tp = new TopicPartition("foo", 0)
    val replicaManager = mock(classOf[ReplicaManager])
    val partitionRecordWriter = new PartitionWriterImpl(
      replicaManager,
      new StringKeyValueSerializer(),
      CompressionType.NONE,
      Time.SYSTEM
    )

    when(replicaManager.getLogConfig(tp)).thenReturn(None)

    val records = List(
      ("k0", "v0"),
      ("k1", "v1"),
      ("k2", "v2"),
    )

    assertThrows(classOf[NotLeaderOrFollowerException],
      () => partitionRecordWriter.append(tp, records.asJava))
  }
}
