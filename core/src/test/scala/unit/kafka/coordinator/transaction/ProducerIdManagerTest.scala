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
package kafka.coordinator.transaction

import kafka.server.BrokerToControllerChannelManager
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.message.AllocateProducerIdsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AllocateProducerIdsResponse
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}

import java.util.stream.IntStream

class ProducerIdManagerTest {

  var brokerToController: BrokerToControllerChannelManager = EasyMock.niceMock(classOf[BrokerToControllerChannelManager])

  // Mutable test implementation that lets us easily set the idStart and error
  class MockProducerIdManager(val brokerId: Int, var idStart: Long, val idLen: Int, var error: Errors = Errors.NONE)
    extends ProducerIdManager(brokerId, () => 1, brokerToController, 100) {

    override private[transaction] def sendRequest(): Unit = {
      if (error == Errors.NONE) {
        handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
          new AllocateProducerIdsResponseData().setProducerIdStart(idStart).setProducerIdLen(idLen)))
        idStart += idLen
      } else {
        handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
          new AllocateProducerIdsResponseData().setErrorCode(error.code)))
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = Array(1, 2, 10))
  def testContiguousIds(idBlockLen: Int): Unit = {
    val manager = new MockProducerIdManager(0, 0, idBlockLen)

    IntStream.range(0, idBlockLen * 3).forEach { i =>
      assertEquals(i, manager.generateProducerId())
    }
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array("UNKNOWN_SERVER_ERROR", "INVALID_REQUEST"))
  def testUnrecoverableErrors(error: Errors): Unit = {
    val manager = new MockProducerIdManager(0, 0, 1)
    assertEquals(0, manager.generateProducerId())

    manager.error = error
    assertThrows(classOf[Throwable], () => manager.generateProducerId())

    manager.error = Errors.NONE
    assertEquals(1, manager.generateProducerId())
  }

  @Test
  def testInvalidRanges(): Unit = {
    var manager = new MockProducerIdManager(0, -1, 10)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())

    manager = new MockProducerIdManager(0, 0, -1)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())

    manager = new MockProducerIdManager(0, Long.MaxValue-1, 10)
    assertThrows(classOf[KafkaException], () => manager.generateProducerId())
  }
}

