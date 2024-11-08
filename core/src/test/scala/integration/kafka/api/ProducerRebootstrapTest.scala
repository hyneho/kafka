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
package kafka.api

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.time.Duration
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class ProducerRebootstrapTest extends RebootstrapTest {
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testRebootstrap(useRebootstrapTriggerMs: Boolean): Unit = {
    server1.shutdown()
    server1.awaitShutdown()

    val producer = createProducer(configOverrides = clientOverrides(useRebootstrapTriggerMs))

    // Only the server 0 is available for the producer during the bootstrap.
    val recordMetadata0 = producer.send(new ProducerRecord(topic, part, "key 0".getBytes, "value 0".getBytes)).get()
    assertEquals(0, recordMetadata0.offset())

    server0.shutdown()
    server0.awaitShutdown()
    server1.startup()

    // The server 0, originally cached during the bootstrap, is offline.
    // However, the server 1 from the bootstrap list is online.
    // Should be able to produce records.
    val recordMetadata1 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get()
    assertEquals(0, recordMetadata1.offset())

    server1.shutdown()
    server1.awaitShutdown()
    server0.startup()

    // The same situation, but the server 1 has gone and server 0 is back.
    val recordMetadata2 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get()
    assertEquals(1, recordMetadata2.offset())
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testRebootstrapDisabled(useRebootstrapTriggerMs: Boolean): Unit = {
    server1.shutdown()
    server1.awaitShutdown()

    val configOverrides = clientOverrides(useRebootstrapTriggerMs)
    configOverrides.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none")
    if (useRebootstrapTriggerMs)
      configOverrides.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "1000")

    val producer = createProducer(configOverrides = configOverrides)
    val consumer = createConsumer(configOverrides = configOverrides)
    val adminClient = createAdminClient(configOverrides = configOverrides)

    // Only the server 0 is available during the bootstrap.
    val recordMetadata0 = producer.send(new ProducerRecord(topic, part, 0L, "key 0".getBytes, "value 0".getBytes)).get(15, TimeUnit.SECONDS)
    assertEquals(0, recordMetadata0.offset())
    adminClient.listTopics().names().get(15, TimeUnit.SECONDS)
    consumer.assign(Collections.singleton(tp))
    consumeAndVerifyRecords(consumer, 1, 0)

    server0.shutdown()
    server0.awaitShutdown()
    server1.startup()

    assertThrows(classOf[TimeoutException], () => producer.send(new ProducerRecord(topic, part, "key 2".getBytes, "value 2".getBytes)).get(5, TimeUnit.SECONDS))
    assertThrows(classOf[TimeoutException], () => adminClient.listTopics().names().get(5, TimeUnit.SECONDS))

    val producer2 = createProducer(configOverrides = configOverrides)
    producer2.send(new ProducerRecord(topic, part, 1L, "key 1".getBytes, "value 1".getBytes)).get(15, TimeUnit.SECONDS)
    assertEquals(0, consumer.poll(Duration.ofSeconds(5)).count)
  }
}
