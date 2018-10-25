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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureRecordMetadataTest {

  @Test(expected = ExecutionException.class)
  public void testFutureGetWithSeconds() throws ExecutionException, InterruptedException, TimeoutException {
    FutureRecordMetadata future = getFutureRecordMetadata(10L);
    future.chain(getFutureRecordMetadata(1000L));

    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void testFutureGetWithMilliSeconds() throws ExecutionException, InterruptedException, TimeoutException {
    FutureRecordMetadata future = getFutureRecordMetadata(10L);
    future.chain(getFutureRecordMetadata(1000L));

    future.get(5000, TimeUnit.MILLISECONDS);
  }

  private FutureRecordMetadata getFutureRecordMetadata(final long timeout) {
    return new FutureRecordMetadata(
        asyncRequest(timeout),
        0,
        RecordBatch.NO_TIMESTAMP,
        0L,
        0,
        0
    );
  }

  private ProduceRequestResult asyncRequest(final long timeout) {
    final ProduceRequestResult request = new ProduceRequestResult(new TopicPartition("topic", 0));
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(timeout);
        request.set(5L, RecordBatch.NO_TIMESTAMP, new CorruptRecordException());
        request.done();
      } catch (InterruptedException e) {
        // nothing to do
      }
    });
    thread.start();
    return request;
  }
}
