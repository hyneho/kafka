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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SourceTaskOffsetCommitterTest {

    private final ConcurrentHashMap<ConnectorTaskId, ScheduledFuture<?>> committers = new ConcurrentHashMap<>();

    @Mock
    private ScheduledExecutorService executor;
    @Mock
    private ScheduledFuture<?> commitFuture;
    @Mock
    private ScheduledFuture<?> taskFuture;
    @Mock
    private ConnectorTaskId taskId;
    @Mock
    private WorkerSourceTask task;

    private SourceTaskOffsetCommitter committer;

    private static final long DEFAULT_OFFSET_COMMIT_INTERVAL_MS = 1000;

    @Before
    public void setup() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms",
                Long.toString(DEFAULT_OFFSET_COMMIT_INTERVAL_MS));
        WorkerConfig config = new StandaloneConfig(workerProps);
        committer = new SourceTaskOffsetCommitter(config, executor, committers);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSchedule() {
        ArgumentCaptor<Runnable> taskWrapper = ArgumentCaptor.forClass(Runnable.class);

        when(executor.scheduleWithFixedDelay(
                taskWrapper.capture(), eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS),
                eq(DEFAULT_OFFSET_COMMIT_INTERVAL_MS), eq(TimeUnit.MILLISECONDS))
        ).thenReturn((ScheduledFuture) commitFuture);

        committer.schedule(taskId, task);
        assertNotNull(taskWrapper.getValue());
        assertEquals(singletonMap(taskId, commitFuture), committers);
    }

    @Test
    public void testClose() throws Exception {
        long timeoutMs = 1000;

        // Normal termination, where termination times out.
        when(executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)).thenReturn(false);

        committer.close(timeoutMs);

        // Termination interrupted
        when(executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException());

        committer.close(timeoutMs);

        verify(executor, times(2)).shutdown();
    }

    @Test
    public void testRemove() throws Exception {
        // Try to remove a non-existing task
        assertTrue(committers.isEmpty());
        committer.remove(taskId);
        assertTrue(committers.isEmpty());

        // Try to remove an existing task
        when(taskFuture.cancel(false)).thenReturn(false);
        when(taskFuture.isDone()).thenReturn(false);
        when(taskFuture.get())
                .thenReturn(null)
                .thenThrow(new CancellationException())
                .thenThrow(new InterruptedException());
        when(taskId.connector()).thenReturn("MyConnector");
        when(taskId.task()).thenReturn(1);

        committers.put(taskId, taskFuture);
        committer.remove(taskId);
        assertTrue(committers.isEmpty());

        // Try to remove a cancelled task
        committers.put(taskId, taskFuture);
        committer.remove(taskId);
        assertTrue(committers.isEmpty());

        // Try to remove an interrupted task
        try {
            committers.put(taskId, taskFuture);
            committer.remove(taskId);
            fail("Expected ConnectException to be raised");
        } catch (ConnectException e) {
            //ignore
        }
    }

}
