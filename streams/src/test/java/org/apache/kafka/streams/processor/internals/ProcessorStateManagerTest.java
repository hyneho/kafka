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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockBatchingStateRestoreListener;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcessorStateManagerTest {

    private final Set<TopicPartition> noPartitions = Collections.emptySet();
    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    private final MockKeyValueStore nonPersistentStore = new MockKeyValueStore(nonPersistentStoreName, false);
    private final TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private final String storeName = "mockKeyValueStore";
    private final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private final TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private final TaskId taskId = new TaskId(0, 1);
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(storeName, true);
    private final byte[] key = new byte[] {0x0, 0x0, 0x0, 0x1};
    private final byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);
    private final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(changelogTopic, 0, 0, key, value);
    private final LogContext logContext = new LogContext("process-state-manager-test ");

    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;

    @Before
    public void setup() {
        baseDir = TestUtils.tempDirectory();

        stateDirectory = new StateDirectory(new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getPath());
            }
        }), new MockTime(), true);
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME);
        checkpoint = new OffsetCheckpoint(checkpointFile);
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

    @Test
    public void shouldRestoreStoreWithBatchingRestoreSpecification() {
        final TaskId taskId = new TaskId(0, 2);
        final MockBatchingStateRestoreListener batchingRestoreCallback = new MockBatchingStateRestoreListener();

        final KeyValue<byte[], byte[]> expectedKeyValue = KeyValue.pair(key, value);

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.registerStore(persistentStore, batchingRestoreCallback);
            stateMgr.restore(
                persistentStorePartition,
                singletonList(consumerRecord)
            );
            assertThat(batchingRestoreCallback.getRestoredRecords().size(), is(1));
            assertTrue(batchingRestoreCallback.getRestoredRecords().contains(expectedKeyValue));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void shouldRestoreStoreWithSinglePutRestoreSpecification() {
        final TaskId taskId = new TaskId(0, 2);
        final Integer intKey = 1;

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.restore(
                persistentStorePartition,
                singletonList(consumerRecord)
            );
            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(intKey));
            assertEquals(9, persistentStore.values.get(0).length);
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void shouldConvertDataOnRestoreIfStoreImplementsTimestampedBytesStore() {
        final TaskId taskId = new TaskId(0, 2);
        final Integer intKey = 1;

        final MockKeyValueStore persistentStore = getConverterStore();
        final ProcessorStateManager stateMgr = getStandByStateManager(taskId);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.restore(
                persistentStorePartition,
                singletonList(consumerRecord)
            );
            assertThat(persistentStore.keys.size(), is(1));
            assertTrue(persistentStore.keys.contains(intKey));
            assertEquals(17, persistentStore.values.get(0).length);
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testRegisterPersistentStore() {
        final TaskId taskId = new TaskId(0, 2);

        final MockKeyValueStore persistentStore = getPersistentStore();
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreName)
            ),
            changelogReader,
            logContext);

        try {
            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testRegisterNonPersistentStore() {
        final MockKeyValueStore nonPersistentStore =
            new MockKeyValueStore(nonPersistentStoreName, false); // non persistent store
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 2),
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            mkMap(
                mkEntry(persistentStoreName, persistentStoreTopicName),
                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
            ),
            changelogReader,
            logContext);

        try {
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final long storeTopic1LoadedCheckpoint = 10L;
        final String storeName1 = "store1";
        final String storeName2 = "store2";
        final String storeName3 = "store3";

        final String storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
        final String storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
        final String storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

        final Map<String, String> storeToChangelogTopic = new HashMap<>();
        storeToChangelogTopic.put(storeName1, storeTopicName1);
        storeToChangelogTopic.put(storeName2, storeTopicName2);
        storeToChangelogTopic.put(storeName3, storeTopicName3);

        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(
            new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME)
        );
        checkpoint.write(singletonMap(new TopicPartition(storeTopicName1, 0), storeTopic1LoadedCheckpoint));

        final TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        final TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        final TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

        final MockKeyValueStore store1 = new MockKeyValueStore(storeName1, true);
        final MockKeyValueStore store2 = new MockKeyValueStore(storeName2, true);
        final MockKeyValueStore store3 = new MockKeyValueStore(storeName3, true);

        // if there is a source partition, inherit the partition id
        final Set<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            sourcePartitions,
            AbstractTask.TaskType.STANDBY,
            stateDirectory,
            storeToChangelogTopic,
            changelogReader,
            logContext);

        try {
            stateMgr.registerStore(store1, store1.stateRestoreCallback);
            stateMgr.registerStore(store2, store2.stateRestoreCallback);
            stateMgr.registerStore(store3, store3.stateRestoreCallback);

            final Map<TopicPartition, Long> changeLogOffsets = stateMgr.changelogOffsets();

            assertEquals(3, changeLogOffsets.size());
            assertTrue(changeLogOffsets.containsKey(partition1));
            assertTrue(changeLogOffsets.containsKey(partition2));
            assertTrue(changeLogOffsets.containsKey(partition3));
            assertEquals(storeTopic1LoadedCheckpoint, (long) changeLogOffsets.get(partition1));
            assertEquals(-1L, (long) changeLogOffsets.get(partition2));
            assertEquals(-1L, (long) changeLogOffsets.get(partition3));

        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testGetStore() {
        final MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(nonPersistentStoreName, false);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);
        try {
            stateMgr.registerStore(mockKeyValueStore, mockKeyValueStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(mockKeyValueStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(true);
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
        checkpoint.write(emptyMap());

        // set up ack'ed offsets
        final HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
        ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
        ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
                  mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
            changelogReader,
            logContext);
        try {
            // make sure the checkpoint file is not written yet
            assertFalse(checkpointFile.exists());

            stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
            stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        } finally {
            // close the state manager with the ack'ed offsets
            stateMgr.flush();
            stateMgr.checkpoint(ackedOffsets);
            stateMgr.close(true);
        }
        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // make sure that flush is called in the proper order
        assertThat(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));

        // the checkpoint file should contain an offset from the persistent store only.
        final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
        assertThat(checkpointedOffsets, is(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 124L)));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);
        stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }

    @Test
    public void shouldIgnoreIrrelevantLoadedCheckpoints() throws IOException {
        final Map<TopicPartition, Long> offsets = mkMap(
            mkEntry(persistentStorePartition, 99L),
            mkEntry(new TopicPartition("ignoreme", 1234), 12L)
        );
        checkpoint.write(offsets);

        final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStorePartition.topic()),
            changelogReader,
            logContext);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

        stateMgr.checkpoint(emptyMap());
        stateMgr.close(true);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 110L)));
    }

    @Test
    public void shouldOverrideLoadedCheckpointsWithRestoredCheckpoints() throws IOException {
        final Map<TopicPartition, Long> offsets = singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStorePartition.topic()),
            changelogReader,
            logContext);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

        stateMgr.checkpoint(emptyMap());
        stateMgr.close(true);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 110L)));
    }

    @Test
    public void shouldIgnoreIrrelevantRestoredCheckpoints() throws IOException {
        final Map<TopicPartition, Long> offsets = singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStorePartition.topic()),
            changelogReader,
            logContext);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        // should ignore irrelevant topic partitions
        changelogReader.setRestoredOffsets(mkMap(
            mkEntry(persistentStorePartition, 110L),
            mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
        ));

        stateMgr.checkpoint(emptyMap());
        stateMgr.close(true);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 110L)));
    }

    @Test
    public void shouldOverrideRestoredOffsetsWithProcessedOffsets() throws IOException {
        final Map<TopicPartition, Long> offsets = singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);

        final MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStorePartition.topic()),
            changelogReader,
            logContext);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        // should ignore irrelevant topic partitions
        changelogReader.setRestoredOffsets(mkMap(
            mkEntry(persistentStorePartition, 110L),
            mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
        ));

        // should ignore irrelevant topic partitions
        stateMgr.checkpoint(mkMap(
            mkEntry(persistentStorePartition, 220L),
            mkEntry(new TopicPartition("ignoreme", 42), 9000L)
        ));
        stateMgr.close(true);
        final Map<TopicPartition, Long> read = checkpoint.read();

        // the checkpoint gets incremented to be the log position _after_ the committed offset
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 221L)));
    }

    @Test
    public void shouldWriteCheckpointForPersistentLogEnabledStore() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            logContext);
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 11L)));
    }

    @Test
    public void shouldWriteCheckpointForStandbyReplica() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.STANDBY,
            stateDirectory,
            singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            logContext);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);
        final byte[] bytes = Serdes.Integer().serializer().serialize("", 10);
        stateMgr.restore(
            persistentStorePartition,
            singletonList(new ConsumerRecord<>("", 0, 888L, bytes, bytes)));

        stateMgr.checkpoint(emptyMap());

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(singletonMap(persistentStorePartition, 889L)));

    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws IOException {
        final TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.STANDBY,
            stateDirectory,
            singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
            changelogReader,
            logContext);

        stateMgr.registerStore(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        stateMgr.checkpoint(singletonMap(topicPartition, 876L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(emptyMap()));
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws IOException {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.STANDBY,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);

        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(singletonMap(persistentStorePartition, 987L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(emptyMap()));
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);

        try {
            stateManager.registerStore(new MockKeyValueStore(StateManagerUtil.CHECKPOINT_FILE_NAME, true), null);
            fail("should have thrown illegal argument exception when store name same as checkpoint file");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            emptyMap(),
            changelogReader,
            logContext);

        stateManager.registerStore(mockKeyValueStore, null);

        try {
            stateManager.registerStore(mockKeyValueStore, null);
            fail("should have thrown illegal argument exception when store with same name already registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }

    }

    @Test
    public void shouldThrowProcessorStateExceptionOnFlushIfStoreThrowsAnException() {

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            logContext);

        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.flush();
            fail("Should throw ProcessorStateException if store flush throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException() {

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            logContext);

        final MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.registerStore(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.close(true);
            fail("Should throw ProcessorStateException if store close throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    // if the optional is absent, it'll throw an exception and fail the test.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void shouldLogAWarningIfCheckpointThrowsAnIOException() {
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        final ProcessorStateManager stateMgr;
        try {
            stateMgr = new ProcessorStateManager(
                taskId,
                noPartitions,
                AbstractTask.TaskType.ACTIVE,
                stateDirectory,
                singletonMap(persistentStore.name(), persistentStoreTopicName),
                changelogReader,
                logContext);
        } catch (final ProcessorStateException e) {
            e.printStackTrace();
            throw new AssertionError(e);
        }
        stateMgr.registerStore(persistentStore, persistentStore.stateRestoreCallback);

        stateDirectory.clean();
        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
        LogCaptureAppender.unregister(appender);

        boolean foundExpectedLogMessage = false;
        for (final LogCaptureAppender.Event event : appender.getEvents()) {
            if ("WARN".equals(event.getLevel())
                && event.getMessage().startsWith("process-state-manager-test Failed to write offset checkpoint file to [")
                && event.getMessage().endsWith(".checkpoint]")
                && event.getThrowableInfo().get().startsWith("java.io.FileNotFoundException: ")) {

                foundExpectedLogMessage = true;
                break;
            }
        }
        assertTrue(foundExpectedLogMessage);
    }

    @Test
    public void shouldFlushAllStoresEvenIfStoreThrowsException() {
        final AtomicBoolean flushedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true) {
            @Override
            public void flush() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true) {
            @Override
            public void flush() {
                flushedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            logContext);

        stateManager.registerStore(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.registerStore(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.flush();
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(flushedStore.get());
    }

    @Test
    public void shouldCloseAllStoresEvenIfStoreThrowsExcepiton() {

        final AtomicBoolean closedStore = new AtomicBoolean(false);

        final MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        final MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true) {
            @Override
            public void close() {
                closedStore.set(true);
            }
        };
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            logContext);

        stateManager.registerStore(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.registerStore(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.close(true);
        } catch (final ProcessorStateException expected) { /* ignode */ }
        Assert.assertTrue(closedStore.get());
    }

    @Test
    public void shouldDeleteCheckpointFileOnCreationIfEosEnabled() throws IOException {
        checkpoint.write(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L));
        assertTrue(checkpointFile.exists());

        ProcessorStateManager stateManager = null;
        try {
            stateManager = new ProcessorStateManager(
                taskId,
                noPartitions,
                AbstractTask.TaskType.ACTIVE,
                stateDirectory,
                emptyMap(),
                changelogReader,
                logContext);

            assertFalse(checkpointFile.exists());
        } finally {
            if (stateManager != null) {
                stateManager.close(true);
            }
        }
    }

    private ProcessorStateManager getStandByStateManager(final TaskId taskId) {
        return new ProcessorStateManager(
            taskId,
            noPartitions,
            AbstractTask.TaskType.STANDBY,
            stateDirectory,
            singletonMap(persistentStoreName, persistentStoreTopicName),
            changelogReader,
            logContext);
    }

    private MockKeyValueStore getPersistentStore() {
        return new MockKeyValueStore("persistentStore", true);
    }

    private MockKeyValueStore getConverterStore() {
        return new ConverterStore("persistentStore", true);
    }

    private class ConverterStore extends MockKeyValueStore implements TimestampedBytesStore {
        ConverterStore(final String name,
                       final boolean persistent) {
            super(name, persistent);
        }
    }
}
