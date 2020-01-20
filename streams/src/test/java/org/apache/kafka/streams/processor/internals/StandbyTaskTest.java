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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.MockRestoreConsumer;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// TODO K9113: fix tests
@RunWith(EasyMockRunner.class)
public class StandbyTaskTest {

    private final String threadName = "threadName";
    private final String threadId = Thread.currentThread().getName();
    private final TaskId taskId = new TaskId(0, 0);

    private final String storeName1 = "store1";
    private final String storeName2 = "store2";
    private final String globalStoreName = "globalStore";
    private final String applicationId = "test-application";
    private final String storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
    private final String storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);

    private final TopicPartition partition = new TopicPartition(storeChangelogTopicName1, 0);
    private final MockKeyValueStore store1 = (MockKeyValueStore) new MockKeyValueStoreBuilder(storeName1, false).build();
    private final MockKeyValueStore store2 = (MockKeyValueStore) new MockKeyValueStoreBuilder(storeName2, true).build();
    private final ProcessorTopology topology = ProcessorTopologyFactories.withLocalStores(
        asList(store1, store2),
        mkMap(mkEntry(storeName1, storeChangelogTopicName1), mkEntry(storeName2, storeChangelogTopicName2))
    );
    private final TopicPartition globalTopicPartition = new TopicPartition(globalStoreName, 0);
    private final MockKeyValueStore globalStore =
        (MockKeyValueStore) new MockKeyValueStoreBuilder(globalTopicPartition.topic(), true).withLoggingDisabled().build();
    private final ProcessorTopology globalTopology = ProcessorTopologyFactories.withLocalStores(
        singletonList(globalStore),
        singletonMap(globalStoreName, globalStoreName)
    );
    private final StreamsMetricsImpl streamsMetrics =
        new StreamsMetricsImpl(new Metrics(), threadName, StreamsConfig.METRICS_LATEST);

    private File baseDir;
    private StreamsConfig config;
    private StateDirectory stateDirectory;
    private StandbyTask task;

    private StreamsConfig createConfig(final File baseDir) throws IOException {
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath()),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class.getName())
        )));
    }

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockRestoreConsumer<Integer, Integer> restoreStateConsumer = new MockRestoreConsumer<>(
        new IntegerSerializer(),
        new IntegerSerializer()
    );

    @Mock(type = MockType.NICE)
    private ChangelogReader changelogReader;

    @Mock(type = MockType.NICE)
    private ProcessorStateManager stateManager;

    @Mock(type = MockType.NICE)
    private RecordCollector recordCollector;

    @Before
    public void setup() throws Exception {
        restoreStateConsumer.reset();
        restoreStateConsumer.updatePartitions(storeChangelogTopicName1, asList(
            new PartitionInfo(storeChangelogTopicName1, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName1, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        restoreStateConsumer.updatePartitions(storeChangelogTopicName2, asList(
            new PartitionInfo(storeChangelogTopicName2, 0, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName2, 1, Node.noNode(), new Node[0], new Node[0]),
            new PartitionInfo(storeChangelogTopicName2, 2, Node.noNode(), new Node[0], new Node[0])
        ));
        baseDir = TestUtils.tempDirectory();
        config = createConfig(baseDir);
        stateDirectory = new StateDirectory(config, new MockTime(), true);
    }

    @After
    public void cleanup() throws IOException {
        if (task != null && !task.isClosed()) {
            task.closeClean();
            task = null;
        }
        Utils.delete(baseDir);
    }

    @Test
    public void shouldReturnChangelogOffsets() {
        EasyMock.expect(stateManager.changelogOffsets()).andReturn(Collections.singletonMap(partition, 50L));
        stateManager.registerStore(store1, store1.stateRestoreCallback);
        EasyMock.expectLastCall();
        stateManager.registerStore(store2, store2.stateRestoreCallback);
        EasyMock.expectLastCall();

        EasyMock.replay(stateManager);

        task = new StandbyTask(taskId,
            Collections.singleton(partition),
            topology,
            consumer,
            config,
            streamsMetrics,
            stateManager,
            stateDirectory);

        task.initializeStateStores();

        assertEquals(Collections.singletonMap(partition, 50L), task.restoredOffsets());

        EasyMock.verify(stateManager);
    }

    // TODO K9113: fix this
    /*
    @Test
    public void shouldRestoreToKTable() throws IOException {
        consumer.assign(Collections.singletonList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        task = new StandbyTask(
            taskId,
            globalTopicPartitions,
            globalTopology,
            consumer,
            createConfig(baseDir),
            streamsMetrics,
            stateManager,
            stateDirectory
        );
        task.initializeStateStores();

        // The commit offset is at 0L. Records should not be processed
        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(
            globalTopicPartition,
            asList(
                makeConsumerRecord(globalTopicPartition, 10, 1),
                makeConsumerRecord(globalTopicPartition, 20, 2),
                makeConsumerRecord(globalTopicPartition, 30, 3),
                makeConsumerRecord(globalTopicPartition, 40, 4),
                makeConsumerRecord(globalTopicPartition, 50, 5)
            )
        );
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(10L))));
        task.commit(); // update offset limits

        // The commit offset has not reached, yet.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(5, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(11L))));
        task.commit(); // update offset limits

        // one record should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(4, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(45L))));
        task.commit(); // update offset limits

        // The commit offset is now 45. All record except for the last one should be processed.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(50L))));
        task.commit(); // update offset limits

        // The commit offset is now 50. Still the last record remains.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(1, remaining.size());

        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(60L))));
        task.commit(); // update offset limits

        // The commit offset is now 60. No record should be left.
        remaining = task.update(globalTopicPartition, remaining);
        assertEquals(emptyList(), remaining);
    }
    */

    private ConsumerRecord<byte[], byte[]> makeConsumerRecord(final TopicPartition topicPartition,
                                                              final long offset,
                                                              final int key) {
        final IntegerSerializer integerSerializer = new IntegerSerializer();
        return new ConsumerRecord<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            0L,
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            integerSerializer.serialize(null, key),
            integerSerializer.serialize(null, 100)
        );
    }

    // TODO K9113: fix this
    /*
    @Test
    public void shouldNotGetConsumerCommittedOffsetIfThereAreNoRecordUpdates() throws IOException {
        final AtomicInteger committedCallCount = new AtomicInteger();

        final Consumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                committedCallCount.getAndIncrement();
                return super.committed(partitions);
            }
        };

        consumer.assign(Collections.singletonList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        task = new StandbyTask(
            taskId,
            globalTopicPartitions,
            globalTopology,
            consumer,
            createConfig(baseDir),
            streamsMetrics,
            stateManager,
            stateDirectory
        );
        task.initializeStateStores();
        assertThat(committedCallCount.get(), equalTo(0));

        task.update(globalTopicPartition, Collections.emptyList());
        // We should not make a consumer.committed() call because there are no new records.
        assertThat(committedCallCount.get(), equalTo(0));
    }
    */

    // TODO K9113: fix this
    /*
    @Test
    public void shouldGetConsumerCommittedOffsetsOncePerCommit() throws IOException {
        final AtomicInteger committedCallCount = new AtomicInteger();

        final Consumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
                committedCallCount.getAndIncrement();
                return super.committed(partitions);
            }
        };

        consumer.assign(Collections.singletonList(globalTopicPartition));
        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

        task = new StandbyTask(
            taskId,
            globalTopicPartitions,
            globalTopology,
            consumer,
            createConfig(baseDir),
            streamsMetrics,
            stateManager,
            stateDirectory
        );
        task.initializeStateStores();

        task.update(
            globalTopicPartition,
            Collections.singletonList(
                makeConsumerRecord(globalTopicPartition, 1, 1)
            )
        );
        assertThat(committedCallCount.get(), equalTo(1));

        task.update(
            globalTopicPartition,
            Collections.singletonList(
                makeConsumerRecord(globalTopicPartition, 1, 1)
            )
        );
        // We should not make another consumer.committed() call until we commit
        assertThat(committedCallCount.get(), equalTo(1));

        task.commit();
        task.update(
            globalTopicPartition,
            Collections.singletonList(
                makeConsumerRecord(globalTopicPartition, 1, 1)
            )
        );
        // We committed so we're allowed to make another consumer.committed() call
        assertThat(committedCallCount.get(), equalTo(2));
    }
    */

    @Test
    public void shouldCheckpointStoreOffsetsOnCommit() throws IOException {
        stateManager.checkpoint(EasyMock.eq(Collections.emptyMap()));
        EasyMock.replay(stateManager);

        final TaskId taskId = new TaskId(0, 0);
        task = new StandbyTask(
            taskId,
            Utils.mkSet(globalTopicPartition),
            globalTopology,
            consumer,
            config,
            streamsMetrics,
            stateManager,
            stateDirectory
        );
        task.initializeStateStores();

        task.commit();

        EasyMock.verify(stateManager);
    }

    @Test
    public void shouldCloseStateMangerOnTaskCloseWhenCommitFailed() throws Exception {
        consumer.assign(Collections.singletonList(globalTopicPartition));
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(globalTopicPartition.topic(), globalTopicPartition.partition()),
                             new OffsetAndMetadata(100L));
        consumer.commitSync(committedOffsets);

        restoreStateConsumer.updatePartitions(
            globalStoreName,
            Collections.singletonList(new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]))
        );

        final AtomicBoolean closedStateManager = new AtomicBoolean(false);
        task = new StandbyTask(
            taskId,
            Utils.mkSet(globalTopicPartition),
            globalTopology,
            consumer,
            config,
            streamsMetrics,
            stateManager,
            stateDirectory
        ) {
            @Override
            public void commit() {
                throw new RuntimeException("KABOOM!");
            }

            @Override
            void closeStateManager(final boolean clean) throws ProcessorStateException {
                closedStateManager.set(true);
            }
        };
        task.initializeStateStores();
        try {
            task.closeClean();
            fail("should have thrown exception");
        } catch (final Exception e) {
            // expected
            task = null;
        }
        assertTrue(closedStateManager.get());
    }

    private MetricName setupCloseTaskMetric() {
        final MetricName metricName = new MetricName("name", "group", "description", Collections.emptyMap());
        final Sensor sensor = streamsMetrics.threadLevelSensor(threadId, "task-closed", Sensor.RecordingLevel.INFO);
        sensor.add(metricName, new CumulativeSum());
        return metricName;
    }

    private void verifyCloseTaskMetric(final double expected,
                                       final StreamsMetricsImpl streamsMetrics,
                                       final MetricName metricName) {
        final KafkaMetric metric = (KafkaMetric) streamsMetrics.metrics().get(metricName);
        final double totalCloses = metric.measurable().measure(metric.config(), System.currentTimeMillis());
        assertThat(totalCloses, equalTo(expected));
    }

    @Test
    public void shouldRecordTaskClosedMetricOnClose() {
        final MetricName metricName = setupCloseTaskMetric();
        final StandbyTask task = new StandbyTask(
            taskId,
            Utils.mkSet(globalTopicPartition),
            globalTopology,
            consumer,
            config,
            streamsMetrics,
            stateManager,
            stateDirectory
        );

        task.closeClean();

        final double expectedCloseTaskMetric = 1.0;
        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
    }
}
