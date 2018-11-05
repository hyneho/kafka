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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.trogdor.task.TaskWorker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Properties;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class ConsumeBenchWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(ConsumeBenchWorker.class);

    private static final int THROTTLE_PERIOD_MS = 100;

    private final String id;
    private final ConsumeBenchSpec spec;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService executor;
    private WorkerStatusTracker workerStatus;
    private StatusUpdater statusUpdater;
    private Future<?> statusUpdaterFuture;
    private KafkaFutureImpl<String> doneFuture;
    private KafkaConsumer<byte[], byte[]> consumer;
    public ConsumeBenchWorker(String id, ConsumeBenchSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("ConsumeBenchWorker is already running.");
        }
        log.info("{}: Activating ConsumeBenchWorker with {}", id, spec);
        this.statusUpdater = new StatusUpdater();
        this.executor = Executors.newScheduledThreadPool(
            spec.consumerCount() + 2, // 1 thread for all the ConsumeStatusUpdater and 1 for the StatusUpdater
            ThreadUtils.createThreadFactory("ConsumeBenchWorkerThread%d", false));
        this.statusUpdaterFuture = executor.scheduleAtFixedRate(this.statusUpdater, 2, 1, TimeUnit.MINUTES);
        this.workerStatus = status;
        this.doneFuture = doneFuture;
        executor.submit(new Prepare());
    }

    public class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                List<Future<Void>> consumeTasks = new ArrayList<>();
                for (ConsumeMessages task : consumeTasks()) {
                    consumeTasks.add(executor.submit(task));
                }
                executor.submit(new CloseStatusUpdater(consumeTasks));
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }

        private List<ConsumeMessages> consumeTasks() {
            List<ConsumeMessages> tasks = new ArrayList<>();
            String consumerGroup = consumerGroup();
            int consumerCount = spec.consumerCount();
            Map<String, List<TopicPartition>> partitionsByTopic = spec.materializeTopics();
            boolean toUseGroupPartitionAssignment = partitionsByTopic.values().stream().allMatch(List::isEmpty);

            if (!toUseGroupPartitionAssignment && !toUseRandomConsumeGroup() && consumerCount > 1)
                throw new ConfigException(String.format("Will not split partitions across consumers from the %s group", consumerGroup));

            String clientId = clientId(0);
            consumer = consumer(consumerGroup, clientId);
            if (!toUseGroupPartitionAssignment) {
                List<TopicPartition> partitions = populatePartitionsByTopic(consumer, partitionsByTopic)
                    .values().stream().flatMap(List::stream).collect(Collectors.toList());
                tasks.add(new ConsumeMessages(consumer, clientId, partitions));

                for (int i = 0; i < consumerCount - 1; i++) {
                    clientId = clientId(i + 1);
                    tasks.add(new ConsumeMessages(consumer(consumerGroup(), clientId), clientId, partitions));
                }
            } else {
                Set<String> topics = partitionsByTopic.keySet();
                tasks.add(new ConsumeMessages(consumer, clientId, topics));

                for (int i = 0; i < consumerCount - 1; i++) {
                    clientId = clientId(i + 1);
                    tasks.add(new ConsumeMessages(consumer(consumerGroup(), clientId), clientId, topics));
                }
            }

            return tasks;
        }

        private String clientId(int idx) {
            return String.format("consumer.%s-%d", id, idx);
        }

        /**
         * Creates a new KafkaConsumer instance
         */
        private KafkaConsumer<byte[], byte[]> consumer(String consumerGroup, String idx) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("consumer.%s-%s", id, idx));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
            // these defaults maybe over-written by the user-specified commonClientConf or consumerConf
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf());
            return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }

        private String consumerGroup() {
            return toUseRandomConsumeGroup()
                ? "consume-bench-" + UUID.randomUUID().toString()
                : spec.consumerGroup();
        }

        private boolean toUseRandomConsumeGroup() {
            return spec.consumerGroup().equals(ConsumeBenchSpec.EMPTY_CONSUMER_GROUP);
        }

        private Map<String, List<TopicPartition>> populatePartitionsByTopic(KafkaConsumer<byte[], byte[]> consumer,
                                                                         Map<String, List<TopicPartition>> materializedTopics) {
            // fetch partitions for topics who do not have any listed
            for (Map.Entry<String, List<TopicPartition>> entry : materializedTopics.entrySet()) {
                String topicName = entry.getKey();
                List<TopicPartition> partitions = entry.getValue();

                if (partitions.isEmpty()) {
                    List<TopicPartition> fetchedPartitions = consumer.partitionsFor(topicName).stream()
                        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                        .collect(Collectors.toList());
                    partitions.addAll(fetchedPartitions);
                }

                materializedTopics.put(topicName, partitions);
            }

            return materializedTopics;
        }
    }

    public class ConsumeMessages implements Callable<Void> {
        private final Histogram latencyHistogram;
        private final Histogram messageSizeHistogram;
        private final Future<?> statusUpdaterFuture;
        private final Throttle throttle;
        private final String clientId;
        private final KafkaConsumer<byte[], byte[]> consumer;

        private ConsumeMessages(KafkaConsumer<byte[], byte[]> consumer, String clientId) {
            this.latencyHistogram = new Histogram(5000);
            this.messageSizeHistogram = new Histogram(2 * 1024 * 1024);
            this.clientId = clientId;
            this.statusUpdaterFuture = executor.scheduleAtFixedRate(
                new ConsumeStatusUpdater(latencyHistogram, messageSizeHistogram, clientId), 1, 1, TimeUnit.MINUTES);
            int perPeriod = WorkerUtils.perSecToPerPeriod(
                spec.targetMessagesPerSec(), THROTTLE_PERIOD_MS);
            this.throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
            this.consumer = consumer;
        }

        ConsumeMessages(KafkaConsumer<byte[], byte[]> consumer, String clientId, Set<String> topics) {
            this(consumer, clientId);
            log.info("Will consume from topics {} via dynamic group assignment.", topics);
            this.consumer.subscribe(topics);
        }

        ConsumeMessages(KafkaConsumer<byte[], byte[]> consumer, String clientId, List<TopicPartition> partitions) {
            this(consumer, clientId);
            log.info("Will consume from topic partitions {} via manual assignment.", partitions);
            this.consumer.assign(partitions);
        }

        @Override
        public Void call() throws Exception {
            long messagesConsumed = 0;
            long bytesConsumed = 0;
            long startTimeMs = Time.SYSTEM.milliseconds();
            long startBatchMs = startTimeMs;
            int maxMessages = spec.maxMessages();
            try {
                while (messagesConsumed < maxMessages) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(50));
                    if (records.isEmpty()) {
                        continue;
                    }
                    long endBatchMs = Time.SYSTEM.milliseconds();
                    long elapsedBatchMs = endBatchMs - startBatchMs;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        messagesConsumed++;
                        long messageBytes = 0;
                        if (record.key() != null) {
                            messageBytes += record.serializedKeySize();
                        }
                        if (record.value() != null) {
                            messageBytes += record.serializedValueSize();
                        }
                        latencyHistogram.add(elapsedBatchMs);
                        messageSizeHistogram.add(messageBytes);
                        bytesConsumed += messageBytes;
                        if (messagesConsumed >= maxMessages)
                            break;

                        throttle.increment();
                    }
                    startBatchMs = Time.SYSTEM.milliseconds();
                }
            } catch (Exception e) {
                // TODO: Should we close task on consumer failure?
                WorkerUtils.abort(log, "ConsumeRecords", e, doneFuture);
            } finally {
                statusUpdaterFuture.cancel(false);
                StatusData statusData =
                    new ConsumeStatusUpdater(latencyHistogram, messageSizeHistogram, clientId).update();
                long curTimeMs = Time.SYSTEM.milliseconds();
                log.info("{} Consumed total number of messages={}, bytes={} in {} ms.  status: {}",
                         clientId, messagesConsumed, bytesConsumed, curTimeMs - startTimeMs, statusData);
            }
            doneFuture.complete("");
            consumer.unsubscribe();
            consumer.close();
            return null;
        }
    }

    public class CloseStatusUpdater implements Runnable {
        private final List<Future<Void>> consumeTasks;

        CloseStatusUpdater(List<Future<Void>> consumeTasks) {
            this.consumeTasks = consumeTasks;
        }

        @Override
        public void run() {
            while (!consumeTasks.stream().allMatch(Future::isDone)) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    log.debug("{} was interrupted. Closing...", this.getClass().getName());
                    break; // close the thread
                }
            }
            statusUpdaterFuture.cancel(false);
            statusUpdater.update();
        }
    }

    class StatusUpdater implements Runnable {
        final Map<String, JsonNode> statuses;
        final Map<String, Boolean> statusesFailed;

        StatusUpdater() {
            statuses = new HashMap<>();
            statusesFailed = new HashMap<>();
        }

        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                WorkerUtils.abort(log, "ConsumeStatusUpdater", e, doneFuture);
            }
        }

        /**
         * Fails a single status updater. If all have failed, the worker is aborted
         */
        synchronized void failStatus(String clientId) throws KafkaException {
            statusesFailed.put(clientId, true);
            if (!statusesFailed.values().contains(false)) {
                // all statuses have failed, abort the worker
                WorkerUtils.abort(log, "StatusUpdater",
                    new KafkaException("All consumer's status updaters have failed"), doneFuture);
            }
        }

        synchronized void update() {
            workerStatus.update(JsonUtil.JSON_SERDE.valueToTree(statuses));
        }

        synchronized void updateConsumeStatus(String clientId, StatusData status) {
            if (!statusesFailed.containsKey(clientId))
                statusesFailed.put(clientId, false);

            statuses.put(clientId, JsonUtil.JSON_SERDE.valueToTree(status));
        }
    }

    /**
     * Runnable class that updates the status of a single consumer
     */
    public class ConsumeStatusUpdater implements Runnable {
        private final Histogram latencyHistogram;
        private final Histogram messageSizeHistogram;
        private final String clientId;

        ConsumeStatusUpdater(Histogram latencyHistogram, Histogram messageSizeHistogram, String clientId) {
            this.latencyHistogram = latencyHistogram;
            this.messageSizeHistogram = messageSizeHistogram;
            this.clientId = clientId;
        }

        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                log.warn("ConsumeStatusUpdater caught an exception: ", e);
                statusUpdater.failStatus(clientId);
                throw new KafkaException(e);
            }
        }

        StatusData update() {
            Histogram.Summary latSummary = latencyHistogram.summarize(StatusData.PERCENTILES);
            Histogram.Summary msgSummary = messageSizeHistogram.summarize(StatusData.PERCENTILES);
            StatusData statusData = new StatusData(
                latSummary.numSamples(),
                (long) (msgSummary.numSamples() * msgSummary.average()),
                (long) msgSummary.average(),
                latSummary.average(),
                latSummary.percentiles().get(0).value(),
                latSummary.percentiles().get(1).value(),
                latSummary.percentiles().get(2).value());
            statusUpdater.updateConsumeStatus(clientId, statusData);
            log.info("Status={}", JsonUtil.toJsonString(statusData));
            return statusData;
        }
    }

    public static class StatusData {
        private final long totalMessagesReceived;
        private final long totalBytesReceived;
        private final long averageMessageSizeBytes;
        private final float averageLatencyMs;
        private final int p50LatencyMs;
        private final int p95LatencyMs;
        private final int p99LatencyMs;

        /**
         * The percentiles to use when calculating the histogram data.
         * These should match up with the p50LatencyMs, p95LatencyMs, etc. fields.
         */
        final static float[] PERCENTILES = {0.5f, 0.95f, 0.99f};

        @JsonCreator
        StatusData(@JsonProperty("totalMessagesReceived") long totalMessagesReceived,
                   @JsonProperty("totalBytesReceived") long totalBytesReceived,
                   @JsonProperty("averageMessageSizeBytes") long averageMessageSizeBytes,
                   @JsonProperty("averageLatencyMs") float averageLatencyMs,
                   @JsonProperty("p50LatencyMs") int p50latencyMs,
                   @JsonProperty("p95LatencyMs") int p95latencyMs,
                   @JsonProperty("p99LatencyMs") int p99latencyMs) {
            this.totalMessagesReceived = totalMessagesReceived;
            this.totalBytesReceived = totalBytesReceived;
            this.averageMessageSizeBytes = averageMessageSizeBytes;
            this.averageLatencyMs = averageLatencyMs;
            this.p50LatencyMs = p50latencyMs;
            this.p95LatencyMs = p95latencyMs;
            this.p99LatencyMs = p99latencyMs;
        }

        @JsonProperty
        public long totalMessagesReceived() {
            return totalMessagesReceived;
        }

        @JsonProperty
        public long totalBytesReceived() {
            return totalBytesReceived;
        }

        @JsonProperty
        public long averageMessageSizeBytes() {
            return averageMessageSizeBytes;
        }

        @JsonProperty
        public float averageLatencyMs() {
            return averageLatencyMs;
        }

        @JsonProperty
        public int p50LatencyMs() {
            return p50LatencyMs;
        }

        @JsonProperty
        public int p95LatencyMs() {
            return p95LatencyMs;
        }

        @JsonProperty
        public int p99LatencyMs() {
            return p99LatencyMs;
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("ConsumeBenchWorker is not running.");
        }
        log.info("{}: Deactivating ConsumeBenchWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        Utils.closeQuietly(consumer, "consumer");
        this.consumer = null;
        this.executor = null;
        this.statusUpdater = null;
        this.statusUpdaterFuture = null;
        this.workerStatus = null;
        this.doneFuture = null;
    }

}
