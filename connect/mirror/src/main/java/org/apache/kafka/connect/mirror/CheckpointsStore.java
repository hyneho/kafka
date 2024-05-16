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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.mirror.MirrorCheckpointConfig.CHECKPOINTS_TARGET_CONSUMER_ROLE;

/**
 * Reads once the Kafka log for checkpoints and populates a map of
 * checkpoints per consumer group
 */
public class CheckpointsStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CheckpointsStore.class);

    private final MirrorCheckpointTaskConfig config;
    private final Set<String> consumerGroups;

    private TopicAdmin cpAdmin = null;
    private KafkaBasedLog<byte[], byte[]> backingStore = null;
    private Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup;

    private volatile boolean loadSuccess = false;
    private volatile boolean isInitialized = false;

    public CheckpointsStore(MirrorCheckpointTaskConfig config, Set<String> consumerGroups) {
        this.config = config;
        this.consumerGroups = new HashSet<>(consumerGroups);
    }

    // for testing
    CheckpointsStore(Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup) {
        this.config = null;
        this.consumerGroups = null;
        this.checkpointsPerConsumerGroup = checkpointsPerConsumerGroup;
        isInitialized = true;
        loadSuccess =  true;
    }

    // potentially long running
    public void start()  {
        checkpointsPerConsumerGroup = readCheckpoints();
        isInitialized = true;
        log.trace("Checkpoints store content : {}", checkpointsPerConsumerGroup);
    }

    public boolean loadSuccess() {
        return loadSuccess;
    }

    public boolean isInitialized() {
        return isInitialized;
    }


    // return a mutable map - it is expected to be mutated by the Task
    public Map<String, Map<TopicPartition, Checkpoint>> contents() {
        return checkpointsPerConsumerGroup;
    }

    @Override
    public void close() {
        Utils.closeQuietly(backingStore != null ? backingStore::stop : null, "backing store for previous Checkpoints");
        Utils.closeQuietly(cpAdmin, "admin client for previous Checkpoints");
        cpAdmin = null;
        backingStore = null;
    }

    // read the checkpoints topic to initialize the checkpointsPerConsumerGroup state
    // the callback may only handle errors thrown by consumer.poll in KafkaBasedLog
    // e.g. unauthorized to read from topic (non-retriable)
    // if any are encountered, treat the loading of Checkpoints as failed.
    public Map<String, Map<TopicPartition, Checkpoint>> readCheckpoints() {
        Map<String, Map<TopicPartition, Checkpoint>> checkpoints = new HashMap<>();
        Callback<ConsumerRecord<byte[], byte[]>> consumedCallback = new Callback<ConsumerRecord<byte[], byte[]>>() {
            @Override
            public void onCompletion(Throwable error, ConsumerRecord<byte[], byte[]> cpRecord) {
                if (error != null) {
                    // if there is no authorization to READ from the topic, we must throw an error
                    // to stop the KafkaBasedLog forever looping attempting to read to end
                    checkpoints.clear();
                    if (error instanceof RuntimeException) {
                        throw (RuntimeException) error;
                    } else {
                        throw new RuntimeException(error);
                    }
                } else {
                    try {
                        Checkpoint cp = Checkpoint.deserializeRecord(cpRecord);
                        if (consumerGroups.contains(cp.consumerGroupId())) {
                            Map<TopicPartition, Checkpoint> cps = checkpoints.computeIfAbsent(cp.consumerGroupId(), ignored1 -> new HashMap<>());
                            cps.put(cp.topicPartition(), cp);
                        }
                    } catch (SchemaException ex) {
                        log.warn("Ignored invalid checkpoint record at offset {}", cpRecord.offset(), ex);
                    }
                }
            }
        };

        long startTime = System.currentTimeMillis();
        try {
            readCheckpointsImpl(config, consumedCallback);
            log.info("Loading Checkpoints topic took {}ms", System.currentTimeMillis() - startTime);
            loadSuccess = true;
        } catch (Exception error) {
            loadSuccess = false;
            if (error instanceof AuthorizationException) {
                log.warn("Not authorized to access checkpoints topic {} - this will degrade offset translation as fewer checkpoints may be emitted", config.checkpointsTopic(), error);
            } else {
                log.info("Exception encountered loading Checkpoint topic {} - this will degrade offset translation as fewer checkpoints may be emitted", config.checkpointsTopic(), error);
            }
        }
        return checkpoints;
    }

    // accessible for testing
    void readCheckpointsImpl(MirrorCheckpointTaskConfig config, Callback<ConsumerRecord<byte[], byte[]>> consumedCallback) {
        try {
            cpAdmin = new TopicAdmin(
                    config.targetAdminConfig("checkpoint-target-admin"),
                    config.forwardingAdmin(config.targetAdminConfig("checkpoint-target-admin")));

            backingStore = KafkaBasedLog.withExistingClients(
                    config.checkpointsTopic(),
                    MirrorUtils.newConsumer(config.targetConsumerConfig(CHECKPOINTS_TARGET_CONSUMER_ROLE)),
                    null,
                    cpAdmin,
                    consumedCallback,
                    Time.SYSTEM,
                    ignored -> {
                    },
                    topicPartition -> topicPartition.partition() == 0);

            backingStore.start(true);
            backingStore.stop();
        } finally {
            // closing early to free resources
            close();
        }
    }

}
