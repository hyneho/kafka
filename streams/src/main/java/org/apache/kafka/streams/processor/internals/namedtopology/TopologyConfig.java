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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.kafka.streams.StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_DOC;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_DOC;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_DOC;

/**
 * Streams configs that apply at the topology level. The values in the {@link StreamsConfig} parameter of the
 * {@link org.apache.kafka.streams.KafkaStreams} or {@link KafkaStreamsNamedTopologyWrapper} constructors will
 * determine the defaults, which can then be overridden for specific topologies by passing them in when creating the
 * topology via the {@link org.apache.kafka.streams.StreamsBuilder#build(Properties)} or
 * {@link NamedTopologyBuilder#buildNamedTopology(Properties)} methods.
 */
public class TopologyConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;
    static {
        CONFIG = new ConfigDef()
             .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
                     Type.INT,
                     null,
                     Importance.LOW,
                     BUFFERED_RECORDS_PER_PARTITION_DOC)
             .define(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    Type.CLASS,
                    null,
                    Importance.MEDIUM,
                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
             .define(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                     Type.CLASS,
                     null,
                     Importance.MEDIUM,
                     DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC)
             .define(MAX_TASK_IDLE_MS_CONFIG,
                     Type.LONG,
                     null,
                     Importance.MEDIUM,
                     MAX_TASK_IDLE_MS_DOC)
             .define(TASK_TIMEOUT_MS_CONFIG,
                     Type.LONG,
                     null,
                     Importance.MEDIUM,
                     TASK_TIMEOUT_MS_DOC);
    }
    private final Logger log = LoggerFactory.getLogger(TopologyConfig.class);

    public final String topologyName;
    public final boolean eosEnabled;

    public final StreamsConfig applicationConfigs;
    public final Properties topologyOverrides;

    final long maxTaskIdleMs;
    final long taskTimeoutMs;
    final int maxBufferedSize;
    final Supplier<TimestampExtractor> timestampExtractorSupplier;
    final Supplier<DeserializationExceptionHandler> deserializationExceptionHandlerSupplier;

    public TopologyConfig(final String topologyName, final StreamsConfig globalAppConfigs, final Properties topologyOverrides) {
        super(CONFIG, topologyOverrides, false);

        this.topologyName = topologyName;
        this.eosEnabled = StreamThread.eosEnabled(globalAppConfigs);

        this.applicationConfigs = globalAppConfigs;
        this.topologyOverrides = topologyOverrides;

        if (isTopologyOverride(MAX_TASK_IDLE_MS_CONFIG, topologyOverrides)) {
            maxTaskIdleMs = getLong(MAX_TASK_IDLE_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, MAX_TASK_IDLE_MS_CONFIG, maxTaskIdleMs);
        } else {
            maxTaskIdleMs = globalAppConfigs.getLong(MAX_TASK_IDLE_MS_CONFIG);
        }

        if (isTopologyOverride(TASK_TIMEOUT_MS_CONFIG, topologyOverrides)) {
            taskTimeoutMs = getLong(TASK_TIMEOUT_MS_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, TASK_TIMEOUT_MS_CONFIG, taskTimeoutMs);
        } else {
            taskTimeoutMs = globalAppConfigs.getLong(TASK_TIMEOUT_MS_CONFIG);
        }

        if (isTopologyOverride(BUFFERED_RECORDS_PER_PARTITION_CONFIG, topologyOverrides)) {
            maxBufferedSize = getInt(BUFFERED_RECORDS_PER_PARTITION_CONFIG);
            log.info("Topology {} is overriding {} to {}", topologyName, BUFFERED_RECORDS_PER_PARTITION_CONFIG, maxBufferedSize);
        } else {
            maxBufferedSize = globalAppConfigs.getInt(BUFFERED_RECORDS_PER_PARTITION_CONFIG);
        }

        if (isTopologyOverride(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, topologyOverrides)) {
            timestampExtractorSupplier = () -> getConfiguredInstance(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
            log.info("Topology {} is overriding {} to {}", topologyName, DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, getClass(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
        } else {
            timestampExtractorSupplier = () -> globalAppConfigs.getConfiguredInstance(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractor.class);
        }

        if (isTopologyOverride(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, topologyOverrides)) {
            deserializationExceptionHandlerSupplier = () -> getConfiguredInstance(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationExceptionHandler.class);
            log.info("Topology {} is overriding {} to {}", topologyName, DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, getClass(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG));
        } else {
            deserializationExceptionHandlerSupplier = () -> globalAppConfigs.getConfiguredInstance(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationExceptionHandler.class);
        }
    }

    /**
     * @return true if there is an override for this config in the properties of this NamedTopology. Applications that
     *         don't use named topologies will just refer to the global defaults regardless of the topology properties
     */
    private boolean isTopologyOverride(final String config, final Properties topologyOverrides) {
        // TODO KAFKA-13283: eventually we should always have the topology props override the global ones regardless
        // of whether it's a named topology or not
        return topologyName != null && topologyOverrides.containsKey(config);
    }

    public TaskConfig getTaskConfig() {
        return new TaskConfig(
            maxTaskIdleMs,
            taskTimeoutMs,
            maxBufferedSize,
            timestampExtractorSupplier.get(),
            deserializationExceptionHandlerSupplier.get(),
            eosEnabled
        );
    }

    public static class TaskConfig {
        public final long maxTaskIdleMs;
        public final long taskTimeoutMs;
        public final int maxBufferedSize;
        public final TimestampExtractor timestampExtractor;
        public final DeserializationExceptionHandler deserializationExceptionHandler;
        public final boolean eosEnabled;

        private TaskConfig(final long maxTaskIdleMs,
                           final long taskTimeoutMs,
                           final int maxBufferedSize,
                           final TimestampExtractor timestampExtractor,
                           final DeserializationExceptionHandler deserializationExceptionHandler,
                           final boolean eosEnabled) {
            this.maxTaskIdleMs = maxTaskIdleMs;
            this.taskTimeoutMs = taskTimeoutMs;
            this.maxBufferedSize = maxBufferedSize;
            this.timestampExtractor = timestampExtractor;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
            this.eosEnabled = eosEnabled;
        }
    }
}
