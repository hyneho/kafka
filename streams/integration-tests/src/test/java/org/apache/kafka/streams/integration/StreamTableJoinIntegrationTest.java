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
package org.apache.kafka.streams.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.ProcessorWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Tag("integration")
@Timeout(600)
public class StreamTableJoinIntegrationTest extends AbstractJoinIntegrationTest {
    private static final String STORE_NAME = "table-store";
    private static final String APP_ID = "stream-table-join-integration-test";

    public static class LoggingTestWrapper implements ProcessorWrapper {

        @Override
        public <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName, final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {
            return new LoggingTestProcessorSupplier<>(processorName, processorSupplier);
        }

        @Override
        public <KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(final String processorName, final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
            throw new IllegalStateException("Did not expect fixed key");
        }
    }

    private static class LoggingTestProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

        private final String processorName;
        private final ProcessorSupplier<KIn, VIn, KOut, VOut> inner;

        public LoggingTestProcessorSupplier(final String processorName,
                                            final ProcessorSupplier<KIn, VIn, KOut, VOut> inner) {
            this.processorName = processorName;
            this.inner = inner;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            final Set<StoreBuilder<?>> innerStores = inner.stores();

            if (innerStores == null || innerStores.isEmpty()) {
                System.out.println("SOPHIE: processor " + processorName + " has no stores");
            } else {
                System.out.println("SOPHIE: processor " + processorName + " has a store!");
            }
            return innerStores;
        }

        @Override
        public Processor<KIn, VIn, KOut, VOut> get() {
            final Processor<KIn, VIn, KOut, VOut> innerProcessor = inner.get();
            return new LoggingTestProcessor<>(processorName, innerProcessor);
        }
    }

    private static class LoggingTestProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

        private final String processorName;
        private final Processor<KIn, VIn, KOut, VOut> inner;


        public LoggingTestProcessor(final String processorName,
                                    final Processor<KIn, VIn, KOut, VOut> inner) {
            this.processorName = processorName;
            this.inner = inner;
        }

        @Override
        public void init(final ProcessorContext<KOut, VOut> context) {
            inner.init(context);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            System.out.println("SOPHIE: processing record in " + processorName);
            inner.process(record);
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInner(final boolean cacheEnabled) {
        final Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "1088");
        props.put(StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG, LoggingTestWrapper.class);
        final StreamsBuilder builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT);
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null,  6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            null,
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeft(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT);
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-null", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInnerWithVersionedStore(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
                Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner");
        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null,  6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLeftWithVersionedStore(final boolean cacheEnabled) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        final KTable<Long, String> rightTable = builder.table(INPUT_TOPIC_RIGHT, Materialized.as(
                Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMinutes(5))));
        final Properties streamsConfig = setupConfigsAndUtils(cacheEnabled);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left");
        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "C-null", null, 9L)),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 6L)),
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "E-e", null,  15L)),
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "F-a", null,  4L)),
            null
        );

        runTestWithDriver(input, expectedResult, streamsConfig, builder.build(streamsConfig));
    }
}
