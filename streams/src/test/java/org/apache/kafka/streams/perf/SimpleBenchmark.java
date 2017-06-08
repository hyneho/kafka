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
package org.apache.kafka.streams.perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.Stat;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that provides support for a series of benchmarks. It is usually driven by
 * tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py.
 * If ran manually through the main() function below, you must do the following:
 * 1. Have ZK and a Kafka broker set up
 * 2. Run the loading step first: SimpleBenchmark localhost:9092 /tmp/statedir numRecords true "all"
 * 3. Run the stream processing step second: SimpleBenchmark localhost:9092 /tmp/statedir numRecords false "all"
 * Note that what changed is the 4th parameter, from "true" indicating that is a load phase, to "false" indicating
 * that this is a real run.
 *
 * Note that "all" is a convenience option when running this test locally and will not work when running the test
 * at scale (through tests/kafkatest/benchmarks/streams/streams_simple_benchmark_test.py). That is due to exact syncronization
 * needs for each test (e.g., you wouldn't want one instance to run "count" while another
 * is still running "consume"
 */
public class SimpleBenchmark {

    private final String kafka;
    private final File stateDir;
    private final Boolean loadPhase;
    private final String testName;
    private static final String ALL_TESTS = "all";
    private static final String SOURCE_TOPIC = "simpleBenchmarkSourceTopic";
    private static final String SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static final String COUNT_TOPIC = "countTopic";
    private static final String JOIN_TOPIC_1_PREFIX = "joinSourceTopic1";
    private static final String JOIN_TOPIC_2_PREFIX = "joinSourceTopic2";
    private static final String YAHOO_CAMPAIGNS_TOPIC = "yahooCampaigns";
    private static final String YAHOO_EVENTS_TOPIC = "yahooEvents";
    private static final ValueJoiner VALUE_JOINER = new ValueJoiner<byte[], byte[], byte[]>() {
        @Override
        public byte[] apply(final byte[] value1, final byte[] value2) {
            if (value1 == null && value2 == null)
                return new byte[VALUE_SIZE];
            if (value1 == null && value2 != null)
                return value2;
            if (value1 != null && value2 == null)
                return value1;

            byte[] tmp = new byte[value1.length + value2.length];
            System.arraycopy(value1, 0, tmp, 0, value1.length);
            System.arraycopy(value2, 0, tmp, value1.length, value2.length);
            return tmp;
        }
    };

    private static int numRecords;
    private final AtomicInteger processedRecords = new AtomicInteger(0);
    private static long processedBytes = 0;
    private static final int VALUE_SIZE = 100;
    private static final long POLL_MS = 500L;
    private static final long COMMIT_INTERVAL_MS = 30000L;
    private static final int MAX_POLL_RECORDS = 1000;
    private static final int SOCKET_SIZE_BYTES = 1 * 1024 * 1024;

    private static final Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();

    public SimpleBenchmark(final File stateDir, final String kafka, final Boolean loadPhase, final String testName) {
        super();
        this.stateDir = stateDir;
        this.kafka = kafka;
        this.loadPhase = loadPhase;
        this.testName = testName;
    }

    private void run() throws Exception {
        switch (testName) {
            case ALL_TESTS:
                // producer performance
                produce(SOURCE_TOPIC);
                // consumer performance
                consume(SOURCE_TOPIC);
                // simple stream performance source->process
                processStream(SOURCE_TOPIC);
                // simple stream performance source->sink
                processStreamWithSink(SOURCE_TOPIC);
                // simple stream performance source->store
                processStreamWithStateStore(SOURCE_TOPIC);
                // simple stream performance source->cache->store
                processStreamWithCachedStateStore(SOURCE_TOPIC);
                // simple aggregation
                count(COUNT_TOPIC);
                // simple streams performance KSTREAM-KTABLE join
                kStreamKTableJoin(JOIN_TOPIC_1_PREFIX + "KStreamKTable", JOIN_TOPIC_2_PREFIX + "KStreamKTable");
                // simple streams performance KSTREAM-KSTREAM join
                kStreamKStreamJoin(JOIN_TOPIC_1_PREFIX + "KStreamKStream", JOIN_TOPIC_2_PREFIX + "KStreamKStream");
                // simple streams performance KTABLE-KTABLE join
                kTableKTableJoin(JOIN_TOPIC_1_PREFIX + "KTableKTable", JOIN_TOPIC_2_PREFIX + "KTableKTable");
                break;
            case "produce":
                produce(SOURCE_TOPIC);
                break;
            case "consume":
                consume(SOURCE_TOPIC);
                break;
            case "count":
                count(COUNT_TOPIC);
                break;
            case "processstream":
                processStream(SOURCE_TOPIC);
                break;
            case "processstreamwithsink":
                processStreamWithSink(SOURCE_TOPIC);
                break;
            case "processstreamwithstatestore":
                processStreamWithStateStore(SOURCE_TOPIC);
                break;
            case "processstreamwithcachedstatestore":
                processStreamWithCachedStateStore(SOURCE_TOPIC);
                break;
            case "kstreamktablejoin":
                kStreamKTableJoin(JOIN_TOPIC_1_PREFIX + "KStreamKTable", JOIN_TOPIC_2_PREFIX + "KStreamKTable");
                break;
            case "kstreamkstreamjoin":
                kStreamKStreamJoin(JOIN_TOPIC_1_PREFIX + "KStreamKStream", JOIN_TOPIC_2_PREFIX + "KStreamKStream");
                break;
            case "ktablektablejoin":
                kTableKTableJoin(JOIN_TOPIC_1_PREFIX + "KTableKTable", JOIN_TOPIC_2_PREFIX + "KTableKTable");
                break;
            case "yahoo":
                yahooBenchmark(YAHOO_CAMPAIGNS_TOPIC, YAHOO_EVENTS_TOPIC);
                break;
            default:
                throw new Exception("Unknown test name " + testName);

        }
    }

    public static void main(String[] args) throws Exception {
        String kafka = args.length > 0 ? args[0] : "localhost:9092";
        String stateDirStr = args.length > 1 ? args[1] : TestUtils.tempDirectory().getAbsolutePath();
        numRecords = args.length > 2 ? Integer.parseInt(args[2]) : 10000000;
        boolean loadPhase = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
        String testName = args.length > 4 ? args[4].toLowerCase(Locale.ROOT) : ALL_TESTS;

        final File stateDir = new File(stateDirStr);
        stateDir.mkdir();
        final File rocksdbDir = new File(stateDir, "rocksdb-test");
        rocksdbDir.mkdir();

        // Note: this output is needed for automated tests and must not be removed
        System.out.println("StreamsTest instance started");
        System.out.println("kafka=" + kafka);
        System.out.println("stateDir=" + stateDir);
        System.out.println("numRecords=" + numRecords);
        System.out.println("loadPhase=" + loadPhase);
        System.out.println("testName=" + testName);

        SimpleBenchmark benchmark = new SimpleBenchmark(stateDir, kafka, loadPhase, testName);
        benchmark.run();
    }

    private Properties setStreamProperties(final String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(StreamsConfig.POLL_MS_CONFIG, POLL_MS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        return props;
    }

    private Properties setProduceConsumeProperties(final String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        return props;
    }

    private boolean maybeSetupPhase(final String topic, final String clientId,
                                    final boolean skipIfAllTests) throws Exception {
        processedRecords.set(0);
        processedBytes = 0;
        // initialize topics
        if (loadPhase) {
            if (skipIfAllTests) {
                // if we run all tests, the produce test will have already loaded the data
                if (testName.equals(ALL_TESTS)) {
                    // Skipping loading phase since previously loaded
                    return true;
                }
            }
            System.out.println("Initializing topic " + topic);
            // WARNING: The keys must be sequential, i.e., unique, otherwise the logic for when this test
            // stops will not work (in createCountStreams)
            produce(topic, VALUE_SIZE, clientId, numRecords, true, numRecords, false);
            return true;
        }
        return false;
    }

    // just for Yahoo benchmark
    private boolean maybeSetupPhaseCampaigns(final String topic, final String clientId,
                                             final boolean skipIfAllTests,
                                             final int numCampaigns, final int adsPerCampaign,
                                             final String[] ads) throws Exception {
        processedRecords.set(0);
        processedBytes = 0;
        // initialize topics
        if (loadPhase) {
            if (skipIfAllTests) {
                // if we run all tests, the produce test will have already loaded the data
                if (testName.equals(ALL_TESTS)) {
                    // Skipping loading phase since previously loaded
                    return true;
                }
            }
            System.out.println("Initializing topic " + topic);

            processedRecords.set(0);
            processedBytes = 0;
            Properties props = new Properties();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            int arrayIndex = 0;
            for (int c = 0; c < numCampaigns; c++) {
                String campaignID = UUID.randomUUID().toString();
                for (int a = 0; a < adsPerCampaign; a++) {
                    String adId = UUID.randomUUID().toString();
                    String concat = adId + ":" + campaignID;
                    producer.send(new ProducerRecord<>(topic, adId, concat));
                    ads[arrayIndex] = adId;
                    arrayIndex++;
                    processedRecords.getAndIncrement();
                    processedBytes += campaignID.length() + Integer.SIZE;
                }
            }
            return true;
        }
        return false;
    }

    // just for Yahoo benchmark
    private boolean maybeSetupPhaseEvents(final String topic, final String clientId,
                                          final boolean skipIfAllTests, final int numRecords,
                                          final String[] ads) throws Exception {
        processedRecords.set(0);
        processedBytes = 0;
        String[] eventTypes = new String[]{"view", "click", "purchase"};
        Random rand = new Random();
        // initialize topics
        if (loadPhase) {
            if (skipIfAllTests) {
                // if we run all tests, the produce test will have already loaded the data
                if (testName.equals(ALL_TESTS)) {
                    // Skipping loading phase since previously loaded
                    return true;
                }
            }
            System.out.println("Initializing topic " + topic);

            processedRecords.set(0);
            processedBytes = 0;
            Properties props = new Properties();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

            long startTime = System.currentTimeMillis();

            ProjectedEvent event = new ProjectedEvent();

            Map<String, Object> serdeProps = new HashMap<>();
            final Serializer<SimpleBenchmark.ProjectedEvent> ProjectedEventSerializer = new JsonPOJOSerializer<>();
            serdeProps.put("JsonPOJOClass", SimpleBenchmark.ProjectedEvent.class);
            ProjectedEventSerializer.configure(serdeProps, false);

            for (int i = 0; i < numRecords; i++) {
                event.eventType = eventTypes[rand.nextInt(eventTypes.length - 1)];
                event.adID = ads[rand.nextInt(ads.length - 1)];
                event.eventTime = System.currentTimeMillis();
                byte[] value = ProjectedEventSerializer.serialize(topic, event);
                producer.send(new ProducerRecord<>(topic, event.adID, value));
                processedRecords.getAndIncrement();
                processedBytes += value.length + event.adID.length();
            }
            producer.close();

            long endTime = System.currentTimeMillis();


            printResults("Producer Performance [records/latency/rec-sec/MB-sec write]: ", endTime - startTime);
            return true;
        }
        return false;
    }

    private KafkaStreams createCountStreams(Properties streamConfig, String topic, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<Integer, byte[]> input = builder.stream(topic);

        input.groupByKey()
            .count("tmpStoreName").foreach(new CountDownAction(latch));

        return new KafkaStreams(builder, streamConfig);
    }


    static public class ProjectedEvent {
        public String eventType;
        public String adID;
        public long eventTime;
    }

    static public class CampaignAd {
        public String adID;
        public String campaignID;
    }

    // Note: these are also in the streams example package, eventually use 1 file
    private class JsonPOJOSerializer<T> implements Serializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Default constructor needed by Kafka
         */
        public JsonPOJOSerializer() {
        }

        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null)
                return null;

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {
        }

    }

    // Note: these are also in the streams example package, eventuall use 1 file
    private class JsonPOJODeserializer<T> implements Deserializer<T> {
        private ObjectMapper objectMapper = new ObjectMapper();

        private Class<T> tClass;

        /**
         * Default constructor needed by Kafka
         */
        public JsonPOJODeserializer() {
        }

        @SuppressWarnings("unchecked")
        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
            tClass = (Class<T>) props.get("JsonPOJOClass");
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null)
                return null;

            T data;
            try {
                data = objectMapper.readValue(bytes, tClass);
            } catch (Exception e) {
                throw new SerializationException(e);
            }

            return data;
        }

        @Override
        public void close() {

        }
    }


    private KafkaStreams createYahooBenchmarkStreams(final Properties streamConfig, final String campaignsTopic, final String eventsTopic,
                                                     final CountDownLatch latch, final int numRecords) {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<ProjectedEvent> ProjectedEventSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent.class);
        ProjectedEventSerializer.configure(serdeProps, false);
        final Deserializer<ProjectedEvent> ProjectedEventDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent.class);
        ProjectedEventDeserializer.configure(serdeProps, false);

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, ProjectedEvent> kEvents = builder.stream(Serdes.String(),
            Serdes.serdeFrom(ProjectedEventSerializer, ProjectedEventDeserializer), eventsTopic);
        final KTable<String, String> kCampaigns = builder.table(Serdes.String(), Serdes.String(),
            campaignsTopic, "campaign-state");


        KStream<String, ProjectedEvent> filteredEvents = kEvents.peek(new ForeachAction<String, ProjectedEvent>() {
            @Override
            public void apply(String key, ProjectedEvent value) {
                processedRecords.getAndIncrement();
                if (processedRecords.get() % 1000000 == 0) {
                    System.out.println("Processed " + processedRecords.get());
                }
                if (processedRecords.get() == numRecords ) {
                    latch.countDown();
                }
            }
        }).filter(new Predicate<String, ProjectedEvent>() {
            @Override
            public boolean test(final String key, final ProjectedEvent value) {
                return value.eventType.equals("view");
            }
        }).mapValues(new ValueMapper<ProjectedEvent, ProjectedEvent>() {
            @Override
            public ProjectedEvent apply(ProjectedEvent value) {
                ProjectedEvent event = new ProjectedEvent();
                event.adID = value.adID;
                event.eventTime = value.eventTime;
                event.eventType = value.eventType;
                return event;
            }
        });


        KTable<String, CampaignAd> deserCampaigns = kCampaigns.mapValues(new ValueMapper<String, CampaignAd>() {
            @Override
            public CampaignAd apply(String value) {
                String[] parts = value.split(":");
                CampaignAd cAdd = new CampaignAd();
                cAdd.adID = parts[0];
                cAdd.campaignID = parts[1];
                return cAdd;
            }
        });

        KStream<String, String> joined = filteredEvents.join(deserCampaigns, new ValueJoiner<ProjectedEvent, CampaignAd, String>() {
            @Override
            public String apply(ProjectedEvent value1, CampaignAd value2) {
                return value2.campaignID;
            }
        }, Serdes.String(), Serdes.serdeFrom(ProjectedEventSerializer, ProjectedEventDeserializer));


        KStream<String, String> keyedByCampaign = joined.selectKey(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        });

        KTable<Windowed<String>, Long> counts = keyedByCampaign.groupByKey(Serdes.String(), Serdes.String())
            .count(TimeWindows.of(10 * 1000), "time-windows");

        return new KafkaStreams(builder, streamConfig);
    }

    public void yahooBenchmark(final String campaignsTopic, final String eventsTopic) throws Exception {
        int numCampaigns = 100;
        int adsPerCampaign = 10;

        String[] ads = new String[numCampaigns * adsPerCampaign];
        if (maybeSetupPhaseCampaigns(campaignsTopic, "simple-benchmark-produce-campaigns", false,
            numCampaigns, adsPerCampaign, ads)) {
            maybeSetupPhaseEvents(eventsTopic, "simple-benchmark-produce-events", false,
                numRecords, ads);
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        Properties props = setStreamProperties("simple-benchmark-yahoo" + new Random().nextInt());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Runtime.getRuntime().availableProcessors());
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);

        final KafkaStreams streams = createYahooBenchmarkStreams(props, campaignsTopic, eventsTopic, latch, numRecords);
        runGenericBenchmark(streams, "Streams Yahoo Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     * @param countTopic Topic where numbers are stored
     * @throws Exception
     */
    public void count(String countTopic) throws Exception {
        if (maybeSetupPhase(countTopic, "simple-benchmark-produce-count", false)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        Properties props = setStreamProperties("simple-benchmark-count");
        final KafkaStreams streams = createCountStreams(props, countTopic, latch);
        runGenericBenchmark(streams, "Streams Count Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KTable left join. The setup is such that each
     * KStream record joins to exactly one element in the KTable
     */
    public void kStreamKTableJoin(String kStreamTopic, String kTableTopic) throws Exception {
        if (maybeSetupPhase(kStreamTopic, "simple-benchmark-produce-kstream", false)) {
            maybeSetupPhase(kTableTopic, "simple-benchmark-produce-ktable", false);
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);

        // setup join
        Properties props = setStreamProperties("simple-benchmark-kstream-ktable-join");
        final KafkaStreams streams = createKafkaStreamsKStreamKTableJoin(props, kStreamTopic, kTableTopic, latch);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KStream left join. The setup is such that each
     * KStream record joins to exactly one element in the other KStream
     */
    public void kStreamKStreamJoin(String kStreamTopic1, String kStreamTopic2) throws Exception {
        if (maybeSetupPhase(kStreamTopic1, "simple-benchmark-produce-kstream-topic1", false)) {
            maybeSetupPhase(kStreamTopic2, "simple-benchmark-produce-kstream-topic2", false);
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);

        // setup join
        Properties props = setStreamProperties("simple-benchmark-kstream-kstream-join");
        final KafkaStreams streams = createKafkaStreamsKStreamKStreamJoin(props, kStreamTopic1, kStreamTopic2, latch);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKStream LeftJoin Performance [records/latency/rec-sec/MB-sec  joined]: ", latch);
    }

    /**
     * Measure the performance of a KTable-KTable left join. The setup is such that each
     * KTable record joins to exactly one element in the other KTable
     */
    public void kTableKTableJoin(String kTableTopic1, String kTableTopic2) throws Exception {
        if (maybeSetupPhase(kTableTopic1, "simple-benchmark-produce-ktable-topic1", false)) {
            maybeSetupPhase(kTableTopic2, "simple-benchmark-produce-ktable-topic2", false);
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);

        // setup join
        Properties props = setStreamProperties("simple-benchmark-ktable-ktable-join");
        final KafkaStreams streams = createKafkaStreamsKTableKTableJoin(props, kTableTopic1, kTableTopic2, latch);

        // run benchmark
        runGenericBenchmark(streams, "Streams KTableKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    private void printResults(final String nameOfBenchmark, final long latency) {
        System.out.println(nameOfBenchmark +
            processedRecords.get() + "/" +
            latency + "/" +
            recordsPerSec(latency, processedRecords.get()) + "/" +
            megabytesPerSec(latency, processedBytes));
    }

    private void runGenericBenchmark(final KafkaStreams streams, final String nameOfBenchmark, final CountDownLatch latch) {
        streams.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                //ignore
            }
        }
        long endTime = System.currentTimeMillis();
        printResults(nameOfBenchmark, endTime - startTime);

        streams.close();
    }

    private long startStreamsThread(final KafkaStreams streams, final CountDownLatch latch) throws Exception {
        Thread thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }

        long endTime = System.currentTimeMillis();

        streams.close();
        try {
            thread.join();
        } catch (Exception ex) {
            // ignore
        }

        return endTime - startTime;
    }

    public void processStream(final String topic) throws Exception {
        if (maybeSetupPhase(topic, "simple-benchmark-process-stream-load", true)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);

        final KafkaStreams streams = createKafkaStreams(topic, latch);
        long latency = startStreamsThread(streams, latch);

        printResults("Streams Performance [records/latency/rec-sec/MB-sec source]: ", latency);
    }

    public void processStreamWithSink(String topic) throws Exception {
        if (maybeSetupPhase(topic, "simple-benchmark-process-stream-with-sink-load", true)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = createKafkaStreamsWithSink(topic, latch);
        long latency = startStreamsThread(streams, latch);

        printResults("Streams Performance [records/latency/rec-sec/MB-sec source+sink]: ", latency);

    }

    public void processStreamWithStateStore(String topic) throws Exception {
        if (maybeSetupPhase(topic, "simple-benchmark-process-stream-with-state-store-load", true)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = createKafkaStreamsWithStateStore(topic, latch, false);
        long latency = startStreamsThread(streams, latch);
        printResults("Streams Performance [records/latency/rec-sec/MB-sec source+store]: ", latency);

    }

    public void processStreamWithCachedStateStore(String topic) throws Exception {
        if (maybeSetupPhase(topic, "simple-benchmark-process-stream-with-cached-state-store-load", true)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = createKafkaStreamsWithStateStore(topic, latch, true);
        long latency = startStreamsThread(streams, latch);
        printResults("Streams Performance [records/latency/rec-sec/MB-sec source+cache+store]: ", latency);
    }

    public void produce(String topic) throws Exception {
        // loading phase does not make sense for producer
        if (loadPhase) {
            return;
        }
        produce(topic, VALUE_SIZE, "simple-benchmark-produce", numRecords, true, numRecords, true);
    }
    /**
     * Produce values to a topic
     * @param topic Topic to produce to
     * @param valueSizeBytes Size of value in bytes
     * @param clientId String specifying client ID
     * @param numRecords Number of records to produce
     * @param sequential if True, then keys are produced sequentially from 0 to upperRange. In this case upperRange must be >= numRecords.
     *                   if False, then keys are produced randomly in range [0, upperRange)
     * @param printStats if True, print stats on how long producing took. If False, don't print stats. False can be used
     *                   when this produce step is part of another benchmark that produces its own stats
     */
    private void produce(String topic, int valueSizeBytes, String clientId, int numRecords, boolean sequential,
                         int upperRange, boolean printStats) throws Exception {

        processedRecords.set(0);
        processedBytes = 0;
        if (sequential) {
            if (upperRange < numRecords) throw new Exception("UpperRange must be >= numRecords");
        }
        if (!sequential) {
            System.out.println("WARNING: You are using non-sequential keys. If your tests' exit logic expects to see a final key, random keys may not work.");
        }
        Properties props = setProduceConsumeProperties(clientId);

        int key = 0;
        Random rand = new Random();
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(props);

        byte[] value = new byte[valueSizeBytes];
        // put some random values to increase entropy. Some devices
        // like SSDs do compression and if the array is all zeros
        // the performance will be too good.
        new Random().nextBytes(value);
        long startTime = System.currentTimeMillis();

        if (sequential) key = 0;
        else key = rand.nextInt(upperRange);
        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(topic, key, value));
            if (sequential) key++;
            else key = rand.nextInt(upperRange);
            processedRecords.getAndIncrement();
            processedBytes += value.length + Integer.SIZE;
        }
        producer.close();

        long endTime = System.currentTimeMillis();

        if (printStats) {
            printResults("Producer Performance [records/latency/rec-sec/MB-sec write]: ", endTime - startTime);
        }
    }

    public void consume(String topic) throws Exception {
        if (maybeSetupPhase(topic, "simple-benchmark-consumer-load", true)) {
            return;
        }

        Properties props = setProduceConsumeProperties("simple-benchmark-consumer");

        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(props);

        List<TopicPartition> partitions = getAllPartitions(consumer, topic);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Integer key = null;

        long startTime = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<Integer, byte[]> records = consumer.poll(POLL_MS);
            if (records.isEmpty()) {
                if (processedRecords.get() == numRecords)
                    break;
            } else {
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    processedRecords.getAndIncrement();
                    processedBytes += record.value().length + Integer.SIZE;
                    Integer recKey = record.key();
                    if (key == null || key < recKey)
                        key = recKey;
                    if (processedRecords.get() == numRecords)
                        break;
                }
            }
            if (processedRecords.get() == numRecords)
                break;
        }

        long endTime = System.currentTimeMillis();

        consumer.close();
        printResults("Consumer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private KafkaStreams createKafkaStreams(String topic, final CountDownLatch latch) {
        Properties props = setStreamProperties("simple-benchmark-streams");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {

                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        processedRecords.getAndIncrement();
                        processedBytes += value.length + Integer.SIZE;
                        if (processedRecords.get() == numRecords) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });

        return createKafkaStreamsWithExceptionHandler(builder, props);
    }

    private KafkaStreams createKafkaStreamsWithSink(String topic, final CountDownLatch latch) {
        final Properties props = setStreamProperties("simple-benchmark-streams-with-sink");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.to(INTEGER_SERDE, BYTE_SERDE, SINK_TOPIC);
        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    @Override
                    public void init(ProcessorContext context) {
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        processedRecords.getAndIncrement();
                        processedBytes += value.length + Integer.SIZE;
                        if (processedRecords.get() == numRecords) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });

        return createKafkaStreamsWithExceptionHandler(builder, props);
    }

    private class CountDownAction<V> implements ForeachAction<Integer, V> {
        private CountDownLatch latch;
        CountDownAction(final CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void apply(Integer key, V value) {
            processedRecords.getAndIncrement();
            if (value instanceof byte[]) {
                processedBytes += ((byte[]) value).length + Integer.SIZE;
            } else if (value instanceof Long) {
                processedBytes += Long.SIZE + Integer.SIZE;
            } else {
                System.err.println("Unknown value type in CountDownAction");
            }
            if (processedRecords.get() == numRecords) {
                this.latch.countDown();
            }
        }
    }

    private KafkaStreams createKafkaStreamsKStreamKTableJoin(Properties streamConfig, String kStreamTopic,
                                                             String kTableTopic, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, byte[]> input1 = builder.stream(kStreamTopic);
        final KTable<Long, byte[]> input2 = builder.table(kTableTopic, kTableTopic + "-store");

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        return createKafkaStreamsWithExceptionHandler(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsKTableKTableJoin(Properties streamConfig, String kTableTopic1,
                                                            String kTableTopic2, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KTable<Long, byte[]> input1 = builder.table(kTableTopic1, kTableTopic1 + "-store");
        final KTable<Long, byte[]> input2 = builder.table(kTableTopic2, kTableTopic2 + "-store");

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        return createKafkaStreamsWithExceptionHandler(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsKStreamKStreamJoin(Properties streamConfig, String kStreamTopic1,
                                                              String kStreamTopic2, final CountDownLatch latch) {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<Long, byte[]> input1 = builder.stream(kStreamTopic1);
        final KStream<Long, byte[]> input2 = builder.stream(kStreamTopic2);
        final long timeDifferenceMs = 10000L;

        input1.leftJoin(input2, VALUE_JOINER, JoinWindows.of(timeDifferenceMs)).foreach(new CountDownAction(latch));

        return createKafkaStreamsWithExceptionHandler(builder, streamConfig);
    }

    private KafkaStreams createKafkaStreamsWithStateStore(String topic,
                                                          final CountDownLatch latch,
                                                          boolean enableCaching) {
        Properties props = setStreamProperties("simple-benchmark-streams-with-store" + enableCaching);

        KStreamBuilder builder = new KStreamBuilder();

        if (enableCaching) {
            builder.addStateStore(Stores.create("store").withIntegerKeys().withByteArrayValues().persistent().enableCaching().build());
        } else {
            builder.addStateStore(Stores.create("store").withIntegerKeys().withByteArrayValues().persistent().build());
        }
        KStream<Integer, byte[]> source = builder.stream(INTEGER_SERDE, BYTE_SERDE, topic);

        source.process(new ProcessorSupplier<Integer, byte[]>() {
            @Override
            public Processor<Integer, byte[]> get() {
                return new AbstractProcessor<Integer, byte[]>() {
                    KeyValueStore<Integer, byte[]> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore<Integer, byte[]>) context.getStateStore("store");
                    }

                    @Override
                    public void process(Integer key, byte[] value) {
                        store.put(key, value);
                        processedRecords.getAndIncrement();
                        processedBytes += value.length + Integer.SIZE;
                        if (processedRecords.get() == numRecords) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void punctuate(long timestamp) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        }, "store");

        return createKafkaStreamsWithExceptionHandler(builder, props);
    }

    private KafkaStreams createKafkaStreamsWithExceptionHandler(final KStreamBuilder builder, final Properties props) {
        final KafkaStreams streamsClient = new KafkaStreams(builder, props);
        streamsClient.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);

                streamsClient.close(30, TimeUnit.SECONDS);
            }
        });

        return streamsClient;
    }
    
    private double megabytesPerSec(long time, long processedBytes) {
        return  (processedBytes / 1024.0 / 1024.0) / (time / 1000.0);
    }

    private double recordsPerSec(long time, int numRecords) {
        return numRecords / (time / 1000.0);
    }

    private List<TopicPartition> getAllPartitions(KafkaConsumer<?, ?> consumer, String... topics) {
        ArrayList<TopicPartition> partitions = new ArrayList<>();

        for (String topic : topics) {
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

}
