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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    static final String ALL_TESTS = "all";

    private static final String LOADING_PRODUCER_CLIENT_ID = "simple-benchmark-loading-producer";

    private static final String SOURCE_TOPIC_ONE = "simpleBenchmarkSourceTopic1";
    private static final String SOURCE_TOPIC_TWO = "simpleBenchmarkSourceTopic2";
    private static final String SINK_TOPIC = "simpleBenchmarkSinkTopic";

    private static final String YAHOO_CAMPAIGNS_TOPIC = "yahooCampaigns";
    private static final String YAHOO_EVENTS_TOPIC = "yahooEvents";

    private static final ValueJoiner<byte[], byte[], byte[]> VALUE_JOINER = new ValueJoiner<byte[], byte[], byte[]>() {
        @Override
        public byte[] apply(final byte[] value1, final byte[] value2) {
            // dump joiner in order to have as less join overhead as possible
            if (value1 != null)
                return value1;
            else if (value2 != null)
                return value2;
            else
                return new byte[100];
        }
    };

    private static final Serde<byte[]> BYTE_SERDE = Serdes.ByteArray();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();

    long processedBytes = 0L;
    int processedRecords = 0;

    private static final long POLL_MS = 500L;
    private static final long COMMIT_INTERVAL_MS = 30000L;
    private static final int MAX_POLL_RECORDS = 1000;

    /* ----------- benchmark variables that are hard-coded ----------- */

    private static final int KEY_SPACE_SIZE = 10000;

    private static final long STREAM_STREAM_JOIN_WINDOW = 10000L;

    private static final int SOCKET_SIZE_BYTES = 1024 * 1024;

    // the following numbers are based on empirical results and should only
    // be considered for updates when perf results have significantly changed
    private static final int DEFAULT_NUM_RECORDS = 10000000;

    private static final int MAX_WAIT_MS = 3 * 60 * 1000;   // all streams tests will take more than 3min with 10m records

    /* ----------- benchmark variables that can be specified ----------- */

    final String kafka;

    final String testName;

    final int numRecords;

    final Properties props;

    private final int valueSize;

    private final double keySkew;

    /* ----------- ----------------------------------------- ----------- */


    private SimpleBenchmark(final Properties props,
                            final String kafka,
                            final String testName,
                            final int numRecords,
                            final double keySkew,
                            final int valueSize) {
        super();
        this.props = props;
        this.kafka = kafka;
        this.testName = testName;
        this.keySkew = keySkew;
        this.valueSize = valueSize;
        this.numRecords = numRecords;
    }

    private void run() {
        switch (testName) {
            case ALL_TESTS:
                // load source topics
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_TWO, numRecords, keySkew, valueSize);

                // consume performance
                consume(SOURCE_TOPIC_ONE);

                // consume-produce performance
                consumeAndProduce(SOURCE_TOPIC_ONE);

                // simple stream performance source->process, comparing with consumer
                processStream(SOURCE_TOPIC_ONE);

                // simple stream performance source->sink, comparing with consumer-producer
                processStreamWithSink(SOURCE_TOPIC_ONE);

                // simple stream performance source->store
                processStreamWithStateStore(SOURCE_TOPIC_ONE);

                // simple aggregation performance
                countStreamsNonWindowed(SOURCE_TOPIC_ONE);

                // simple windowed aggregation performance
                countStreamsWindowed(SOURCE_TOPIC_ONE);

                // simple streams performance KSTREAM-KTABLE join
                streamTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);

                // simple streams performance KSTREAM-KSTREAM join
                streamStreamJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);

                // simple streams performance KTABLE-KTABLE join
                tableTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "load-one":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                break;
            case "load-two":
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_ONE, numRecords, keySkew, valueSize);
                produce(LOADING_PRODUCER_CLIENT_ID, SOURCE_TOPIC_TWO, numRecords, keySkew, valueSize);
                break;
            case "consume":
                consume(SOURCE_TOPIC_ONE);
                break;
            case "consumeproduce":
                consumeAndProduce(SOURCE_TOPIC_ONE);
                break;
            case "streamcount":
                countStreamsNonWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamcountwindowed":
                countStreamsWindowed(SOURCE_TOPIC_ONE);
                break;
            case "streamprocess":
                processStream(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithsink":
                processStreamWithSink(SOURCE_TOPIC_ONE);
                break;
            case "streamprocesswithstatestore":
                processStreamWithStateStore(SOURCE_TOPIC_ONE);
                break;
            case "streamtablejoin":
                streamTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "streamstreamjoin":
                streamStreamJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "tabletablejoin":
                tableTableJoin(SOURCE_TOPIC_ONE, SOURCE_TOPIC_TWO);
                break;
            case "yahoo":
                yahooBenchmark(YAHOO_CAMPAIGNS_TOPIC, YAHOO_EVENTS_TOPIC);
                break;
            default:
                throw new RuntimeException("Unknown test name " + testName);

        }
    }

    public static void main(String[] args) throws IOException {
        String propFileName = args.length > 0 ? args[0] : null;
        String testName = args.length > 1 ? args[1].toLowerCase(Locale.ROOT) : ALL_TESTS;
        int numRecords = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_NUM_RECORDS;
        double keySkew = args.length > 3 ? Double.parseDouble(args[3]) : 0d; // default to even distribution
        int valueSize = args.length > 4 ? Integer.parseInt(args[4]) : 1;

        final Properties props = Utils.loadProps(propFileName);
        final String kafka = props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }

        // Note: this output is needed for automated tests and must not be removed
        System.out.println("StreamsTest instance started");

        System.out.println("testName=" + testName);
        System.out.println("kafka=" + kafka);
        System.out.println("streamsProperties=" + props);
        System.out.println("numRecords=" + numRecords);
        System.out.println("keySkew=" + keySkew);
        System.out.println("valueSize=" + valueSize);

        SimpleBenchmark benchmark = new SimpleBenchmark(props, kafka, testName, numRecords, keySkew, valueSize);

        benchmark.run();
    }

    public void setStreamProperties(final String applicationId) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "simple-benchmark");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // the socket buffer needs to be large, especially when running in AWS with
        // high latency. if running locally the default is fine.
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, SOCKET_SIZE_BYTES);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(StreamsConfig.POLL_MS_CONFIG, POLL_MS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        //TODO remove this config or set to smaller value when KIP-91 is merged
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 60000);
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

    void resetStats() {
        processedRecords = 0;
        processedBytes = 0L;
    }

    /**
     * Produce values to a topic
     * @param clientId String specifying client ID
     * @param topic Topic to produce to
     * @param numRecords Number of records to produce
     * @param keySkew Key zipf distribution skewness
     * @param valueSize Size of value in bytes
     */
    private void produce(final String clientId,
                         final String topic,
                         final int numRecords,
                         final double keySkew,
                         final int valueSize) {

        Properties props = setProduceConsumeProperties(clientId);

        ZipfGenerator keyGen = new ZipfGenerator(KEY_SPACE_SIZE, keySkew);

        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(props);

        byte[] value = new byte[valueSize];
        // put some random values to increase entropy. Some devices
        // like SSDs do compression and if the array is all zeros
        // the performance will be too good.
        new Random().nextBytes(value);

        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(topic, keyGen.next(), value));
        }

        producer.close();
    }

    private void consumeAndProduce(String topic) {
        Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");
        Properties producerProps = setProduceConsumeProperties("simple-benchmark-producer");

        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps);

        List<TopicPartition> partitions = getAllPartitions(consumer, topic);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        long startTime = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<Integer, byte[]> records = consumer.poll(POLL_MS);
            if (records.isEmpty()) {
                if (processedRecords == numRecords)
                    break;
            } else {
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    producer.send(new ProducerRecord<>(SINK_TOPIC, record.key(), record.value()));
                    processedRecords++;
                    processedBytes += record.value().length + Integer.SIZE;
                    if (processedRecords == numRecords)
                        break;
                }
            }
            if (processedRecords == numRecords)
                break;
        }

        long endTime = System.currentTimeMillis();

        consumer.close();
        producer.close();

        printResults("ConsumerProducer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void consume(String topic) {
        Properties consumerProps = setProduceConsumeProperties("simple-benchmark-consumer");

        KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);

        List<TopicPartition> partitions = getAllPartitions(consumer, topic);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        long startTime = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<Integer, byte[]> records = consumer.poll(POLL_MS);
            if (records.isEmpty()) {
                if (processedRecords == numRecords)
                    break;
            } else {
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    processedRecords++;
                    processedBytes += record.value().length + Integer.SIZE;
                    if (processedRecords == numRecords)
                        break;
                }
            }
            if (processedRecords == numRecords)
                break;
        }

        long endTime = System.currentTimeMillis();

        consumer.close();

        printResults("Consumer Performance [records/latency/rec-sec/MB-sec read]: ", endTime - startTime);
    }

    private void processStream(final String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, byte[]> source = builder.stream(topic, Consumed.with(INTEGER_SERDE, BYTE_SERDE));

        source.peek(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        long latency = startStreamsThread(streams, latch);

        printResults("Streams Source Performance [records/latency/rec-sec/MB-sec source]: ", latency);
    }

    private void processStreamWithSink(String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-source-sink");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).to(SINK_TOPIC);

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        long latency = startStreamsThread(streams, latch);

        printResults("Streams SourceSink Performance [records/latency/rec-sec/MB-sec source+sink]: ", latency);
    }

    private void processStreamWithStateStore(String topic) {
        final CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-streams-with-store");

        StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<KeyValueStore<Integer, byte[]>> storeBuilder
                = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("store"), INTEGER_SERDE, BYTE_SERDE);
        builder.addStateStore(storeBuilder);

        KStream<Integer, byte[]> source = builder.stream(topic);

        source.peek(new CountDownAction(latch)).process(new ProcessorSupplier<Integer, byte[]>() {
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
                    }

                    @Override
                    public void punctuate(long timestamp) {}

                    @Override
                    public void close() {}
                };
            }
        }, "store");

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        long latency = startStreamsThread(streams, latch);
        printResults("Streams Stateful Performance [records/latency/rec-sec/MB-sec source+store]: ", latency);

    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void countStreamsNonWindowed(String sourceTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-nonwindowed-count");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .count();

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a simple aggregate like count.
     * Counts the occurrence of numbers (note that normally people count words, this
     * example counts numbers)
     */
    private void countStreamsWindowed(String sourceTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-windowed-count");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, byte[]> input = builder.stream(sourceTopic);

        input.peek(new CountDownAction(latch))
                .groupByKey()
                .windowedBy(TimeWindows.of(1000).advanceBy(500))
                .count();

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);
        runGenericBenchmark(streams, "Streams Count Windowed Performance [records/latency/rec-sec/MB-sec counted]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KTable left join. The setup is such that each
     * KStream record joins to exactly one element in the KTable
     */
    private void streamTableJoin(String kStreamTopic, String kTableTopic) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-table-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, byte[]> input1 = builder.stream(kStreamTopic);
        final KTable<Integer, byte[]> input2 = builder.table(kTableTopic);

        input1.leftJoin(input2, VALUE_JOINER).foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    /**
     * Measure the performance of a KStream-KStream left join. The setup is such that each
     * KStream record joins to exactly one element in the other KStream
     */
    private void streamStreamJoin(String kStreamTopic1, String kStreamTopic2) {
        CountDownLatch latch = new CountDownLatch(1);

        setStreamProperties("simple-benchmark-stream-stream-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, byte[]> input1 = builder.stream(kStreamTopic1);
        final KStream<Integer, byte[]> input2 = builder.stream(kStreamTopic2);

        input1.leftJoin(input2, VALUE_JOINER, JoinWindows.of(STREAM_STREAM_JOIN_WINDOW)).foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KStreamKStream LeftJoin Performance [records/latency/rec-sec/MB-sec  joined]: ", latch);
    }

    /**
     * Measure the performance of a KTable-KTable left join. The setup is such that each
     * KTable record joins to exactly one element in the other KTable
     */
    private void tableTableJoin(String kTableTopic1, String kTableTopic2) {
        CountDownLatch latch = new CountDownLatch(1);

        // setup join
        setStreamProperties("simple-benchmark-ktable-ktable-join");

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, byte[]> input1 = builder.table(kTableTopic1);
        final KTable<Integer, byte[]> input2 = builder.table(kTableTopic2);

        input1.leftJoin(input2, VALUE_JOINER).toStream().foreach(new CountDownAction(latch));

        final KafkaStreams streams = createKafkaStreamsWithExceptionHandler(builder, props);

        // run benchmark
        runGenericBenchmark(streams, "Streams KTableKTable LeftJoin Performance [records/latency/rec-sec/MB-sec joined]: ", latch);
    }

    void printResults(final String nameOfBenchmark, final long latency) {
        System.out.println(nameOfBenchmark +
            processedRecords + "/" +
            latency + "/" +
            recordsPerSec(latency, processedRecords) + "/" +
            megabytesPerSec(latency, processedBytes));
    }

    void runGenericBenchmark(final KafkaStreams streams, final String nameOfBenchmark, final CountDownLatch latch) {
        streams.start();

        long startTime = System.currentTimeMillis();

        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        long endTime = System.currentTimeMillis();
        printResults(nameOfBenchmark, endTime - startTime);

        streams.close();
    }

    private long startStreamsThread(final KafkaStreams streams, final CountDownLatch latch) {
        Thread thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();

        long startTime = System.currentTimeMillis();
        long endTime = startTime;

        while (latch.getCount() > 0 && (endTime - startTime < MAX_WAIT_MS)) {
            try {
                latch.await(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }

            endTime = System.currentTimeMillis();
        }

        streams.close();
        try {
            thread.join();
        } catch (Exception ex) {
            // ignore
        }

        return endTime - startTime;
    }

    private class CountDownAction implements ForeachAction<Integer, byte[]> {

        private CountDownLatch latch;

        CountDownAction(final CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void apply(Integer key, byte[] value) {
            processedRecords++;
            processedBytes += Integer.SIZE + value.length;

            if (processedRecords == numRecords) {
                this.latch.countDown();
            }
        }
    }

    private KafkaStreams createKafkaStreamsWithExceptionHandler(final StreamsBuilder builder, final Properties props) {
        final KafkaStreams streamsClient = new KafkaStreams(builder.build(), props);
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

    private void yahooBenchmark(final String campaignsTopic, final String eventsTopic) {
        YahooBenchmark benchmark = new YahooBenchmark(this, campaignsTopic, eventsTopic);

        benchmark.run();
    }

    private class ZipfGenerator {
        private Random rand = new Random(System.currentTimeMillis());
        private int size;
        private double skew;
        private double bottom = 0.0d;

        ZipfGenerator(int size, double skew) {
            this.size = size;
            this.skew = skew;

            for (int i = 1; i < size; i++) {
                this.bottom += 1.0d / Math.pow(i, this.skew);
            }
        }

        int next() {
            if (skew == 0.0d) {
                return rand.nextInt(size);
            } else {
                int rank;
                double dice;
                double friquency;

                rank = rand.nextInt(size);
                friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                dice = rand.nextDouble();

                while (!(dice < friquency)) {
                    rank = rand.nextInt(size);
                    friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                    dice = rand.nextDouble();
                }

                return rank;
            }
        }
    }
}
