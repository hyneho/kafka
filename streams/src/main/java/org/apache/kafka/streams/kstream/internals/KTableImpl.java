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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;


/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String FOREACH_NAME = "KTABLE-FOREACH-";

    public static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    public static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    public static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String PRINTING_NAME = "KSTREAM-PRINTER-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    public static final String SOURCE_NAME = "KTABLE-SOURCE-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    public static final String STATE_STORE_NAME = "STATE-STORE-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final KeyValueMapper<K, V, String> defaultKeyValueMapper;

    private final String queryableStoreName;
    private final boolean isQueryable;

    private boolean sendOldValues = false;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      ProcessorSupplier<?, ?> processorSupplier,
                      Set<String> sourceNodes,
                      final String queryableStoreName,
                      boolean isQueryable) {
        super(topology, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = null;
        this.valSerde = null;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    public KTableImpl(KStreamBuilder topology,
                      String name,
                      ProcessorSupplier<?, ?> processorSupplier,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      Set<String> sourceNodes,
                      final String queryableStoreName,
                      boolean isQueryable) {
        super(topology, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.isQueryable = isQueryable;
        this.defaultKeyValueMapper = new KeyValueMapper<K, V, String>() {
            @Override
            public String apply(K key, V value) {
                return String.format("%s, %s", key, value);
            }
        };
    }

    @Override
    public String queryableStoreName() {
        if (!isQueryable) {
            return null;
        }
        return this.queryableStoreName;
    }

    String internalStoreName() {
        return this.queryableStoreName;
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final StateStoreSupplier<KeyValueStore> storeSupplier,
                                  boolean isFilterNot) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        String name = topology.newName(FILTER_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this, predicate, isFilterNot, internalStoreName);
        topology.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            topology.addStateStore(storeSupplier, name);
        }
        return new KTableImpl<>(topology, name, processorSupplier, this.keySerde, this.valSerde, sourceNodes, internalStoreName, internalStoreName != null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return filter(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate, final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate, final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return filterNot(predicate, (String) null);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, this.valSerde, queryableStoreName);
        }
        return doFilter(predicate, storeSupplier, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doFilter(predicate, storeSupplier, true);
    }

    private <V1> KTable<K, V1> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapperWithKey,
                                           final Serde<V1> valueSerde,
                                           final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(mapperWithKey);
        String name = topology.newName(MAPVALUES_NAME);
        String internalStoreName = null;
        if (storeSupplier != null) {
            internalStoreName = storeSupplier.name();
        }
        KTableProcessorSupplier<K, V, V1> processorSupplier = new KTableMapValues<>(this, mapperWithKey, internalStoreName);
        topology.addProcessor(name, processorSupplier, this.name);
        if (storeSupplier != null) {
            topology.addStateStore(storeSupplier, name);
            return new KTableImpl<>(topology, name, processorSupplier, this.keySerde, valueSerde, sourceNodes, internalStoreName, true);
        } else {
            return new KTableImpl<>(topology, name, processorSupplier, sourceNodes, this.queryableStoreName, false);
        }
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper) {
        return mapValues(withKey(mapper), null, (String) null);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapperWithKey) {
        return mapValues(mapperWithKey, null, (String) null);
    }

    @Override
    public <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                        final Serde<V1> valueSerde,
                                        final String queryableStoreName) {
        return mapValues(withKey(mapper), valueSerde, queryableStoreName);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapperWithKey,
                                        final Serde<VR> valueSerde,
                                        final String queryableStoreName) {
        StateStoreSupplier<KeyValueStore> storeSupplier = null;
        if (queryableStoreName != null) {
            storeSupplier = keyValueStore(this.keySerde, valueSerde, queryableStoreName);
        }
        return doMapValues(mapperWithKey, valueSerde, storeSupplier);
    }

    @Override
    public  <V1> KTable<K, V1> mapValues(final ValueMapper<? super V, ? extends V1> mapper,
                                         final Serde<V1> valueSerde,
                                         final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doMapValues(withKey(mapper), valueSerde, storeSupplier);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapperWithKey,
                                        final Serde<VR> valueSerde,
                                        final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doMapValues(mapperWithKey, valueSerde, storeSupplier);
    }

    @Override
    public void print() {
        print(null, null, this.name);
    }

    @Override
    public void print(String label) {
        print(null, null, label);
    }

    @Override
    public void print(Serde<K> keySerde, Serde<V> valSerde) {
        print(keySerde, valSerde, this.name);
    }


    @Override
    public void print(Serde<K> keySerde, final Serde<V> valSerde, String label) {
        Objects.requireNonNull(label, "label can't be null");
        String name = topology.newName(PRINTING_NAME);
        topology.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(null, defaultKeyValueMapper, label), keySerde, valSerde), this.name);
    }

    @Override
    public void writeAsText(String filePath) {
        writeAsText(filePath, this.name, null, null);
    }

    @Override
    public void writeAsText(String filePath, String label) {
        writeAsText(filePath, label, null, null);
    }

    @Override
    public void writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde) {
        writeAsText(filePath, this.name, keySerde, valSerde);
    }

    /**
     * @throws TopologyBuilderException if file is not found
     */
    @Override
    public void writeAsText(String filePath, String label, Serde<K> keySerde, Serde<V> valSerde) {
        Objects.requireNonNull(filePath, "filePath can't be null");
        Objects.requireNonNull(label, "label can't be null");
        if (filePath.trim().isEmpty()) {
            throw new TopologyBuilderException("filePath can't be an empty string");
        }
        String name = topology.newName(PRINTING_NAME);
        try {
            PrintWriter printWriter = new PrintWriter(filePath, StandardCharsets.UTF_8.name());
            topology.addProcessor(name, new KStreamPrint<>(new PrintForeachAction(printWriter, defaultKeyValueMapper, label), keySerde, valSerde), this.name);
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            String message = "Unable to write stream to file at [" + filePath + "] " + e.getMessage();
            throw new TopologyBuilderException(message);
        }
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        Objects.requireNonNull(action, "action can't be null");
        String name = topology.newName(FOREACH_NAME);
        KStreamPeek<K, Change<V>> processorSupplier = new KStreamPeek<>(new ForeachAction<K, Change<V>>() {
            @Override
            public void apply(K key, Change<V> value) {
                action.apply(key, value.newValue);
            }
        }, false);
        topology.addProcessor(name, processorSupplier, this.name);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        final String internalStoreName = queryableStoreName != null ? queryableStoreName : topology.newStoreName(KTableImpl.TOSTREAM_NAME);

        to(keySerde, valSerde, partitioner, topic);

        return topology.table(null, new FailOnInvalidTimestamp(), keySerde, valSerde, topic, internalStoreName);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        to(keySerde, valSerde, partitioner, topic);

        return topology.table(null, new FailOnInvalidTimestamp(), keySerde, valSerde, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(keySerde, valSerde, partitioner, topic, (String) null);
    }
    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final String queryableStoreName) {
        return through(keySerde, valSerde, null, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(keySerde, valSerde, null, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final Serde<K> keySerde,
                                final Serde<V> valSerde,
                                final String topic) {
        return through(keySerde, valSerde, null, topic, (String) null);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final String queryableStoreName) {
        return through(null, null, partitioner, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, partitioner, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                                final String topic) {
        return through(null, null, partitioner, topic, (String) null);
    }

    @Override
    public KTable<K, V> through(final String topic,
                                final String queryableStoreName) {
        return through(null, null, null, topic, queryableStoreName);
    }

    @Override
    public KTable<K, V> through(final String topic,
                                final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return through(null, null, null, topic, storeSupplier);
    }

    @Override
    public KTable<K, V> through(final String topic) {
        return through(null, null, null, topic, (String) null);
    }

    @Override
    public void to(String topic) {
        to(null, null, null, topic);
    }

    @Override
    public void to(StreamPartitioner<? super K, ? super V> partitioner, String topic) {
        to(null, null, partitioner, topic);
    }

    @Override
    public void to(Serde<K> keySerde, Serde<V> valSerde, String topic) {
        this.toStream().to(keySerde, valSerde, null, topic);
    }

    @Override
    public void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<? super K, ? super V> partitioner, String topic) {
        this.toStream().to(keySerde, valSerde, partitioner, topic);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = topology.newName(TOSTREAM_NAME);

        topology.addProcessor(name, new KStreamMapValues<K, Change<V>, V>(new ValueMapperWithKey<K, Change<V>, V>() {
            @Override
            public V apply(K key, Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(topology, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, withKey(joiner), false, false, null, (String) null);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey) {
        return doJoin(other, joinerWithKey, false, false, null, (String) null);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final Serde<R> joinSerde,
                                     final String queryableStoreName) {
        return doJoin(other, withKey(joiner), false, false, joinSerde, queryableStoreName);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                       final Serde<VR> joinSerde,
                                       final String queryableStoreName) {
        return doJoin(other, joinerWithKey, false, false, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                     final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, withKey(joiner), false, false, storeSupplier);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                       final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joinerWithKey, false, false, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, withKey(joiner), true, true, null, (String) null);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey) {
        return doJoin(other, joinerWithKey, true, true, null, (String) null);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final Serde<R> joinSerde,
                                          final String queryableStoreName) {
        return doJoin(other, withKey(joiner), true, true, joinSerde, queryableStoreName);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                            final Serde<VR> joinSerde,
                                            final String queryableStoreName) {
        return doJoin(other, joinerWithKey, true, true, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                          final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, withKey(joiner), true, true, storeSupplier);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                            final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joinerWithKey, true, true, storeSupplier);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, withKey(joiner), true, false, null, (String) null);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey) {
        return doJoin(other, joinerWithKey, true, false, null, (String) null);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final Serde<R> joinSerde,
                                         final String queryableStoreName) {
        return doJoin(other, withKey(joiner), true, false, joinSerde, queryableStoreName);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                           final Serde<VR> joinSerde,
                                           final String queryableStoreName) {
        return doJoin(other, joinerWithKey, true, false, joinSerde, queryableStoreName);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                         final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, withKey(joiner), true, false, storeSupplier);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joinerWithKey,
                                           final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        return doJoin(other, joinerWithKey, true, false, storeSupplier);
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends R> joinerWithKey,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final Serde<R> joinSerde,
                                        final String queryableStoreName) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joinerWithKey, "joiner can't be null");

        final StateStoreSupplier storeSupplier = queryableStoreName == null ? null : keyValueStore(this.keySerde, joinSerde, queryableStoreName);

        return doJoin(other, joinerWithKey, leftOuter, rightOuter, storeSupplier);
    }

    private <V1, R> KTable<K, R> doJoin(final KTable<K, V1> other,
                                        ValueJoinerWithKey<? super K, ? super V, ? super V1, ? extends R> joinerWithKey,
                                        final boolean leftOuter,
                                        final boolean rightOuter,
                                        final StateStoreSupplier<KeyValueStore> storeSupplier) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joinerWithKey, "joiner can't be null");
        final String internalQueryableName = storeSupplier == null ? null : storeSupplier.name();
        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K>) other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final String joinThisName = topology.newName(JOINTHIS_NAME);
        final String joinOtherName = topology.newName(JOINOTHER_NAME);
        final String joinMergeName = topology.newName(MERGE_NAME);

        final KTableKTableAbstractJoin<K, R, V, V1> joinThis;
        final KTableKTableAbstractJoin<K, R, V1, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableJoin<>(this, (KTableImpl<K, ?, V1>) other, joinerWithKey);
            joinOther = new KTableKTableJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joinerWithKey));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joinerWithKey);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joinerWithKey));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joinerWithKey);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joinerWithKey));
        }

        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(topology, joinThisName, joinThis, sourceNodes, this.internalStoreName(), false),
                new KTableImpl<K, V1, R>(topology, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                        ((KTableImpl<K, ?, ?>) other).internalStoreName(), false),
                internalQueryableName
        );

        topology.addProcessor(joinThisName, joinThis, this.name);
        topology.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        topology.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        topology.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        topology.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());

        if (internalQueryableName != null) {
            topology.addStateStore(storeSupplier, joinMergeName);
        }

        return new KTableImpl<>(topology, joinMergeName, joinMerge, allSourceNodes, internalQueryableName, internalQueryableName != null);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  Serde<K1> keySerde,
                                                  Serde<V1> valueSerde) {

        Objects.requireNonNull(selector, "selector can't be null");
        String selectName = topology.newName(SELECT_NAME);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<K, V, K1, V1>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        topology.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();

        return new KGroupedTableImpl<>(topology, selectName, this.name, keySerde, valueSerde);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return this.groupBy(selector, null, null);
    }

    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

}