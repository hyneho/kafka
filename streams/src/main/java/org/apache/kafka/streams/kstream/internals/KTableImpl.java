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

import static org.apache.kafka.streams.kstream.internals.graph.GraphGraceSearchUtil.findAndVerifyWindowGrace;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySchema;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ForeignJoinSubscriptionProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ForeignJoinSubscriptionSendProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionJoinForeignProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResolverJoinProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapperSerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionStoreReceiveProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapperSerde;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.KTableKTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSinkNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSourceNode;
import org.apache.kafka.streams.kstream.internals.graph.TableProcessorNode;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.KTableSuppressProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.suppress.NamedSuppressed;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;
import org.apache.kafka.streams.processor.internals.StaticTopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K, V> implements KTable<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableImpl.class);

    static final String SOURCE_NAME = "KTABLE-SOURCE-";

    static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String SUPPRESS_NAME = "KTABLE-SUPPRESS-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private static final String TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

    private static final String FK_JOIN = "KTABLE-FK-JOIN-";
    private static final String FK_JOIN_STATE_STORE_NAME = FK_JOIN + "SUBSCRIPTION-STATE-STORE-";
    private static final String SUBSCRIPTION_REGISTRATION = FK_JOIN + "SUBSCRIPTION-REGISTRATION-";
    private static final String SUBSCRIPTION_RESPONSE = FK_JOIN + "SUBSCRIPTION-RESPONSE-";
    private static final String SUBSCRIPTION_PROCESSOR = FK_JOIN + "SUBSCRIPTION-PROCESSOR-";
    private static final String SUBSCRIPTION_RESPONSE_RESOLVER_PROCESSOR = FK_JOIN + "SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-";
    private static final String FK_JOIN_OUTPUT_NAME = FK_JOIN + "OUTPUT-";

    private static final String TOPIC_SUFFIX = "-topic";
    private static final String SINK_NAME = "KTABLE-SINK-";

    private final ProcessorSupplier<?, ?, ?, ?> processorSupplier;

    private final String queryableStoreName;

    private boolean sendOldValues = false;

    public <KIn, VIn, KOut, VOut> KTableImpl(final String name,
                      final Serde<K> keySerde,
                      final Serde<V> valueSerde,
                      final Set<String> subTopologySourceNodes,
                      final String queryableStoreName,
                      final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
                      final GraphNode graphNode,
                      final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, subTopologySourceNodes, graphNode, builder);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
    }

    @Override
    public String queryableStoreName() {
        return queryableStoreName;
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final Named named,
                                  final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                  final boolean filterNot) {
        final Serde<K> keySerde;
        final Serde<V> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;

        if (materializedInternal != null) {
            // we actually do not need to generate store names at all since if it is not specified, we will not
            // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            if (materializedInternal.storeName() == null) {
                builder.newStoreName(FILTER_NAME);
            }
            // we can inherit parent key and value serde if user do not provide specific overrides, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) parent
            valueSerde = materializedInternal.valueSerde() != null ? materializedInternal.valueSerde() : this.valueSerde;
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = this.valueSerde;
            queryableStoreName = null;
            storeBuilder = null;
        }
        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, FILTER_NAME);

        final ProcessorSupplier<K, Change<V>, K, Change<V>> processorSupplier = new KTableFilter<>(this, predicate, filterNot,
            queryableStoreName);

        final ProcessorParameters<K, V, ?, ?> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );

        final GraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.addGraphNode(this.graphNode, tableNode);

        return new KTableImpl<K, S, V>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Named named,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doFilter(predicate, named, materializedInternal, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return filter(predicate, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, NamedInternal.empty(), null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Named named) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, named, null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return filterNot(predicate, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Named named,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        final NamedInternal renamed = new NamedInternal(named);
        return doFilter(predicate, renamed, materializedInternal, true);
    }

    private <VOut> KTable<K, VOut> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper,
                                           final Named named,
                                           final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        final Serde<K> keySerde;
        final Serde<VOut> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder;

        if (materializedInternal != null) {
            // we actually do not need to generate store names at all since if it is not specified, we will not
            // materialize the store; but we still need to burn one index BEFORE generating the processor to keep compatibility.
            if (materializedInternal.storeName() == null) {
                builder.newStoreName(MAPVALUES_NAME);
            }
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, MAPVALUES_NAME);

        final ProcessorSupplier<K, Change<V>, K, Change<VOut>> processorSupplier = new KTableMapValues<>(this, mapper, queryableStoreName);

        // leaving in calls to ITB until building topology with graph

        final ProcessorParameters<K, VOut, ?, ?> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );
        final GraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder
        );

        builder.addGraphNode(this.graphNode, tableNode);

        // don't inherit parent value serde, since this operation may change the value type, more specifically:
        // we preserve the key following the order of 1) materialized, 2) parent, 3) null
        // we preserve the value following the order of 1) materialized, 2) null
        return new KTableImpl<K, S, VOut>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder
        );
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), NamedInternal.empty(), null);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper,
                                        final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), named, null);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, NamedInternal.empty(), null);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper,
                                        final Named named) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, named, null);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper,
                                        final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapper<? super V, ? extends VOut> mapper,
                                        final Named named,
                                        final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(withKey(mapper), named, materializedInternal);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper,
                                        final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return mapValues(mapper, NamedInternal.empty(), materialized);
    }

    @Override
    public <VOut> KTable<K, VOut> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VOut> mapper,
                                        final Named named,
                                        final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(mapper, named, materializedInternal);
    }

    @Override
    public <VOut> KTable<K, VOut> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                                              final String... stateStoreNames) {
        return doTransformValues(transformerSupplier, null, NamedInternal.empty(), stateStoreNames);
    }

    @Override
    public <VOut> KTable<K, VOut> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                                              final Named named,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(named, "processorName can't be null");
        return doTransformValues(transformerSupplier, null, new NamedInternal(named), stateStoreNames);
    }

    @Override
    public <VOut> KTable<K, VOut> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                                              final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized,
                                              final String... stateStoreNames) {
        return transformValues(transformerSupplier, materialized, NamedInternal.empty(), stateStoreNames);
    }

    @Override
    public <VOut> KTable<K, VOut> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                                              final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized,
                                              final Named named,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        Objects.requireNonNull(named, "named can't be null");
        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doTransformValues(transformerSupplier, materializedInternal, new NamedInternal(named), stateStoreNames);
    }

    private <VOut> KTable<K, VOut> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VOut> transformerSupplier,
                                                 final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                                 final NamedInternal namedInternal,
                                                 final String... stateStoreNames) {
        Objects.requireNonNull(stateStoreNames, "stateStoreNames");
        final Serde<K> keySerde;
        final Serde<VOut> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder;

        if (materializedInternal != null) {
            // don't inherit parent value serde, since this operation may change the value type, more specifically:
            // we preserve the key following the order of 1) materialized, 2) parent, 3) null
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            // we preserve the value following the order of 1) materialized, 2) null
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final String name = namedInternal.orElseGenerateWithPrefix(builder, TRANSFORMVALUES_NAME);

        final KTableChangeProcessorSupplier<K, V, VOut, K, VOut> processorSupplier = new KTableTransformValues<>(
            this,
            transformerSupplier,
            queryableStoreName);

        final ProcessorParameters<K, VOut, ?, ?> processorParameters = unsafeCastProcessorParametersToCompletelyDifferentType(
            new ProcessorParameters<>(processorSupplier, name)
        );

        final GraphNode tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder,
            stateStoreNames
        );

        builder.addGraphNode(this.graphNode, tableNode);

        return new KTableImpl<>(
            name,
            keySerde,
            valueSerde,
            subTopologySourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder);
    }

    @Override
    public KStream<K, V> toStream() {
        return toStream(NamedInternal.empty());
    }

    @Override
    public KStream<K, V> toStream(final Named named) {
        Objects.requireNonNull(named, "named can't be null");

        final String name = new NamedInternal(named).orElseGenerateWithPrefix(builder, TOSTREAM_NAME);
        final KStreamMapValues<K, Change<V>, V> kStreamMapValues = new KStreamMapValues<>((key, change) -> change.newValue);
        final ProcessorParameters<K, Change<V>, K, V> processorParameters = new ProcessorParameters<>(
            kStreamMapValues,
            name);

        final ProcessorGraphNode<K, Change<V>> toStreamNode = new ProcessorGraphNode<>(
            name,
            processorParameters);

        builder.addGraphNode(this.graphNode, toStreamNode);

        // we can inherit parent key and value serde
        return new KStreamImpl<>(name, keySerde, valueSerde, subTopologySourceNodes, false, toStreamNode, builder);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper,
                                        final Named named) {
        return toStream(named).selectKey(mapper);
    }

    @Override
    public KTable<K, V> suppress(final Suppressed<? super K> suppressed) {
        final String name;
        if (suppressed instanceof NamedSuppressed) {
            final String givenName = ((NamedSuppressed<?>) suppressed).name();
            name = givenName != null ? givenName : builder.newProcessorName(SUPPRESS_NAME);
        } else {
            throw new IllegalArgumentException("Custom subclasses of Suppressed are not supported.");
        }

        final SuppressedInternal<K> suppressedInternal = buildSuppress(suppressed, name);

        final String storeName =
            suppressedInternal.name() != null ? suppressedInternal.name() + "-store" : builder.newStoreName(SUPPRESS_NAME);

        final ProcessorSupplier<K, Change<V>, K, Change<V>> suppressionSupplier = new KTableSuppressProcessorSupplier<>(
            suppressedInternal,
            storeName,
            this
        );

        final StoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> storeBuilder;

        if (suppressedInternal.bufferConfig().isLoggingEnabled()) {
            final Map<String, String> topicConfig = suppressedInternal.bufferConfig().getLogConfig();
            storeBuilder = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(
                storeName,
                keySerde,
                valueSerde)
                .withLoggingEnabled(topicConfig);
        } else {
            storeBuilder = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(
                storeName,
                keySerde,
                valueSerde)
                .withLoggingDisabled();
        }

        final ProcessorGraphNode<K, Change<V>> node = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(suppressionSupplier, name),
            storeBuilder
        );

        builder.addGraphNode(graphNode, node);

        return new KTableImpl<K, S, V>(
            name,
            keySerde,
            valueSerde,
            Collections.singleton(this.name),
            null,
            suppressionSupplier,
            node,
            builder
        );
    }

    @SuppressWarnings("unchecked")
    private SuppressedInternal<K> buildSuppress(final Suppressed<? super K> suppress, final String name) {
        if (suppress instanceof FinalResultsSuppressionBuilder) {
            final long grace = findAndVerifyWindowGrace(graphNode);
            LOG.info("Using grace period of [{}] as the suppress duration for node [{}].",
                     Duration.ofMillis(grace), name);

            final FinalResultsSuppressionBuilder<?> builder = (FinalResultsSuppressionBuilder<?>) suppress;

            final SuppressedInternal<?> finalResultsSuppression =
                builder.buildFinalResultsSuppression(Duration.ofMillis(grace));

            return (SuppressedInternal<K>) finalResultsSuppression;
        } else if (suppress instanceof SuppressedInternal) {
            return (SuppressedInternal<K>) suppress;
        } else {
            throw new IllegalArgumentException("Custom subclasses of Suppressed are not allowed.");
        }
    }

    @Override
    public <V1, VOut> KTable<K, VOut> join(final KTable<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner) {
        return doJoin(other, joiner, NamedInternal.empty(), null, false, false);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> join(final KTable<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                           final Named named) {
        return doJoin(other, joiner, named, null, false, false);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> join(final KTable<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                           final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return join(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> join(final KTable<K, V1> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                           final Named named,
                                           final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, false, false);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> outerJoin(final KTable<K, V1> other,
                                                final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner) {
        return outerJoin(other, joiner, NamedInternal.empty());
    }

    @Override
    public <V1, VOut> KTable<K, VOut> outerJoin(final KTable<K, V1> other,
                                                final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                                final Named named) {
        return doJoin(other, joiner, named, null, true, true);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> outerJoin(final KTable<K, V1> other,
                                                final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                                final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return outerJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> outerJoin(final KTable<K, V1> other,
                                                final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                                final Named named,
                                                final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, true);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> leftJoin(final KTable<K, V1> other,
                                               final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner) {
        return leftJoin(other, joiner, NamedInternal.empty());
    }

    @Override
    public <V1, VOut> KTable<K, VOut> leftJoin(final KTable<K, V1> other,
                                               final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                               final Named named) {
        return doJoin(other, joiner, named, null, true, false);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> leftJoin(final KTable<K, V1> other,
                                               final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                               final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return leftJoin(other, joiner, NamedInternal.empty(), materialized);
    }

    @Override
    public <V1, VOut> KTable<K, VOut> leftJoin(final KTable<K, V1> other,
                                               final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                               final Named named,
                                               final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, named, materializedInternal, true, false);
    }

    @SuppressWarnings("unchecked")
    private <V1, VOut> KTable<K, VOut> doJoin(final KTable<K, V1> other,
                                              final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner,
                                              final Named joinName,
                                              final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                              final boolean leftOuter,
                                              final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joinName, "joinName can't be null");

        final NamedInternal renamed = new NamedInternal(joinName);
        final String joinMergeName = renamed.orElseGenerateWithPrefix(builder, MERGE_NAME);
        final Set<String> allSourceNodes = ensureCopartitionWith(Collections.singleton((AbstractStream<K, V1>) other));

        if (leftOuter) {
            enableSendingOldValues(true);
        }
        if (rightOuter) {
            ((KTableImpl<?, ?, ?>) other).enableSendingOldValues(true);
        }

        final KTableKTableAbstractJoin<K, V, V1, VOut> joinThis;
        final KTableKTableAbstractJoin<K, V1, V, VOut> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        }

        final String joinThisName = renamed.suffixWithOrElseGet("-join-this", builder, JOINTHIS_NAME);
        final String joinOtherName = renamed.suffixWithOrElseGet("-join-other", builder, JOINOTHER_NAME);

        final ProcessorParameters<K, Change<V>, K, Change<VOut>> joinThisProcessorParameters = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K, Change<V1>, K, Change<VOut>> joinOtherProcessorParameters = new ProcessorParameters<>(joinOther, joinOtherName);

        final Serde<K> keySerde;
        final Serde<VOut> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VOut>> storeBuilder;

        if (materializedInternal != null) {
            if (materializedInternal.keySerde() == null) {
                materializedInternal.withKeySerde(this.keySerde);
            }
            keySerde = materializedInternal.keySerde();
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.storeName();
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final KTableKTableJoinNode<K, V, V1, VOut> kTableKTableJoinNode =
            KTableKTableJoinNode.<K, V, V1, VOut>kTableKTableJoinNodeBuilder()
                .withNodeName(joinMergeName)
                .withJoinThisProcessorParameters(joinThisProcessorParameters)
                .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
                .withThisJoinSideNodeName(name)
                .withOtherJoinSideNodeName(((KTableImpl<?, ?, ?>) other).name)
                .withJoinThisStoreNames(valueGetterSupplier().storeNames())
                .withJoinOtherStoreNames(
                    ((KTableImpl<?, ?, ?>) other).valueGetterSupplier().storeNames())
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
                .withQueryableStoreName(queryableStoreName)
                .withStoreBuilder(storeBuilder)
                .build();
        builder.addGraphNode(this.graphNode, kTableKTableJoinNode);

        // we can inherit parent key serde if user do not provide specific overrides
        return new KTableImpl<K, Change<VOut>, VOut>(
            kTableKTableJoinNode.nodeName(),
            kTableKTableJoinNode.keySerde(),
            kTableKTableJoinNode.valueSerde(),
            allSourceNodes,
            kTableKTableJoinNode.queryableStoreName(),
            kTableKTableJoinNode.joinMerger(),
            kTableKTableJoinNode,
            builder
        );
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return groupBy(selector, Grouped.with(null, null));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Grouped<K1, V1> grouped) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
        final GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<>(grouped);
        final String selectName = new NamedInternal(groupedInternal.name()).orElseGenerateWithPrefix(builder, SELECT_NAME);

        final KTableChangeProcessorSupplier<K, V, KeyValue<K1, V1>, K1, V1> selectSupplier = new KTableRepartitionMap<>(this, selector);
        final ProcessorParameters<K, Change<V>, K1, Change<V1>> processorParameters = new ProcessorParameters<>(selectSupplier, selectName);

        // select the aggregate key and values (old and new), it would require parent to send old values
        final ProcessorGraphNode<K, Change<V>> groupByMapNode = new ProcessorGraphNode<>(selectName, processorParameters);

        builder.addGraphNode(this.graphNode, groupByMapNode);

        this.enableSendingOldValues(true);
        return new KGroupedTableImpl<>(
            builder,
            selectName,
            subTopologySourceNodes,
            groupedInternal,
            groupByMapNode
        );
    }

    @SuppressWarnings("unchecked")
    public KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            final KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            // whenever a source ktable is required for getter, it should be materialized
            source.materialize();
            return new KTableSourceValueGetterSupplier<>(source.queryableName());
        } else if (processorSupplier instanceof KStreamAggregateProcessorSupplier) {
            return ((KStreamAggregateProcessorSupplier<?, S, K, V>) processorSupplier).view();
        } else {
            return ((KTableChangeProcessorSupplier<K, S, V, ?, ?>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                final KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                if (!forceMaterialization && !source.materialized()) {
                    return false;
                }
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggregateProcessorSupplier) {
                ((KStreamAggregateProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                final KTableChangeProcessorSupplier<K, S, V, ?, ?> tableProcessorSupplier =
                    (KTableChangeProcessorSupplier<K, S, V, ?, ?>) processorSupplier;
                if (!tableProcessorSupplier.enableSendingOldValues(forceMaterialization)) {
                    return false;
                }
            }
            sendOldValues = true;
        }
        return true;
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

    /**
     * We conflate V with Change<V> in many places. This will get fixed in the implementation of KIP-478.
     * For now, I'm just explicitly lying about the parameterized type.
     */
    @SuppressWarnings("unchecked")
    private <VOut> ProcessorParameters<K, VOut, ?, ?> unsafeCastProcessorParametersToCompletelyDifferentType(final ProcessorParameters<K, Change<V>, ?, ?> kObjectProcessorParameters) {
        return (ProcessorParameters<K, VOut, ?, ?>) kObjectProcessorParameters;
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> join(final KTable<K1, V1> other,
                                               final Function<V, K1> foreignKeyExtractor,
                                               final ValueJoiner<V, V1, VOut> joiner) {
        return doJoinOnForeignKey(
            other,
            foreignKeyExtractor,
            joiner,
            NamedInternal.empty(),
            Materialized.with(null, null),
            false
        );
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> join(final KTable<K1, V1> other,
                                               final Function<V, K1> foreignKeyExtractor,
                                               final ValueJoiner<V, V1, VOut> joiner,
                                               final Named named) {
        return doJoinOnForeignKey(
            other,
            foreignKeyExtractor,
            joiner,
            named,
            Materialized.with(null, null),
            false
        );
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> join(final KTable<K1, V1> other,
                                               final Function<V, K1> foreignKeyExtractor,
                                               final ValueJoiner<V, V1, VOut> joiner,
                                               final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoinOnForeignKey(other, foreignKeyExtractor, joiner, NamedInternal.empty(), materialized, false);
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> join(final KTable<K1, V1> other,
                                               final Function<V, K1> foreignKeyExtractor,
                                               final ValueJoiner<V, V1, VOut> joiner,
                                               final Named named,
                                               final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoinOnForeignKey(other, foreignKeyExtractor, joiner, named, materialized, false);
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> leftJoin(final KTable<K1, V1> other,
                                                   final Function<V, K1> foreignKeyExtractor,
                                                   final ValueJoiner<V, V1, VOut> joiner) {
        return doJoinOnForeignKey(
            other,
            foreignKeyExtractor,
            joiner,
            NamedInternal.empty(),
            Materialized.with(null, null),
            true
        );
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> leftJoin(final KTable<K1, V1> other,
                                                   final Function<V, K1> foreignKeyExtractor,
                                                   final ValueJoiner<V, V1, VOut> joiner,
                                                   final Named named) {
        return doJoinOnForeignKey(
            other,
            foreignKeyExtractor,
            joiner,
            named,
            Materialized.with(null, null),
            true
        );
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> leftJoin(final KTable<K1, V1> other,
                                                   final Function<V, K1> foreignKeyExtractor,
                                                   final ValueJoiner<V, V1, VOut> joiner,
                                                   final Named named,
                                                   final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoinOnForeignKey(other, foreignKeyExtractor, joiner, named, materialized, true);
    }

    @Override
    public <K1, V1, VOut> KTable<K, VOut> leftJoin(final KTable<K1, V1> other,
                                                   final Function<V, K1> foreignKeyExtractor,
                                                   final ValueJoiner<V, V1, VOut> joiner,
                                                   final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized) {
        return doJoinOnForeignKey(other, foreignKeyExtractor, joiner, NamedInternal.empty(), materialized, true);
    }

    @SuppressWarnings("unchecked")
    private <K1, V1, VOut> KTable<K, VOut> doJoinOnForeignKey(final KTable<K1, V1> foreignKeyTable,
                                                              final Function<V, K1> foreignKeyExtractor,
                                                              final ValueJoiner<V, V1, VOut> joiner,
                                                              final Named joinName,
                                                              final Materialized<K, VOut, KeyValueStore<Bytes, byte[]>> materialized,
                                                              final boolean leftJoin) {
        Objects.requireNonNull(foreignKeyTable, "foreignKeyTable can't be null");
        Objects.requireNonNull(foreignKeyExtractor, "foreignKeyExtractor can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(joinName, "joinName can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        //Old values are a useful optimization. The old values from the foreignKeyTable table are compared to the new values,
        //such that identical values do not cause a prefixScan. PrefixScan and propagation can be expensive and should
        //not be done needlessly.
        ((KTableImpl<?, ?, ?>) foreignKeyTable).enableSendingOldValues(true);

        //Old values must be sent such that the ForeignJoinSubscriptionSendProcessorSupplier can propagate deletions to the correct node.
        //This occurs whenever the extracted foreignKey changes values.
        enableSendingOldValues(true);

        final NamedInternal renamed = new NamedInternal(joinName);

        final String subscriptionTopicName = renamed.suffixWithOrElseGet(
            "-subscription-registration",
            builder,
            SUBSCRIPTION_REGISTRATION
        ) + TOPIC_SUFFIX;

        // the decoration can't be performed until we have the configuration available when the app runs,
        // so we pass Suppliers into the components, which they can call at run time

        final Supplier<String> subscriptionPrimaryKeySerdePseudoTopic =
            () -> internalTopologyBuilder().decoratePseudoTopic(subscriptionTopicName + "-pk");

        final Supplier<String> subscriptionForeignKeySerdePseudoTopic =
            () -> internalTopologyBuilder().decoratePseudoTopic(subscriptionTopicName + "-fk");

        final Supplier<String> valueHashSerdePseudoTopic =
            () -> internalTopologyBuilder().decoratePseudoTopic(subscriptionTopicName + "-vh");

        builder.internalTopologyBuilder.addInternalTopic(subscriptionTopicName, InternalTopicProperties.empty());

        final Serde<K1> foreignKeySerde = ((KTableImpl<K1, V1, ?>) foreignKeyTable).keySerde;
        final Serde<SubscriptionWrapper<K>> subscriptionWrapperSerde = new SubscriptionWrapperSerde<>(subscriptionPrimaryKeySerdePseudoTopic, keySerde);
        final SubscriptionResponseWrapperSerde<V1> responseWrapperSerde =
            new SubscriptionResponseWrapperSerde<>(((KTableImpl<K1, V1, V1>) foreignKeyTable).valueSerde);

        final CombinedKeySchema<K1, K> combinedKeySchema = new CombinedKeySchema<>(
            subscriptionForeignKeySerdePseudoTopic,
            foreignKeySerde,
            subscriptionPrimaryKeySerdePseudoTopic,
            keySerde
        );

        final ProcessorGraphNode<K, Change<V>> subscriptionNode = new ProcessorGraphNode<>(
            new ProcessorParameters<>(
                new ForeignJoinSubscriptionSendProcessorSupplier<>(
                    foreignKeyExtractor,
                    subscriptionForeignKeySerdePseudoTopic,
                    valueHashSerdePseudoTopic,
                    foreignKeySerde,
                    valueSerde == null ? null : valueSerde.serializer(),
                    leftJoin
                ),
                renamed.suffixWithOrElseGet("-subscription-registration-processor", builder, SUBSCRIPTION_REGISTRATION)
            )
        );
        builder.addGraphNode(graphNode, subscriptionNode);


        final StreamSinkNode<K1, SubscriptionWrapper<K>> subscriptionSink = new StreamSinkNode<>(
            renamed.suffixWithOrElseGet("-subscription-registration-sink", builder, SINK_NAME),
            new StaticTopicNameExtractor<>(subscriptionTopicName),
            new ProducedInternal<>(Produced.with(foreignKeySerde, subscriptionWrapperSerde))
        );
        builder.addGraphNode(subscriptionNode, subscriptionSink);

        final StreamSourceNode<K1, SubscriptionWrapper<K>> subscriptionSource = new StreamSourceNode<>(
            renamed.suffixWithOrElseGet("-subscription-registration-source", builder, SOURCE_NAME),
            Collections.singleton(subscriptionTopicName),
            new ConsumedInternal<>(Consumed.with(foreignKeySerde, subscriptionWrapperSerde))
        );
        builder.addGraphNode(subscriptionSink, subscriptionSource);

        // The subscription source is the source node on the *receiving* end *after* the repartition.
        // This topic needs to be copartitioned with the Foreign Key table.
        final Set<String> copartitionedRepartitionSources =
            new HashSet<>(((KTableImpl<?, ?, ?>) foreignKeyTable).subTopologySourceNodes);
        copartitionedRepartitionSources.add(subscriptionSource.nodeName());
        builder.internalTopologyBuilder.copartitionSources(copartitionedRepartitionSources);


        final StoreBuilder<TimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>> subscriptionStore =
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(
                    renamed.suffixWithOrElseGet("-subscription-store", builder, FK_JOIN_STATE_STORE_NAME)
                ),
                new Serdes.BytesSerde(),
                subscriptionWrapperSerde
            );
        builder.addStateStore(subscriptionStore);

        final StatefulProcessorNode<K1, SubscriptionWrapper<K>> subscriptionReceiveNode =
            new StatefulProcessorNode<>(
                new ProcessorParameters<>(
                    new SubscriptionStoreReceiveProcessorSupplier<>(subscriptionStore, combinedKeySchema),
                    renamed.suffixWithOrElseGet("-subscription-receive", builder, SUBSCRIPTION_PROCESSOR)
                ),
                Collections.singleton(subscriptionStore),
                Collections.emptySet()
            );
        builder.addGraphNode(subscriptionSource, subscriptionReceiveNode);

        final StatefulProcessorNode<CombinedKey<K1, K>, Change<ValueAndTimestamp<SubscriptionWrapper<K>>>> subscriptionJoinForeignNode =
            new StatefulProcessorNode<>(
                new ProcessorParameters<>(
                    new SubscriptionJoinForeignProcessorSupplier<>(
                        ((KTableImpl<K1, V1, V1>) foreignKeyTable).valueGetterSupplier()
                    ),
                    renamed.suffixWithOrElseGet("-subscription-join-foreign", builder, SUBSCRIPTION_PROCESSOR)
                ),
                Collections.emptySet(),
                Collections.singleton(
                    ((KTableImpl<K1, V1, V1>) foreignKeyTable).valueGetterSupplier())
            );
        builder.addGraphNode(subscriptionReceiveNode, subscriptionJoinForeignNode);

        final StatefulProcessorNode<K1, Change<Object>> foreignJoinSubscriptionNode = new StatefulProcessorNode<>(
            new ProcessorParameters<>(
                new ForeignJoinSubscriptionProcessorSupplier<>(subscriptionStore, combinedKeySchema),
                renamed.suffixWithOrElseGet("-foreign-join-subscription", builder, SUBSCRIPTION_PROCESSOR)
            ),
            Collections.singleton(subscriptionStore),
            Collections.emptySet()
        );
        builder.addGraphNode(((KTableImpl<K1, V1, ?>) foreignKeyTable).graphNode, foreignJoinSubscriptionNode);


        final String finalRepartitionTopicName = renamed.suffixWithOrElseGet("-subscription-response", builder, SUBSCRIPTION_RESPONSE) + TOPIC_SUFFIX;
        builder.internalTopologyBuilder.addInternalTopic(finalRepartitionTopicName, InternalTopicProperties.empty());

        final StreamSinkNode<K, SubscriptionResponseWrapper<V1>> foreignResponseSink =
            new StreamSinkNode<>(
                renamed.suffixWithOrElseGet("-subscription-response-sink", builder, SINK_NAME),
                new StaticTopicNameExtractor<>(finalRepartitionTopicName),
                new ProducedInternal<>(Produced.with(keySerde, responseWrapperSerde))
            );
        builder.addGraphNode(subscriptionJoinForeignNode, foreignResponseSink);
        builder.addGraphNode(foreignJoinSubscriptionNode, foreignResponseSink);

        final StreamSourceNode<K, SubscriptionResponseWrapper<V1>> foreignResponseSource = new StreamSourceNode<>(
            renamed.suffixWithOrElseGet("-subscription-response-source", builder, SOURCE_NAME),
            Collections.singleton(finalRepartitionTopicName),
            new ConsumedInternal<>(Consumed.with(keySerde, responseWrapperSerde))
        );
        builder.addGraphNode(foreignResponseSink, foreignResponseSource);

        // the response topic has to be copartitioned with the left (primary) side of the join
        final Set<String> resultSourceNodes = new HashSet<>(this.subTopologySourceNodes);
        resultSourceNodes.add(foreignResponseSource.nodeName());
        builder.internalTopologyBuilder.copartitionSources(resultSourceNodes);

        final KTableValueGetterSupplier<K, V> primaryKeyValueGetter = valueGetterSupplier();
        final SubscriptionResolverJoinProcessorSupplier<K, V, V1, VOut> resolverProcessorSupplier =
            new SubscriptionResolverJoinProcessorSupplier<>(
                primaryKeyValueGetter,
                valueSerde == null ? null : valueSerde.serializer(),
                valueHashSerdePseudoTopic,
                joiner,
                leftJoin
            );
        final StatefulProcessorNode<K, SubscriptionResponseWrapper<V1>> resolverNode = new StatefulProcessorNode<>(
            new ProcessorParameters<>(
                resolverProcessorSupplier,
                renamed.suffixWithOrElseGet("-subscription-response-resolver", builder,
                    SUBSCRIPTION_RESPONSE_RESOLVER_PROCESSOR)
            ),
            Collections.emptySet(),
            Collections.singleton(primaryKeyValueGetter)
        );
        builder.addGraphNode(foreignResponseSource, resolverNode);

        final String resultProcessorName = renamed.suffixWithOrElseGet("-result", builder, FK_JOIN_OUTPUT_NAME);

        final MaterializedInternal<K, VOut, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(
                materialized,
                builder,
                FK_JOIN_OUTPUT_NAME
            );

        // If we have a key serde, it's still valid, but we don't know the value serde, since it's the result
        // of the joiner (VOut).
        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }

        final KTableSource<K, VOut> resultProcessorSupplier = new KTableSource<>(
            materializedInternal.storeName(),
            materializedInternal.queryableStoreName()
        );

        final StoreBuilder<TimestampedKeyValueStore<K, VOut>> resultStore =
            new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();

        final TableProcessorNode<K, VOut> resultNode = new TableProcessorNode<>(
            resultProcessorName,
            new ProcessorParameters<>(
                resultProcessorSupplier,
                resultProcessorName
            ),
            resultStore
        );
        builder.addGraphNode(resolverNode, resultNode);

        return new KTableImpl<K, V, VOut>(
            resultProcessorName,
            keySerde,
            materializedInternal.valueSerde(),
            resultSourceNodes,
            materializedInternal.storeName(),
            resultProcessorSupplier,
            resultNode,
            builder
        );
    }
}
