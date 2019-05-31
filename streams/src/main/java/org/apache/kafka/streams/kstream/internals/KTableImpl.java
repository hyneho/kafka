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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.KTableKTableJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.TableProcessorNode;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.KTableSuppressProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.suppress.NamedSuppressed;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;
import org.apache.kafka.streams.processor.TypedProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.graph.GraphGraceSearchUtil.findAndVerifyWindowGrace;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, V> extends AbstractStream<K, V> implements KTable<K, V> {
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

    private final TypedProcessorSupplier<?, ?, K, Change<V>> processorSupplier;

    private final StreamsGraphNode<?, ?, K, Change<V>> streamsGraphNode;

    private final String queryableStoreName;

    private boolean sendOldValues = false;

    public KTableImpl(final String name,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final TypedProcessorSupplier<?, ?, K, Change<V>> processorSupplier,
                      final StreamsGraphNode<?, ?, K, Change<V>> streamsGraphNode,
                      final InternalStreamsBuilder builder) {
        super(name, keySerde, valSerde, sourceNodes, builder);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.streamsGraphNode = streamsGraphNode;
    }

    @Override
    public String queryableStoreName() {
        return queryableStoreName;
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
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
            valueSerde = materializedInternal.valueSerde() != null ? materializedInternal.valueSerde() : this.valSerde;
            queryableStoreName = materializedInternal.queryableStoreName();
            // only materialize if materialized is specified and it has queryable name
            storeBuilder = queryableStoreName != null ? (new TimestampedKeyValueStoreMaterializer<>(materializedInternal)).materialize() : null;
        } else {
            keySerde = this.keySerde;
            valueSerde = this.valSerde;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final String name = builder.newProcessorName(FILTER_NAME);

        final KTableProcessorSupplier<K, Change<V>, K, V> processorSupplier =
            new KTableFilter<>(this, predicate, filterNot, queryableStoreName);

        final ProcessorParameters<K, Change<V>, K, Change<V>> processorParameters = new ProcessorParameters<>(processorSupplier, name);

        final StreamsGraphNode<K, Change<V>, K, Change<V>> tableNode = new TableProcessorNode<>(name, processorParameters, storeBuilder, null);

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        return new KTableImpl<>(name,
                                keySerde,
                                valueSerde,
                                sourceNodes,
                                queryableStoreName,
                                processorSupplier,
                                tableNode,
                                builder);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doFilter(predicate, materializedInternal, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doFilter(predicate, materializedInternal, true);
    }

    private <VR> KTable<K, VR> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                           final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {
        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

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

        final String name = builder.newProcessorName(MAPVALUES_NAME);

        final KTableProcessorSupplier<K, Change<V>, K, VR> processorSupplier = new KTableMapValues<>(this, mapper, queryableStoreName);

        // leaving in calls to ITB until building topology with graph

        final ProcessorParameters<K, Change<V>, K, Change<VR>> processorParameters = new ProcessorParameters<>(processorSupplier, name);
        final StreamsGraphNode<K, Change<V>, K, Change<VR>> tableNode = new TableProcessorNode<>(name, processorParameters, storeBuilder, null);

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        // don't inherit parent value serde, since this operation may change the value type, more specifically:
        // we preserve the key following the order of 1) materialized, 2) parent, 3) null
        // we preserve the value following the order of 1) materialized, 2) null
        return new KTableImpl<>(
            name,
            keySerde,
            valueSerde,
            sourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder
        );
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doMapValues(withKey(mapper), materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        return doMapValues(mapper, materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final String... stateStoreNames) {
        return doTransformValues(transformerSupplier, null, stateStoreNames);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);

        return doTransformValues(transformerSupplier, materializedInternal, stateStoreNames);
    }

    private <VR> KTable<K, VR> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                                 final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                                 final String... stateStoreNames) {
        Objects.requireNonNull(stateStoreNames, "stateStoreNames");
        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

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

        final String name = builder.newProcessorName(TRANSFORMVALUES_NAME);

        final KTableProcessorSupplier<K, Change<V>, K, VR> processorSupplier = new KTableTransformValues<>(
            this,
            transformerSupplier,
            queryableStoreName);

        final ProcessorParameters<K, Change<V>, K, Change<VR>> processorParameters = new ProcessorParameters<>(processorSupplier, name);

        final StreamsGraphNode<K, Change<V>, K, Change<VR>> tableNode = new TableProcessorNode<>(
            name,
            processorParameters,
            storeBuilder,
            stateStoreNames
        );

        builder.addGraphNode(this.streamsGraphNode, tableNode);

        return new KTableImpl<>(
            name,
            keySerde,
            valueSerde,
            sourceNodes,
            queryableStoreName,
            processorSupplier,
            tableNode,
            builder);
    }

    @Override
    public KStream<K, V> toStream() {
        final String name = builder.newProcessorName(TOSTREAM_NAME);

        final TypedProcessorSupplier<K, Change<V>, K, V> kStreamMapValues = new KStreamMapValues<>((key, change) -> change.newValue);
        final ProcessorParameters<K, Change<V>, K, V> processorParameters = new ProcessorParameters<>(kStreamMapValues, name);

        final ProcessorGraphNode<K, Change<V>, K, V> toStreamNode = new ProcessorGraphNode<>(
            name,
            processorParameters
        );

        builder.addGraphNode(this.streamsGraphNode, toStreamNode);

        // we can inherit parent key and value serde
        return new KStreamImpl<>(name, keySerde, valSerde, sourceNodes, false, toStreamNode, builder);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
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

        final KTableProcessorSupplier<K, Change<V>, K, V> suppressionSupplier = new KTableSuppressProcessorSupplier<>(
            suppressedInternal,
            storeName,
            this
        );

        final ProcessorGraphNode<K, Change<V>, K, Change<V>> node = new StatefulProcessorNode<>(
            name,
            new ProcessorParameters<>(suppressionSupplier, name),
            new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valSerde)
        );

        builder.addGraphNode(streamsGraphNode, node);

        return new KTableImpl<>(
            name,
            keySerde,
            valSerde,
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
            final long grace = findAndVerifyWindowGrace(streamsGraphNode);
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
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, false, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, materializedInternal, false, false);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, true);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, materializedInternal, true, true);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, MERGE_NAME);

        return doJoin(other, joiner, materializedInternal, true, false);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KTable<K, VR> doJoin(final KTable<K, VO> other,
                                          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal,
                                          final boolean leftOuter,
                                          final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");

        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final Set<String> allSourceNodes = ensureJoinableWith((AbstractStream<K, VO>) other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl<K, VO>) other).enableSendingOldValues();
        }

        final KTableProcessorSupplier<K, Change<V>, K, VR> joinThis;
        final KTableProcessorSupplier<K, Change<VO>, K, VR> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, VO>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, VO>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, VO>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, VO>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, VO>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, VO>) other, this, reverseJoiner(joiner));
        }

        final String joinThisName = builder.newProcessorName(JOINTHIS_NAME);
        final String joinOtherName = builder.newProcessorName(JOINOTHER_NAME);

        final ProcessorParameters<K, Change<V>, K, Change<VR>> joinThisProcessorParameters =
            new ProcessorParameters<>(joinThis, joinThisName);

        final ProcessorParameters<K, Change<VO>, K, Change<VR>> joinOtherProcessorParameters =
            new ProcessorParameters<>(joinOther, joinOtherName);

        final Serde<K> keySerde;
        final Serde<VR> valueSerde;
        final String queryableStoreName;
        final StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        if (materializedInternal != null) {
            keySerde = materializedInternal.keySerde() != null ? materializedInternal.keySerde() : this.keySerde;
            valueSerde = materializedInternal.valueSerde();
            queryableStoreName = materializedInternal.storeName();
            storeBuilder = new TimestampedKeyValueStoreMaterializer<>(materializedInternal).materialize();
        } else {
            keySerde = this.keySerde;
            valueSerde = null;
            queryableStoreName = null;
            storeBuilder = null;
        }

        final KTableKTableJoinNode<K, V, VO, VR> kTableKTableJoinNode =
            KTableKTableJoinNode.<K, V, VO, VR>kTableKTableJoinNodeBuilder()
                .withNodeName(joinMergeName)
                .withJoinThisProcessorParameters(joinThisProcessorParameters)
                .withJoinOtherProcessorParameters(joinOtherProcessorParameters)
                .withThisJoinSideNodeName(name)
                .withOtherJoinSideNodeName(((KTableImpl<K, VO>) other).name)
                .withJoinThisStoreNames(valueGetterSupplier().storeNames())
                .withJoinOtherStoreNames(((KTableImpl<K, VO>) other).valueGetterSupplier().storeNames())
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
                .withQueryableStoreName(queryableStoreName)
                .withStoreBuilder(storeBuilder)
                .build();
        builder.addGraphNode(this.streamsGraphNode, kTableKTableJoinNode);

        // we can inherit parent key serde if user do not provide specific overrides
        return new KTableImpl<>(
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
    @Deprecated
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final org.apache.kafka.streams.kstream.Serialized<K1, V1> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        final SerializedInternal<K1, V1> serializedInternal = new SerializedInternal<>(serialized);
        return groupBy(selector, Grouped.with(serializedInternal.keySerde(), serializedInternal.valueSerde()));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Grouped<K1, V1> grouped) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(grouped, "grouped can't be null");
        final String selectName = builder.newProcessorName(SELECT_NAME);

        final KTableProcessorSupplier<K, Change<V>, K1, V1> selectSupplier = new KTableRepartitionMap<>(this, selector);
        final ProcessorParameters<K, Change<V>, K1, Change<V1>> processorParameters = new ProcessorParameters<>(selectSupplier, selectName);

        // select the aggregate key and values (old and new), it would require parent to send old values
        final ProcessorGraphNode<K, Change<V>, K1, Change<V1>> groupByMapNode = new ProcessorGraphNode<>(selectName, processorParameters);

        builder.addGraphNode(this.streamsGraphNode, groupByMapNode);

        this.enableSendingOldValues();
        final GroupedInternal<K1, V1> groupedInternal = new GroupedInternal<>(grouped);
        return new KGroupedTableImpl<>(
            builder,
            selectName,
            sourceNodes,
            groupedInternal,
            groupByMapNode
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            final KTableSource source = (KTableSource) processorSupplier;
            // whenever a source ktable is required for getter, it should be materialized
            source.materialize();
            return new KTableSourceValueGetterSupplier<>(source.queryableName());
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, ?, V>) this.processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, ?, K, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                ((KTableSource) processorSupplier).enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }
}
