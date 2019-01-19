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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Arrays;

public class MaterializedKTableKTableJoinNode<K, V1, V2, VR> extends KTableKTableJoinNode<K, V1, V2, VR> {

    private final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal;

    @SuppressWarnings("unchecked")
    MaterializedKTableKTableJoinNode(final String nodeName,
                         final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner,
                         final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final Serde<K> keySerde,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames, final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {

        super(nodeName,
            valueJoiner,
            joinThisProcessorParameters,
            joinOtherProcessorParameters,
            new ProcessorParameters<>(
                KTableKTableJoinMerger.of(
                    (KTableProcessorSupplier<K, V1, VR>) (joinThisProcessorParameters.processorSupplier()),
                    (KTableProcessorSupplier<K, V2, VR>) (joinOtherProcessorParameters.processorSupplier()),
                    materializedInternal.storeName()),
                nodeName),
            thisJoinSide,
            otherJoinSide,
            keySerde,
            joinThisStoreNames,
            joinOtherStoreNames);

        this.materializedInternal = materializedInternal;
    }

    @Override
    public Serde<K> keySerde() {
        // Override default keySerde if materialization keySerde is defined.
        return materializedInternal.keySerde() != null ? materializedInternal.keySerde() : super.keySerde();
    }

    @Override
    public Serde<VR> valueSerde() {
        // Use materialization valueSerde, instead of default valueSerde.
        return materializedInternal.valueSerde();
    }

    @Override
    public String queryableStoreName() {
        return materializedInternal.storeName();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        super.writeToTopology(topologyBuilder);

        final String mergeProcessorName = mergeProcessorParameters().processorName();
        final StoreBuilder<KeyValueStore<K, VR>> storeBuilder =
            new KeyValueStoreMaterializer<>(materializedInternal).materialize();
        topologyBuilder.addStateStore(storeBuilder, mergeProcessorName);
    }

    @Override
    public String toString() {
        return "MaterializedKTableKTableJoinNode{" +
            "joinThisStoreNames=" + Arrays.toString(joinThisStoreNames()) +
            ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames()) +
            ", materializedInternal=" + materializedInternal +
            "} " + super.toString();
    }
}
