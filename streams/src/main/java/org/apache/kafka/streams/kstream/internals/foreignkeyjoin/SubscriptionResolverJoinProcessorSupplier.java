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

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Murmur3;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class SubscriptionResolverJoinProcessorSupplier<K, V, VO, VR> implements ProcessorSupplier<K, SubscriptionResponseWrapper<VO>> {
    private final KTableValueGetterSupplier<K, V> valueGetterSupplier;
    private final Serializer<V> valueSerializer;
    private final ValueJoiner<V, VO, VR> joiner;
    private final boolean leftJoin;

    public SubscriptionResolverJoinProcessorSupplier(final KTableValueGetterSupplier<K, V> valueGetterSupplier,
                                                     final Serializer<V> valueSerializer,
                                                     final ValueJoiner<V, VO, VR> joiner,
                                                     final boolean leftJoin) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.valueSerializer = valueSerializer;
        this.joiner = joiner;
        this.leftJoin = leftJoin;
    }

    @Override
    public Processor<K, SubscriptionResponseWrapper<VO>> get() {
        return new AbstractProcessor<K, SubscriptionResponseWrapper<VO>>() {

            private KTableValueGetter<K, V> valueGetter;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                valueGetter = valueGetterSupplier.get();
                valueGetter.init(context);
            }

            @Override
            public void process(final K key, final SubscriptionResponseWrapper<VO> value) {
                if (value.getVersion() != SubscriptionResponseWrapper.CURRENT_VERSION) {
                    //Guard against modifications to SubscriptionResponseWrapper. Need to ensure that there is
                    //compatibility with previous versions to enable rolling upgrades. Must develop a strategy for
                    //upgrading from older SubscriptionWrapper versions to newer versions.
                    throw new UnsupportedVersionException("SubscriptionResponseWrapper is of an incompatible version.");
                }
                final ValueAndTimestamp<V> currentValueWithTimestamp = valueGetter.get(key);

                //final V currentValue = currentValueWithTimestamp.value();
                //We are unable to access the actual source topic name for the valueSerializer at runtime, without
                //tightly coupling to KTableRepartitionProcessorSupplier.
                //While we can use the source topic from where the events came from, we shouldn't serialize against it
                //as it causes problems with the confluent schema registry, which requires each topic have only a single
                //registered schema.
                final String dummySerializationTopic = context().topic() + "-join-resolver";
                final long[] currentHash = currentValueWithTimestamp == null ?
                        null :
                        Murmur3.hash128(valueSerializer.serialize(dummySerializationTopic, currentValueWithTimestamp.value()));

                final long[] messageHash = value.getOriginalValueHash();

                //If this value doesn't match the current value from the original table, it is stale and should be discarded.
                if (java.util.Arrays.equals(messageHash, currentHash)) {
                    final VR result;

                    if (value.getForeignValue() == null && !leftJoin ||
                            leftJoin && currentValueWithTimestamp == null && value.getForeignValue() == null) {
                        result = null; //Emit tombstone
                    } else {
                        result = joiner.apply(currentValueWithTimestamp == null ? null : currentValueWithTimestamp.value(), value.getForeignValue());
                    }
                    context().forward(key, result);
                }
            }
        };
    }
}
