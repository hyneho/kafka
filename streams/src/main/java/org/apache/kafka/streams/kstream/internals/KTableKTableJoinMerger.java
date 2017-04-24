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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

class KTableKTableJoinMerger<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V> parent1;
    private final KTableImpl<K, ?, V> parent2;
    private final String queryableName;
    private boolean sendOldValues = false;

    public KTableKTableJoinMerger(final KTableImpl<K, ?, V> parent1,
                                  final KTableImpl<K, ?, V> parent2,
                                  final String queryableName) {
        this.parent1 = parent1;
        this.parent2 = parent2;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return queryableName != null ? new MaterializedKTableKTableJoinMergeProcessor() : new KTableKTableJoinMergeProcessor<>();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return parent1.valueGetterSupplier();
    }

    @Override
    public void enableSendingOldValues() {
        parent1.enableSendingOldValues();
        parent2.enableSendingOldValues();
        sendOldValues = true;
    }

    private static final class KTableKTableJoinMergeProcessor<K, V>
        extends AbstractProcessor<K, Change<V>> {
        @Override
        public void process(K key, Change<V> value) {
            context().forward(key, value);
        }
    }

    private class MaterializedKTableKTableJoinMergeProcessor<K, V>
        extends AbstractProcessor<K, Change<V>> {
        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, V>) context.getStateStore(queryableName);
            tupleForwarder = new TupleForwarder<>(store, context,
                new ForwardingCacheFlushListener<K, V>(context, sendOldValues),
                sendOldValues);
        }

        @Override
        public void process(K key, Change<V> value) {

            store.put(key, value.newValue);
            tupleForwarder.maybeForward(key, value.newValue, value.oldValue);
        }
    }

}
