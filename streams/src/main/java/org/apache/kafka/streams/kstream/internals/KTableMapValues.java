/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableMapValues<K1, V1, V2> extends KTableProcessorSupplier<K1, V1, V2> {

    private final ValueMapper<V1, V2> mapper;

    public KTableMapValues(ValueMapper<V1, V2> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor<K1, V1> get() {
        return new KTableMapProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K1, V2> view(KTableValueGetterSupplier<K1, V1> parentValueGetterSupplier) {
        return new KTableDerivedValueGetterSupplier<K1, V1, V2>(parentValueGetterSupplier) {

            public KTableValueGetter<K1, V2> get() {
                return new KTableMapValuesValueGetter(parentValueGetterSupplier.get());
            }

        };
    }

    private V2 computeNewValue(V1 value) {
        V2 newValue = null;

        if (value != null)
            newValue = mapper.apply(value);

        return newValue;
    }

    private class KTableMapProcessor extends AbstractProcessor<K1, V1> {

        @Override
        public void process(K1 key, V1 value) {
            context().forward(key, computeNewValue(value));
        }

    }

    private class KTableMapValuesValueGetter implements KTableValueGetter<K1, V2> {

        private final KTableValueGetter<K1, V1> parentGetter;

        public KTableMapValuesValueGetter(KTableValueGetter<K1, V1> parentGetter) {
            this.parentGetter = parentGetter;
        }

        public void init(ProcessorContext context) {
            parentGetter.init(context);
        }

        public V2 get(K1 key) {
            return computeNewValue(parentGetter.get(key));
        }

    }

}
