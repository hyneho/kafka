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

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.TypedProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TypedProcessorSupplier;

class KStreamMapValues<K, V, V1> implements TypedProcessorSupplier<K, V, K, V1> {

    private final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapper;

    public KStreamMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends V1> mapper) {
        this.mapper = mapper;
    }

    @Override
    public TypedProcessor<K, V, K, V1> get() {
        return new KStreamMapProcessor();
    }

    private class KStreamMapProcessor implements TypedProcessor<K, V, K, V1> {
        private ProcessorContext<K, V1> context;

        @Override
        public void init(final ProcessorContext<K, V1> context) {
            this.context = context;
        }

        @Override
        public void process(final K readOnlyKey, final V value) {
            final V1 newValue = mapper.apply(readOnlyKey, value);
            context.forward(readOnlyKey, newValue);
        }

        @Override
        public void close() {

        }
    }
}
