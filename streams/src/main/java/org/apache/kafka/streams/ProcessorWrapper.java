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

package org.apache.kafka.streams;

import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Wrapper class that can be used to inject custom wrappers around the processors of their application topology.
 * The returned instance MUST wrap the supplied {@code ProcessorSupplier} and the {@code Processor} it supplies.
 * Returning a new or completely different instance can have unexpected and undesirable affects.
 * <p>
 * Can be configured, if desired, by implementing the {@link #configure(Map)} method, which will be invoked
 * with the Streams application configs once the {@code ProcessorWrapper} is instantiated.
 */
public interface ProcessorWrapper extends Configurable {

    @Override
    default void configure(final Map<String, ?> configs) {
        // do nothing
    }

    <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                         final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier);

    <KIn, VIn,  VOut> FixedKeyProcessorSupplier<KIn, VIn,  VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                               final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier);
}
