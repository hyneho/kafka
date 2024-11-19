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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.ProcessorWrapper;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class NoOpProcessorWrapper implements ProcessorWrapper {

    @Override
    public <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                                final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier) {
        return processorSupplier;
    }

    @Override
    public <KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                                    final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier) {
        return processorSupplier;
    }
}
