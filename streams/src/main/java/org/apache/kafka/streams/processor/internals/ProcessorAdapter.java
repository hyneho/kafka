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


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public final class ProcessorAdapter<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    private final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate;

    public static <KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut> adapt(final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate) {
        if (delegate == null) {
            return null;
        } else {
            return new ProcessorAdapter<>(delegate);
        }
    }

    private ProcessorAdapter(final org.apache.kafka.streams.processor.Processor<KIn, VIn> delegate) {
        this.delegate = delegate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext<KOut, VOut> context) {
        delegate.init(ProcessorContextReverseAdapter.adapt((InternalApiProcessorContext<Object, Object>) context));
    }

    @Override
    public void process(final KIn key, final VIn value) {
        delegate.process(key, value);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
