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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.NoOpValueTransformerWithKeySupplier;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class KStreamTransformValuesTest {
    private final String topicName = "topic";
    private final MockProcessorSupplier<Integer, Integer, Void, Void> supplier = new MockProcessorSupplier<>();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.Integer());
    private InternalProcessorContext context = mock(InternalProcessorContext.class);

    @SuppressWarnings("unchecked")
    @Test
    public void shouldInitializeTransformerWithForwardDisabledProcessorContext() {
        final NoOpValueTransformerWithKeySupplier<String, String> transformer = new NoOpValueTransformerWithKeySupplier<>();
        final KStreamTransformValues<String, String, String> transformValues = new KStreamTransformValues<>(transformer);
        final Processor<String, String, String, String> processor = transformValues.get();

        processor.init(context);

        assertThat(transformer.context, isA((Class) ForwardingDisabledProcessorContext.class));
    }
}
