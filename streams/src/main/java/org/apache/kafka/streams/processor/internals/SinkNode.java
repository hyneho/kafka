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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class SinkNode<K, V> extends ProcessorNode<K, V> {

    private final String topic;
    private Serializer<K> keySerializer;
    private Serializer<V> valSerializer;
    private final StreamPartitioner<K, V> partitioner;

    private ProcessorContext context;

    public SinkNode(String name, String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, StreamPartitioner<K, V> partitioner) {
        super(name);

        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valSerializer = valSerializer;
        this.partitioner = partitioner;
    }

    /**
     * @throws UnsupportedOperationException if this method adds a child to a sink node
     */
    @Override
    public void addChild(ProcessorNode<?, ?> child) {
        throw new UnsupportedOperationException("sink node does not allow addChild");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context, StreamsMetrics metrics) {
        this.context = context;

        // if serializers are null, get the default ones from the context
        if (this.keySerializer == null) this.keySerializer = (Serializer<K>) context.keySerde().serializer();
        if (this.valSerializer == null) this.valSerializer = (Serializer<V>) context.valueSerde().serializer();

        // if value serializers are for {@code Change} values, set the inner serializer when necessary
        if (this.valSerializer instanceof ChangedSerializer &&
                ((ChangedSerializer) this.valSerializer).inner() == null)
            ((ChangedSerializer) this.valSerializer).setInner(context.valueSerde().serializer());
        this.nodeMetrics = new NodeMetrics(metrics, name(),  "task." + context.taskId());
    }


    @Override
    public void process(final K key, final V value) {
        nodeMetrics.nodeProcessTimeSensor.record();
        RecordCollector collector = ((RecordCollector.Supplier) context).recordCollector();

        final long timestamp = context.timestamp();
        if (timestamp < 0) {
            throw new StreamsException("A record consumed from an input topic has invalid (negative) timestamp, " +
                "possibly because a pre-0.10 producer client was used to write this record to Kafka without embedding a timestamp, " +
                "or because the input topic was created before upgrading the Kafka cluster to 0.10+. " +
                "Use a different TimestampExtractor to process this data.");
        }

        try {
            collector.send(new ProducerRecord<K, V>(topic, null, timestamp, key, value), keySerializer, valSerializer, partitioner);
        } catch (ClassCastException e) {
            throw new StreamsException(
                    String.format("A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                                    "(key type: %s / value type: %s). Change the default Serdes in StreamConfig or " +
                                    "provide correct Serdes via method parameters.",
                                    keySerializer.getClass().getName(),
                                    valSerializer.getClass().getName(),
                                    key.getClass().getName(),
                                    value.getClass().getName()),
                    e);
        }
    }

    @Override
    public void close() {
        nodeMetrics.removeAllSensors();
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("topic:" + topic);
        return sb.toString();
    }
}
