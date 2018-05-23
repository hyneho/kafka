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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.util.Properties;

/**
 * This class provides access to {@link TopologyTestDriver} protected methods.
 * It should only be used for internal testing, in the rare occasions where the
 * necessary functionality is not supported by {@link TopologyTestDriver}.
 */
public class TopologyTestDriverWrapper extends TopologyTestDriver {


    public TopologyTestDriverWrapper(final Topology topology,
                                     final Properties config) {
        super(topology, config);
    }

    /**
     * Get the processor context
     *
     * @param processorName used to search for a processor connected to this StateStore, which is set as current node
     * @return the processor context
     */
    public ProcessorContext getProcessorContext(final String processorName) {
        final ProcessorContext context = task.context();
        ((ProcessorContextImpl) context).setCurrentNode(getProcessor(processorName));
        return context;
    }

    /**
     * Get a processor by name
     *
     * @param name the name to search for
     * @return the processor matching the search name
     */
    public ProcessorNode getProcessor(final String name) {
        for (final ProcessorNode node : processorTopology.processors()) {
            if (node.name().equals(name)) {
                return node;
            }
        }
        throw new StreamsException("Could not find a processor named '" + name + "'");
    }
}
