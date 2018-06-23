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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, V1, V2, VR> extends BaseJoinProcessorNode<K, V1, V2, VR> {

    private final ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
    private final ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
    private final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
    private final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
    private final Joined<K, V1, V2> joined;


    StreamStreamJoinNode(final String nodeName,
                         final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner,
                         final ProcessorParameters<K, V1> joinThisProcessorParameters,
                         final ProcessorParameters<K, V2> joinOtherProcessParameters,
                         final ProcessorParameters<K, VR> joinMergeProcessorParameters,
                         final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder,
                         final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder,
                         final Joined<K, V1, V2> joined,
                         final StreamsGraphNode lhsWindowedStreamsNode,
                         final StreamsGraphNode otherWindowedStreamsGraphNode) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowStoreBuilder = thisWindowStoreBuilder;
        this.otherWindowStoreBuilder = otherWindowStoreBuilder;
        this.joined = joined;
        this.thisWindowedStreamProcessorParameters = ((ProcessorNode) lhsWindowedStreamsNode).processorParameters();
        this.otherWindowedStreamProcessorParameters = ((ProcessorNode) otherWindowedStreamsGraphNode).processorParameters();

    }



    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        topologyBuilder.addProcessor(thisProcessorParameters().processorName(), thisProcessorParameters().processorSupplier(), thisWindowedStreamProcessorParameters.processorName());
        topologyBuilder.addProcessor(otherProcessorParameters().processorName(), otherProcessorParameters().processorSupplier(), otherWindowedStreamProcessorParameters.processorName());
        topologyBuilder.addProcessor(mergeProcessorParameters().processorName(), mergeProcessorParameters().processorSupplier(), thisProcessorParameters().processorName(), otherProcessorParameters().processorName());
        topologyBuilder.addStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorParameters.processorName(), otherProcessorParameters().processorName());
        topologyBuilder.addStateStore(otherWindowStoreBuilder, otherWindowedStreamProcessorParameters.processorName(), thisProcessorParameters().processorName());
    }

    public static <K, V, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder() {
        return new StreamStreamJoinNodeBuilder<>();
    }

    public static final class StreamStreamJoinNodeBuilder<K, V1, V2, VR> {

        private String nodeName;
        private ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;
        private StreamsGraphNode leftHandSideGraphNode;
        private StreamsGraphNode otherSideGraphNode;


        private StreamStreamJoinNodeBuilder() {
        }


        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(final ValueJoiner<? super V1, ? super V2, ? extends VR> valueJoiner) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(final ProcessorParameters<K, V1> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(final ProcessorParameters<K, V2> joinOtherProcessParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(final ProcessorParameters<K, VR> joinMergeProcessorParameters) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withLeftHandWindowedStreamsGraphNode(final StreamsGraphNode leftHandSideGraphNode) {
            this.leftHandSideGraphNode = leftHandSideGraphNode;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamsGraphNode(final StreamsGraphNode otherSideGraphNode) {
            this.otherSideGraphNode = otherSideGraphNode;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowStoreBuilder(final StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder) {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowStoreBuilder(final StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder) {
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoined(final Joined<K, V1, V2> joined) {
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> build() {

            return new StreamStreamJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisWindowStoreBuilder,
                                              otherWindowStoreBuilder,
                                              joined,
                                              leftHandSideGraphNode,
                                              otherSideGraphNode);


        }
    }
}
