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

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;

import java.util.Arrays;

public class NonMaterializedKTableKTableJoinNode<K, V1, V2, VR> extends KTableKTableJoinNode<K, V1, V2, VR> {

    NonMaterializedKTableKTableJoinNode(final String nodeName,
                         final ValueJoiner<? super Change<V1>, ? super Change<V2>, ? extends Change<VR>> valueJoiner,
                         final ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
                         final ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
                         final ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters,
                         final String thisJoinSide,
                         final String otherJoinSide,
                         final String[] joinThisStoreNames,
                         final String[] joinOtherStoreNames) {

        super(nodeName,
            valueJoiner,
            joinThisProcessorParameters,
            joinOtherProcessorParameters,
            joinMergeProcessorParameters,
            thisJoinSide,
            otherJoinSide,
            joinThisStoreNames,
            joinOtherStoreNames);
    }

    @Override
    public String toString() {
        return "NonMaterializedKTableKTableJoinNode{" +
            "joinThisStoreNames=" + Arrays.toString(getJoinThisStoreNames()) +
            ", joinOtherStoreNames=" + Arrays.toString(getJoinOtherStoreNames()) +
            "} " + super.toString();
    }
}
