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
package org.apache.kafka.raft;

import org.apache.kafka.common.Node;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RaftTestUtil {
    public static RaftConfig buildRaftConfig(
            int requestTimeoutMs,
            int retryBackoffMs,
            int electionTimeoutMs,
            int electionBackoffMs,
            int fetchTimeoutMs,
            int appendLingerMs,
            List<Node> voterNodes
    ) {
        Map<Integer, InetSocketAddress> voterConnections = voterNodes.stream()
            .collect(Collectors.toMap(Node::id, node -> new InetSocketAddress(node.host(), node.port())));
        return new RaftConfig(voterConnections, requestTimeoutMs, retryBackoffMs, electionTimeoutMs, electionBackoffMs,
            fetchTimeoutMs, appendLingerMs);
    }

    public static List<Node> voterNodesFromIds(Set<Integer> voterIds,
                                               Function<Integer, InetSocketAddress> voterAddressGenerator) {
        return voterIds.stream().map(voterId -> {
            InetSocketAddress voterAddress = voterAddressGenerator.apply(voterId);
            return new Node(voterId, voterAddress.getHostName(), voterAddress.getPort());
        }).collect(Collectors.toList());
    }
}
