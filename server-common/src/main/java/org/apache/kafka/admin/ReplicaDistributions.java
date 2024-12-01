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
package org.apache.kafka.admin;

import java.util.List;
import java.util.Map;

public class ReplicaDistributions {
    private final Map<Integer, List<String>> partitionRacks;
    private final Map<Integer, Integer> brokerLeaderCount;
    private final Map<Integer, Integer> brokerReplicasCount;

    public ReplicaDistributions(Map<Integer, List<String>> partitionRacks,
                                Map<Integer, Integer> brokerLeaderCount,
                                Map<Integer, Integer> brokerReplicasCount) {
        this.partitionRacks = partitionRacks;
        this.brokerLeaderCount = brokerLeaderCount;
        this.brokerReplicasCount = brokerReplicasCount;
    }

    public Map<Integer, List<String>> getPartitionRacks() {
        return partitionRacks;
    }

    public Map<Integer, Integer> getBrokerLeaderCount() {
        return brokerLeaderCount;
    }

    public Map<Integer, Integer> getBrokerReplicasCount() {
        return brokerReplicasCount;
    }
}
