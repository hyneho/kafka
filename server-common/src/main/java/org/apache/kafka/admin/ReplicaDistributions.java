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
