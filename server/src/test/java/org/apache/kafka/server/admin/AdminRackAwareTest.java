package org.apache.kafka.server.admin;

import org.apache.kafka.admin.AdminUtils;
import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.admin.ReplicaDistributions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AdminRackAwareTest{

    public static Map<Integer, List<Integer>> replicaToBrokerAssignmentAsJava(Map<Integer, List<Integer>> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                                .map(Integer::intValue)
                                .collect(Collectors.toList())
                ));
    }
    public static Collection<BrokerMetadata> toBrokerMetadata(Map<Integer, String> rackMap, List<Integer> brokersWithoutRack) {
        List<BrokerMetadata> res = rackMap.entrySet().stream()
                .map(entry -> new BrokerMetadata(entry.getKey(), Optional.ofNullable(entry.getValue())))
                .collect(Collectors.toList());

        res.addAll(brokersWithoutRack.stream()
                .map(brokerId -> new BrokerMetadata(brokerId, Optional.empty()))
                .collect(Collectors.toList()));

        res.sort(Comparator.comparingInt(b -> b.id));

        return res;
    }

    public static ReplicaDistributions getReplicaDistribution(Map<Integer, List<Integer>> assignment, Map<Integer, String> brokerRackMapping) {
        Map<Integer, Integer> leaderCount = new HashMap<>();
        Map<Integer, Integer> partitionCount = new HashMap<>();
        Map<Integer, List<String>> partitionRackMap = new HashMap<>();

        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            Integer partitionId = entry.getKey();
            List<Integer> replicaList = entry.getValue();

            // The leader is the first broker in the list
            Integer leader = replicaList.get(0);
            leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1);

            for (Integer brokerId : replicaList) {
                partitionCount.put(brokerId, partitionCount.getOrDefault(brokerId, 0) + 1);

                String rack = brokerRackMapping.get(brokerId);

                // Add the rack to the list of racks for this partition
                partitionRackMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(rack);
            }
        }

        return new ReplicaDistributions(partitionRackMap, leaderCount, partitionCount);
    }

    public static void assertReplicaDistribution(
            Map<Integer, List<Integer>> assignment,
            Map<Integer, String> brokerRackMapping,
            int numBrokers,
            int numPartitions,
            int replicationFactor) {

        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            List<Integer> brokerList = entry.getValue();
            Set<Integer> brokerSet = new HashSet<>(brokerList);
            assertEquals(brokerSet.size(), brokerList.size());
        }

        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        // Verify Rack-Awareness
        Map<Integer, List<String>> partitionRackMap = distribution.getPartitionRacks();
        List<Integer> distinctRackSizes = partitionRackMap.values().stream()
                .map(rackList -> (int) rackList.stream().distinct().count())
                .collect(Collectors.toList());

        List<Integer> expectedRackSizes = Collections.nCopies(numPartitions, replicationFactor);
        assertEquals(expectedRackSizes, distinctRackSizes);


        // Verify Leader Distribution
        Map<Integer, Integer> leaderCount = distribution.getBrokerLeaderCount();
        int leaderCountPerBroker = numPartitions / numBrokers;

        List<Integer> expectedLeaderCounts = Collections.nCopies(numBrokers, leaderCountPerBroker);
        List<Integer> actualLeaderCounts = new ArrayList<>(leaderCount.values());

        assertEquals(expectedLeaderCounts, actualLeaderCounts);


        // Verify Replicas Distribution
        Map<Integer, Integer> replicasCount = distribution.getBrokerReplicasCount();
        int numReplicasPerBroker = (numPartitions * replicationFactor) / numBrokers;

        List<Integer> expectedReplicasCounts = Collections.nCopies(numBrokers, numReplicasPerBroker);
        List<Integer> actualReplicasCounts = new ArrayList<>(replicasCount.values());

        assertEquals(expectedReplicasCounts, actualReplicasCounts);
    }

    @Test
    public void testGetRackAlternatedBrokerListAndAssignReplicasToBrokers() {
        Map<Integer, String> rackMap = Map.of(
                0, "rack1",
                1, "rack3",
                2, "rack3",
                3, "rack2",
                4, "rack2",
                5, "rack1"
        );

        List<Integer> newList = AdminUtils.getRackAlternatedBrokerList(rackMap);
        assertEquals(Arrays.asList(0, 3, 1, 5, 4, 2), newList);

        Map<Integer, String> anotherRackMap = new HashMap<>(rackMap);
        anotherRackMap.remove(5);
        List<Integer> anotherList = AdminUtils.getRackAlternatedBrokerList(anotherRackMap);
        assertEquals(Arrays.asList(0, 3, 1, 4, 2), anotherList);

        Map<Integer, List<Integer>> assignment = replicaToBrokerAssignmentAsJava(AdminUtils.
                assignReplicasToBrokers(toBrokerMetadata(rackMap, null), 7, 3, 0, 0));


        Map<Integer, List<Integer>> expected = Map.of(
                0, Arrays.asList(0, 3, 1),
                1, Arrays.asList(3, 1, 5),
                2, Arrays.asList(1, 5, 4),
                3, Arrays.asList(5, 4, 2),
                4, Arrays.asList(4, 2, 0),
                5, Arrays.asList(2, 0, 3),
                6, Arrays.asList(0, 4, 2)
        );

        assertEquals(expected, assignment);
    }

    @Test
    public void testAssignmentWithRackAware() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment = replicaToBrokerAssignmentAsJava(
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping, null), numPartitions, replicationFactor, 2, 0)
        );

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor);
    }

    @Test
    public void testAssignmentWithRackAwareWithRandomStartIndex() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping, null), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor);
    }

    @Test
    public void testAssignmentWithRackAwareWithUnevenReplicas() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 13;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment = CoreUtils.replicaToBrokerAssignmentAsMap(
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping, null), numPartitions, replicationFactor, 0, 0)
        );

        checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor, false, false);
    }

    @Test
    public void testAssignmentWithRackAwareWithUnevenRacks() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack1",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment = CoreUtils.replicaToBrokerAssignmentAsMap(
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
        );

        checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor, false);
    }

    @Test
    public void testAssignmentWith2ReplicasRackAware() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 2;
        Map<Integer, List<Integer>> assignment = CoreUtils.replicaToBrokerAssignmentAsMap(
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor)
        );

        checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor);
    }

    @Test
    public void testRackAwareExpansion() {
        Map<Integer, String> brokerRackMapping = Map.of(
                6, "rack1",
                7, "rack2",
                8, "rack2",
                9, "rack3",
                10, "rack3",
                11, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 2;
        Map<Integer, List<Integer>> assignment = CoreUtils.replicaToBrokerAssignmentAsMap(
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor, -1, 12)
        );

        checkReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(), numPartitions, replicationFactor);
    }

    // Similar Java translations can be made for other test cases.
}

