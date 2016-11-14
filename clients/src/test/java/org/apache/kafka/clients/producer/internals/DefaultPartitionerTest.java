/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultPartitionerTest {
    private byte[] keyBytes = "key".getBytes();
    private Partitioner partitioner = new DefaultPartitioner();
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[] {node0, node1, node2};
    private String topic = "test";
    // Intentionally make the partition list not in partition order to test the edge cases.
    private List<PartitionInfo> partitions = asList(new PartitionInfo(topic, 1, null, nodes, nodes),
                                                    new PartitionInfo(topic, 2, node1, nodes, nodes),
                                                    new PartitionInfo(topic, 0, node0, nodes, nodes));
    private Cluster cluster = new Cluster("clusterId", asList(node0, node1, node2), partitions,
            Collections.<String>emptySet(), Collections.<String>emptySet());

    @Test
    public void testKeyPartitionIsStable() {
        int partition = partitioner.partition("test",  null, keyBytes, null, null, cluster);
        assertEquals("Same key should yield same partition", partition, partitioner.partition("test", null, keyBytes, null, null, cluster));
    }

    @Test
    public void testRoundRobinWithUnavailablePartitions() {
        // When there are some unavailable partitions, we want to make sure that (1) we always pick an available partition,
        // and (2) the available partitions are selected in a round robin way.
        int countForPart0 = 0;
        int countForPart2 = 0;
        for (int i = 1; i <= 100; i++) {
            int part = partitioner.partition("test", null, null, null, null, cluster);
            assertTrue("We should never choose a leader-less node in round robin", part == 0 || part == 2);
            if (part == 0)
                countForPart0++;
            else
                countForPart2++;
        }
        assertEquals("The distribution between two available partitions should be even", countForPart0, countForPart2);
    }

    @Test
    public void testRoundRobin() throws InterruptedException {
        final String topicA = "topicA";
        final String topicB = "topicB";

        List<PartitionInfo> allPartitions = asList(new PartitionInfo(topicA, 0, node0, nodes, nodes),
                new PartitionInfo(topicA, 1, node1, nodes, nodes),
                new PartitionInfo(topicA, 2, node2, nodes, nodes),
                new PartitionInfo(topicB, 0, node0, nodes, nodes)
                );
        Cluster testCluster = new Cluster("clusterId", asList(node0, node1, node2), allPartitions,
                Collections.<String>emptySet(), Collections.<String>emptySet());

        final Map<Integer, Integer> partitionCount = new HashMap<>();

        for (int i = 0; i < 30; ++i) {
            int partition = partitioner.partition(topicA, null, null, null, null, testCluster);
            Integer count = partitionCount.get(partition);
            if (null == count) count = 0;
            partitionCount.put(partition, count + 1);

            if (i % 5 == 0) {
                partitioner.partition(topicB, null, null, null, null, testCluster);
            }
        }

        Assert.assertEquals(10, (int) partitionCount.get(0));
        Assert.assertEquals(10, (int) partitionCount.get(1));
        Assert.assertEquals(10, (int) partitionCount.get(2));
    }

    @Test
    public void testPartitionPerf() throws Exception {
        List<PartitionInfo> allPartitions = new ArrayList<PartitionInfo>();

        final int topicCount = 37;
        for(int i = 0; i < topicCount; ++i) {
            final String topici = "topic" + i;
            allPartitions.add(new PartitionInfo(topici, 0, node0, nodes, nodes));
            allPartitions.add(new PartitionInfo(topici, 1, node1, nodes, nodes));
            allPartitions.add(new PartitionInfo(topici, 2, node2, nodes, nodes));
        }
        final Cluster testCluster = new Cluster("clusterId", asList(node0, node1, node2), allPartitions,
                Collections.<String>emptySet(), Collections.<String>emptySet());

        int threadCount = 100;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        long start = System.currentTimeMillis();
        final int loopCount = 10000;
        for(int i = 0; i < threadCount; ++i) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(int j = 0; j < loopCount; ++j) {
                        String theTopic = "topic" + j % topicCount;
                        partitioner.partition(theTopic, null, null, null, null, testCluster);
                    }
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        double avg = (System.currentTimeMillis() - start)/ (double)(threadCount * loopCount);
        System.out.println("avg partition time(ms)=" + String.format("%5f", avg));
    }
}
