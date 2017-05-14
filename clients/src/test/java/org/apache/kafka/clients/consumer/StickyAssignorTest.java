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
package org.apache.kafka.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

public class StickyAssignorTest {

    private StickyAssignor assignor = new StickyAssignor();

    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, Collections.<String>emptyList());

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 0);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumerId));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);
        Map<String, List<String>> subscriptions = Collections.singletonMap(consumerId, topics(topic1, topic2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic1, 0), tp(topic2, 0), tp(topic2, 1)), assignment.get(consumerId));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 0)), assignment.get(consumer1));
        assertEquals(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 0)), assignment.get(consumer1));
        assertEquals(Arrays.asList(tp(topic, 1)), assignment.get(consumer2));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testMultipleConsumersMixedTopicSubscriptions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic1));
        subscriptions.put(consumer2, topics(topic1, topic2));
        subscriptions.put(consumer3, topics(topic1));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic1, 0), tp(topic1, 2)), assignment.get(consumer1));
        assertEquals(Arrays.asList(tp(topic2, 0), tp(topic2, 1)), assignment.get(consumer2));
        assertEquals(Arrays.asList(tp(topic1, 1)), assignment.get(consumer3));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic1, topic2));
        subscriptions.put(consumer2, topics(topic1, topic2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic1, 0), tp(topic1, 2), tp(topic2, 1)), assignment.get(consumer1));
        assertEquals(Arrays.asList(tp(topic1, 1), tp(topic2, 0), tp(topic2, 2)), assignment.get(consumer2));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testAddRemoveConsumerOneTopic() {
        String topic = "topic";
        String consumer1 = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 0), tp(topic, 1), tp(topic, 2)), assignment.get(consumer1));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));

        String consumer2 = "consumer2";
        subscriptions.put(consumer2, topics(topic));
        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertEquals(Arrays.asList(tp(topic, 1), tp(topic, 2)), assignment.get(consumer1));
        assertEquals(Arrays.asList(tp(topic, 0)), assignment.get(consumer2));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));

        subscriptions.remove(consumer1);
        oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        assertTrue(assignment.get(consumer2).contains(tp(topic, 0)));
        assertTrue(assignment.get(consumer2).contains(tp(topic, 1)));
        assertTrue(assignment.get(consumer2).contains(tp(topic, 2)));

        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    /**
     * This unit test performs sticky assignment for a scenario that round robin assignor handles poorly.
     * Topics (partitions per topic): topic1 (2), topic2 (1), topic3 (2), topic4 (1), topic5 (2)
     * Subscriptions:
     *  - consumer1: topic1, topic2, topic3, topic4, topic5
     *  - consumer2: topic1, topic3, topic5
     *  - consumer3: topic1, topic3, topic5
     *  - consumer4: topic1, topic2, topic3, topic4, topic5
     * Round Robin Assignment Result:
     *  - consumer1: topic1-0, topic3-0, topic5-0
     *  - consumer2: topic1-1, topic3-1, topic5-1
     *  - consumer3:
     *  - consumer4: topic2-0, topic4-0
     * Sticky Assignment Result:
     *  - consumer1: topic2-0, topic3-0
     *  - consumer2: topic1-0, topic3-1
     *  - consumer3: topic1-1, topic5-0
     *  - consumer4: topic4-0, topic5-1
     */
    @Test
    public void testPoorRoundRobinAssignmentScenario() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 5; i++)
            partitionsPerTopic.put(String.format("topic%d", i), (i % 2) + 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer1", Arrays.asList("topic1", "topic2", "topic3", "topic4", "topic5"));
        subscriptions.put("consumer2", Arrays.asList("topic1", "topic3", "topic5"));
        subscriptions.put("consumer3", Arrays.asList("topic1", "topic3", "topic5"));
        subscriptions.put("consumer4", Arrays.asList("topic1", "topic2", "topic3", "topic4", "topic5"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
    }

    @Test
    public void testAddRemoveTopicTwoConsumers() {
        String topic = "topic";
        String consumer1 = "consumer";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put(consumer1, topics(topic));
        subscriptions.put(consumer2, topics(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        // verify balance
        assertTrue(isFullyBalanced(assignment));
        verifyValidityAndBalance(subscriptions, assignment);
        // verify stickiness
        List<TopicPartition> consumer1Assignment1 = assignment.get(consumer1);
        List<TopicPartition> consumer2Assignment1 = assignment.get(consumer2);
        assertTrue((consumer1Assignment1.size() == 1 && consumer2Assignment1.size() == 2) ||
                   (consumer1Assignment1.size() == 2 && consumer2Assignment1.size() == 1));

        String topic2 = "topic2";
        partitionsPerTopic.put(topic2, 3);
        subscriptions.put(consumer1, topics(topic, topic2));
        subscriptions.put(consumer2, topics(topic, topic2));
        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        // verify balance
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
        // verify stickiness
        List<TopicPartition> consumer1assignment = assignment.get(consumer1);
        List<TopicPartition> consumer2assignment = assignment.get(consumer2);
        assertTrue(consumer1assignment.size() == 3 && consumer2assignment.size() == 3);
        assertTrue(consumer1assignment.containsAll(consumer1Assignment1));
        assertTrue(consumer2assignment.containsAll(consumer2Assignment1));
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));

        partitionsPerTopic.remove(topic);
        subscriptions.put(consumer1, topics(topic2));
        subscriptions.put(consumer2, topics(topic2));
        oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        // verify balance
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(isFullyBalanced(assignment));
        // verify stickiness
        List<TopicPartition> consumer1Assignment3 = assignment.get(consumer1);
        List<TopicPartition> consumer2Assignment3 = assignment.get(consumer2);
        assertTrue((consumer1Assignment3.size() == 1 && consumer2Assignment3.size() == 2) ||
                   (consumer1Assignment3.size() == 2 && consumer2Assignment3.size() == 1));
        assertTrue(consumer1assignment.containsAll(consumer1Assignment3));
        assertTrue(consumer2assignment.containsAll(consumer2Assignment3));
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testReassignmentAfterOneConsumerLeaves() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 20; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), i);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 20; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 1; j <= i; j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);

        subscriptions.remove("consumer10");
        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testReassignmentAfterOneConsumerAdded() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic", 20);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 10; i++)
            subscriptions.put(String.format("consumer%02d", i), Collections.singletonList("topic"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);

        subscriptions.put("consumer10", Collections.singletonList("topic"));
        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testSameSubscriptions() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 15; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), i);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 1; i < 9; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 1; j <= partitionsPerTopic.size(); j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);

        subscriptions.remove("consumer05");
        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testLargeAssignmentWithMultipleConsumersLeaving() {
        Random rand = new Random();
        int topicCount = 40;
        int consumerCount = 200;

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 0; i < topicCount; i++)
            partitionsPerTopic.put(getTopicName(i, topicCount), rand.nextInt(10) + 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 0; i < consumerCount; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = 0; j < rand.nextInt(20); j++)
                topics.add(getTopicName(rand.nextInt(topicCount), topicCount));
            subscriptions.put(getConsumerName(i, consumerCount), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);

        for (int i = 0; i < 100; ++i) {
            String c = getConsumerName(rand.nextInt(consumerCount), consumerCount);
            subscriptions.remove(c);
        }

        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testNewSubscription() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i < 5; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            List<String> topics = new ArrayList<String>();
            for (int j = i; j <= 3 * i - 2; j++)
                topics.add(String.format("topic%02d", j));
            subscriptions.put(String.format("consumer%02d", i), topics);
        }

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);

        subscriptions.get("consumer00").add("topic01");

        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
    }

    @Test
    public void testReassignmentWithRandomSubscriptionsAndChanges() {
        final int minNumConsumers = 20;
        final int maxNumConsumers = 40;
        final int minNumTopics = 10;
        final int maxNumTopics = 20;

        for (int round = 1; round <= 100; ++round) {
            int numTopics = minNumTopics + new Random().nextInt(maxNumTopics - minNumTopics);

            ArrayList<String> topics = new ArrayList<>();
            for (int i = 0; i < numTopics; ++i)
                topics.add(getTopicName(i, maxNumTopics));

            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (int i = 0; i < numTopics; ++i)
                partitionsPerTopic.put(getTopicName(i, maxNumTopics), i + 1);

            int numConsumers = minNumConsumers + new Random().nextInt(maxNumConsumers - minNumConsumers);

            Map<String, List<String>> subscriptions = new HashMap<>();
            for (int i = 0; i < numConsumers; ++i) {
                List<String> sub = Utils.sorted(getRandomSublist(topics));
                subscriptions.put(getConsumerName(i, maxNumConsumers), sub);
            }

            StickyAssignor assignor = new StickyAssignor();

            Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
            verifyValidityAndBalance(subscriptions, assignment);

            subscriptions.clear();
            for (int i = 0; i < numConsumers; ++i) {
                List<String> sub = Utils.sorted(getRandomSublist(topics));
                subscriptions.put(getConsumerName(i, maxNumConsumers), sub);
            }

            Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
            assignment = assignor.assign(partitionsPerTopic, subscriptions);
            verifyValidityAndBalance(subscriptions, assignment);
            assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));
        }
    }

    private String getTopicName(int i, int maxNum) {
        return getCanonicalName("t", i, maxNum);
    }

    private String getConsumerName(int i, int maxNum) {
        return getCanonicalName("c", i, maxNum);
    }

    private String getCanonicalName(String str, int i, int maxNum) {
        return str + pad(i, Integer.toString(maxNum).length());
    }

    private String pad(int num, int digits) {
        StringBuilder sb = new StringBuilder();
        int iDigits = Integer.toString(num).length();

        for (int i = 1; i <= digits - iDigits; ++i)
            sb.append("0");

        sb.append(num);
        return sb.toString();
    }

    @Test
    public void testMoveExistingAssignments() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (int i = 1; i <= 6; i++)
            partitionsPerTopic.put(String.format("topic%02d", i), 1);

        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer01", topics("topic01", "topic02"));
        subscriptions.put("consumer02", topics("topic01", "topic02", "topic03", "topic04"));
        subscriptions.put("consumer03", topics("topic02", "topic03", "topic04", "topic05", "topic06"));

        assignor.currentAssignment.put("consumer01", new ArrayList<>(Arrays.asList(tp("topic01", 0))));
        assignor.currentAssignment.put("consumer02", new ArrayList<>(Arrays.asList(tp("topic02", 0), tp("topic03", 0))));
        assignor.currentAssignment.put("consumer03", new ArrayList<>(Arrays.asList(tp("topic04", 0), tp("topic05", 0), tp("topic06", 0))));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
    }

    @Test
    public void testStickiness() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put("topic01", 3);
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("consumer01", topics("topic01"));
        subscriptions.put("consumer02", topics("topic01"));
        subscriptions.put("consumer03", topics("topic01"));
        subscriptions.put("consumer04", topics("topic01"));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        Map<String, TopicPartition> partitionsAssigned = new HashMap<>();

        Set<Entry<String, List<TopicPartition>>> assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            int size = topicPartitions.size();
            assertTrue("Consumer " + consumer + " is assigned more topic partitions than expected.", size <= 1);
            if (size == 1)
                partitionsAssigned.put(consumer, topicPartitions.get(0));
        }

        // removing the potential group leader
        subscriptions.remove("consumer01");

        Map<String, List<TopicPartition>> oldAssignment = assignor.deepCopy(assignment);
        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        verifyValidityAndBalance(subscriptions, assignment);
        assertTrue(PartitionMovements.verifyStickiness(oldAssignment, assignment));

        assignments = assignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
            String consumer = entry.getKey();
            List<TopicPartition> topicPartitions = entry.getValue();
            assertEquals("Consumer " + consumer + " is assigned more topic partitions than expected.", 1, topicPartitions.size());
            assertTrue("Stickiness was not honored for consumer " + consumer,
                    (!partitionsAssigned.containsKey(consumer)) || (assignment.get(consumer).contains(partitionsAssigned.get(consumer))));
        }
    }

    public static List<String> topics(String... topics) {
        return Arrays.asList(topics);
    }

    public static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    private static boolean isFullyBalanced(Map<String, List<TopicPartition>> assignment) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (List<TopicPartition> topicPartitions: assignment.values()) {
            int size = topicPartitions.size();
            if (size < min)
                min = size;
            if (size > max)
                max = size;
        }
        return max - min <= 1;
    }

    private static List<String> getRandomSublist(ArrayList<String> list) {
        List<String> selectedItems = new ArrayList<>(list);
        int len = list.size();
        Random random = new Random();
        int howManyToRemove = random.nextInt(len);

        for (int i = 1; i <= howManyToRemove; ++i)
            selectedItems.remove(random.nextInt(selectedItems.size()));

        return selectedItems;
    }

    /**
     * Verifies that the given assignment is valid and balanced with respect to the given subscriptions
     * Validity requirements:
     * - each consumer is subscribed to topics of all partitions assigned to it, and
     * - each partition is assigned to no more than one consumer
     * Balance requirements:
     * - the assignment is fully balanced (the numbers of topic partitions assigned to consumers differ by at most one), or
     * - there is no topic partition that can be moved from one consumer to another with 2+ fewer topic partitions
     *
     * @param subscriptions: topic subscriptions of each consumer
     * @param assignment: given assignment for balance check
     */
    private static void verifyValidityAndBalance(Map<String, List<String>> subscriptions, Map<String, List<TopicPartition>> assignments) {
        int size = subscriptions.size();
        assert size == assignments.size();

        List<String> consumers = Utils.sorted(assignments.keySet());

        for (int i = 0; i < size; ++i) {
            String consumer = consumers.get(i);
            List<TopicPartition> partitions = assignments.get(consumer);
            for (TopicPartition partition: partitions)
                assertTrue("Error: Partition " + partition + "is assigned to c" + i + ", but it is not subscribed to Topic t" + partition.topic()
                        + "\nSubscriptions: " + subscriptions.toString() + "\nAssignments: " + assignments.toString(),
                        subscriptions.get(consumer).contains(partition.topic()));

            if (i == size - 1)
                continue;

            for (int j = i + 1; j < size; ++j) {
                String otherConsumer = consumers.get(j);
                List<TopicPartition> otherPartitions = assignments.get(otherConsumer);

                Set<TopicPartition> intersection = new HashSet<>(partitions);
                intersection.retainAll(otherPartitions);
                assertTrue("Error: Consumers c" + i + " and c" + j + " have common partitions assigned to them: " + intersection.toString()
                        + "\nSubscriptions: " + subscriptions.toString() + "\nAssignments: " + assignments.toString(),
                        intersection.isEmpty());

                int len = partitions.size();
                int otherLen = otherPartitions.size();

                if (Math.abs(len - otherLen) <= 1)
                    continue;

                Map<String, List<Integer>> map = asMap(partitions);
                Map<String, List<Integer>> otherMap = asMap(otherPartitions);

                if (len > otherLen) {
                    for (String topic: map.keySet())
                        assertTrue("Error: Some partitions can be moved from c" + i + " to c" + j + " to achieve a better balance"
                                + "\nc" + i + " has " + len + " partitions, and c" + j + " has " + otherLen + " partitions."
                                + "\nSubscriptions: " + subscriptions.toString() + "\nAssignments: " + assignments.toString(),
                                !otherMap.containsKey(topic));
                }

                if (otherLen > len) {
                    for (String topic: otherMap.keySet())
                        assertTrue("Error: Some partitions can be moved from c" + j + " to c" + i + " to achieve a better balance"
                                + "\nc" + i + " has " + len + " partitions, and c" + j + " has " + otherLen + " partitions."
                                + "\nSubscriptions: " + subscriptions.toString() + "\nAssignments: " + assignments.toString(),
                                !map.containsKey(topic));
                }
            }
        }
    }

    private static Map<String, List<Integer>> asMap(Collection<TopicPartition> partitions) {
        Map<String, List<Integer>> partitionMap = new HashMap<>();
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();
            List<Integer> topicPartitions = partitionMap.get(topic);
            if (topicPartitions == null) {
                topicPartitions = new ArrayList<>();
                partitionMap.put(topic, topicPartitions);
            }
            topicPartitions.add(partition.partition());
        }
        return partitionMap;
    }
}
