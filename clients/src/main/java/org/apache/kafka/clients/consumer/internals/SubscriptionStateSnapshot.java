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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionStateSnapshot {

    private final Map<TopicPartition, SnapshotState> assignment;

    private final Set<String> subscription;

    private final boolean hasNoSubscriptionOrUserAssignment;

    SubscriptionStateSnapshot(final Map<TopicPartition, SnapshotState> assignment,
                              final Set<String> subscription,
                              final boolean hasNoSubscriptionOrUserAssignment) {
        this.assignment = Collections.unmodifiableMap(assignment);
        this.subscription = Collections.unmodifiableSet(subscription);
        this.hasNoSubscriptionOrUserAssignment = hasNoSubscriptionOrUserAssignment;
    }

    SubscriptionStateSnapshot() {
        this.assignment = Collections.emptyMap();
        this.subscription = Collections.emptySet();
        this.hasNoSubscriptionOrUserAssignment = true;
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        return hasNoSubscriptionOrUserAssignment;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        return assignment.entrySet().stream()
            .filter(entry -> entry.getValue().offsetAndMetadata.isPresent())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().offsetAndMetadata.get()));
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.containsKey(tp);
    }

    public SubscriptionState.FetchPosition validPosition(TopicPartition tp) {
        SnapshotState state = assignment.get(tp);

        if (state == null)
            throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

        return state.validPosition.orElse(null);
    }

    public Set<TopicPartition> pausedPartitions() {
        return assignment.entrySet().stream()
            .filter(entry -> entry.getValue().paused)
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
    }

    public Set<TopicPartition> assignedPartitions() {
        return assignment.keySet();
    }

    public Set<String> subscription() {
        return subscription;
    }

    static class SnapshotState {

        private final boolean paused;

        private final Optional<SubscriptionState.FetchPosition> validPosition;

        private final Optional<OffsetAndMetadata> offsetAndMetadata;

        SnapshotState(final boolean paused,
                      final Optional<SubscriptionState.FetchPosition> validPosition,
                      final Optional<OffsetAndMetadata> offsetAndMetadata) {
            this.paused = paused;
            this.validPosition = validPosition;
            this.offsetAndMetadata = offsetAndMetadata;
        }

        @Override
        public String toString() {
            return "SnapshotState{" +
                "paused=" + paused +
                ", validPosition=" + validPosition +
                ", offsetAndMetadata=" + offsetAndMetadata +
                '}';
        }
    }

    @Override
    public String toString() {
        return "SubscriptionStateSnapshot{" +
            "assignment=" + assignment +
            ", subscription=" + subscription +
            ", hasNoSubscriptionOrUserAssignment=" + hasNoSubscriptionOrUserAssignment +
            '}';
    }
}
