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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Event that signifies that the application thread has executed the
 * {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} and
 * {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callbacks. If either callback execution threw
 * an error, the <em>first</em> error encountered is included in the event should any event listener want to know.
 */
public class PartitionReconciliationCompleteEvent extends ApplicationEvent {

    private final SortedSet<TopicPartition> revokedPartitions;
    private final SortedSet<TopicPartition> assignedPartitions;
    private final Optional<KafkaException> error;

    public PartitionReconciliationCompleteEvent(SortedSet<TopicPartition> revokedPartitions,
                                                SortedSet<TopicPartition> assignedPartitions,
                                                Optional<KafkaException> error) {
        super(Type.PARTITION_RECONCILIATION_COMPLETE);
        this.revokedPartitions = Collections.unmodifiableSortedSet(revokedPartitions);
        this.assignedPartitions = Collections.unmodifiableSortedSet(assignedPartitions);
        this.error = error;
    }

    public SortedSet<TopicPartition> revokedPartitions() {
        return revokedPartitions;
    }

    public SortedSet<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    public Optional<KafkaException> error() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PartitionReconciliationCompleteEvent that = (PartitionReconciliationCompleteEvent) o;

        return revokedPartitions.equals(that.revokedPartitions) &&
                assignedPartitions.equals(that.assignedPartitions) &&
                error.equals(that.error);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + revokedPartitions.hashCode();
        result = 31 * result + assignedPartitions.hashCode();
        result = 31 * result + error.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", revokedPartitions=" + revokedPartitions +
                ", assignedPartitions=" + assignedPartitions +
                ", error=" + error;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
