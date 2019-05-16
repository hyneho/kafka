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

package org.apache.kafka.clients.admin;


import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * The result of {@link AdminClient#electLeaders(ElectionType, Collection, ElectLeadersOptions)}
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
final public class ElectLeadersResult {
    private final KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> electionFuture;

    ElectLeadersResult(KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> electionFuture) {
        this.electionFuture = electionFuture;
    }

    /**
     * <p>Get a future for the topic partitions for which a leader election was attempted.
     * If the election succeeded then the value for a topic partition will be the empty Optional.
     * Otherwise the election failed and the Optional will be set with the error.</p>
     */
    public KafkaFuture<Map<TopicPartition, Optional<Throwable>>> partitions() {
        return electionFuture;
    }
}
