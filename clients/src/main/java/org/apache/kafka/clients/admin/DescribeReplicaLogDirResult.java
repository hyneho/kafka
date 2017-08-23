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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;


/**
 * The result of {@link AdminClient#describeReplicaLogDir(Collection)}.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DescribeReplicaLogDirResult {
    private final Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures;

    DescribeReplicaLogDirResult(Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from replica to future which can be used to check the log directory information of individual replicas
     */
    public Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds if log directory information of all replicas are available
     */
    public KafkaFuture<Map<TopicPartitionReplica, ReplicaLogDirInfo>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(new KafkaFuture.Function<Void, Map<TopicPartitionReplica, ReplicaLogDirInfo>>() {
                @Override
                public Map<TopicPartitionReplica, ReplicaLogDirInfo> apply(Void v) {
                    Map<TopicPartitionReplica, ReplicaLogDirInfo> replicaLogDirInfos = new HashMap<>();
                    for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : futures.entrySet()) {
                        try {
                            replicaLogDirInfos.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return replicaLogDirInfos;
                }
            });
    }

    static public class ReplicaLogDirInfo {
        // The log directory of the primary replica of this partition on the given broker.
        // Null if the primary replica is not found for this partition on the given broker.
        public final String currentReplicaLogDir;
        // Defined as max(HW of partition - LEO of the primary replica, 0).
        public final long currentReplicaOffsetLag;
        // The log directory of the temporary replica of this partition on the given broker.
        // Null if the temporary replica is not found for this partition on the given broker.
        public final String temporaryReplicaLogDir;
        // The LEO of the primary replica - LEO of the temporary replica.
        // -1 if either primary or the temporary replica is not found for this partition on the given broker.
        public final long temporaryReplicaOffsetLag;

        public ReplicaLogDirInfo() {
            this(null, DescribeLogDirsResponse.INVALID_OFFSET_LAG, null, DescribeLogDirsResponse.INVALID_OFFSET_LAG);
        }

        public ReplicaLogDirInfo(String currentReplicaLogDir,
                                 long currentReplicaOffsetLag,
                                 String temporaryReplicaLogDir,
                                 long temporaryReplicaOffsetLag) {
            this.currentReplicaLogDir = currentReplicaLogDir;
            this.currentReplicaOffsetLag = currentReplicaOffsetLag;
            this.temporaryReplicaLogDir = temporaryReplicaLogDir;
            this.temporaryReplicaOffsetLag = temporaryReplicaOffsetLag;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (temporaryReplicaLogDir != null) {
                builder.append("(currentReplicaLogDir=")
                    .append(currentReplicaLogDir)
                    .append(", temporaryReplicaLogDir=")
                    .append(temporaryReplicaLogDir)
                    .append(", temporaryReplicaOffsetLag=")
                    .append(temporaryReplicaOffsetLag)
                    .append(")");
            } else {
                builder.append("ReplicaLogDirInfo(currentReplicaLogDir=").append(currentReplicaLogDir).append(")");
            }
            return builder.toString();
        }
    }
}
