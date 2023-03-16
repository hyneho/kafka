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
package org.apache.kafka.metadata.migration;

import java.util.Optional;

/**
 * The cluster-wide ZooKeeper migration state.
 *
 * An enumeration of the possible states of the ZkMigrationState field in ZkMigrationStateRecord.
 * This information is persisted in the metadata log and image.
 *
 * @see org.apache.kafka.common.metadata.ZkMigrationStateRecord
 */
public enum ZkMigrationState {
    /**
     * This is a synthetic value used internally by the controller to indicate that no decision has
     * been made about the state of a ZK migration. This value should _not_ be written into the log.
     */
    UNINITIALIZED((byte) -1),

    /**
     * The cluster was created in KRaft mode. A cluster that was created in ZK mode can never attain
     * this state; the endpoint of migration is POST_MIGRATION, instead.
     */
    NONE((byte) 0),

    /**
     * A KRaft controller has been elected with "zookeeper.metadata.migration.enable" set to "true".
     * The controller is now awaiting the preconditions for starting the migration to KRaft. In this
     * state, the metadata log does not yet contain the cluster's data. There is a metadata quorum,
     * but it is not doing anything useful yet.
     */
    PRE_MIGRATION((byte) 1),

    /**
     * The ZK data has been migrated, and the KRaft controller is now writing metadata to both ZK
     * and the metadata log. The controller will remain in this state until all of the brokers have
     * been restarted in KRaft mode.
     */
    MIGRATION((byte) 2),

    /**
     * The migration from ZK has been fully completed. The cluster is running in KRaft mode. This state
     * will persist indefinitely after the migration. In operational terms, this is the same as the NONE
     * state.
     */
    POST_MIGRATION((byte) 3);

    private final byte value;

    ZkMigrationState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static ZkMigrationState of(byte value) {
        return optionalOf(value)
            .orElseThrow(() -> new IllegalArgumentException(String.format("Value %s is not a valid Zk migration state", value)));
    }

    public static Optional<ZkMigrationState> optionalOf(byte value) {
        for (ZkMigrationState state : ZkMigrationState.values()) {
            if (state.value == value) {
                return Optional.of(state);
            }
        }
        return Optional.empty();
    }
}
