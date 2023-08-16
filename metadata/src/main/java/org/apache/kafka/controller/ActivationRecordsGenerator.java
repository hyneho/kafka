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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.AbortTransactionRecord;
import org.apache.kafka.common.metadata.BeginTransactionRecord;
import org.apache.kafka.common.metadata.EndTransactionRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ActivationRecordsGenerator {

    static ControllerResult<Void> recordsForEmptyLog(
        Logger log,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        BootstrapMetadata bootstrapMetadata,
        MetadataVersion metadataVersion
    ) {
        List<ApiMessageAndVersion> records = new ArrayList<>();

        if (transactionStartOffset != -1L) {
            // In-flight bootstrap transaction
            if (!metadataVersion.isMetadataTransactionSupported()) {
                throw new RuntimeException("Detected partial bootstrap records transaction at " +
                    transactionStartOffset + ", but the metadata.version " + metadataVersion +
                    " does not support transactions. Cannot continue.");
            } else {
                log.warn("Detected partial bootstrap records transaction at " + transactionStartOffset +
                    " during controller activation. Aborting this transaction and re-committing bootstrap records");
                records.add(new ApiMessageAndVersion(
                    new AbortTransactionRecord().setReason("Controller failover"), (short) 0));
                records.add(new ApiMessageAndVersion(
                    new BeginTransactionRecord().setName("Bootstrap records"), (short) 0));
            }
        } else if (metadataVersion.isMetadataTransactionSupported()) {
            // No in-flight transaction
            records.add(new ApiMessageAndVersion(
                new BeginTransactionRecord().setName("Bootstrap records"), (short) 0));
        }

        // If no records have been replayed, we need to write out the bootstrap records.
        // This will include the new metadata.version, as well as things like SCRAM
        // initialization, etc.
        log.info("The metadata log appears to be empty. Appending {} bootstrap record(s) " +
            "at metadata.version {} from {}.",
            bootstrapMetadata.records().size(),
            metadataVersion,
            bootstrapMetadata.source());
        records.addAll(bootstrapMetadata.records());

        if (metadataVersion.isMigrationSupported()) {
            if (zkMigrationEnabled) {
                log.info("Putting the controller into pre-migration mode. No metadata updates will be allowed " +
                    "until the ZK metadata has been migrated");
                records.add(ZkMigrationState.PRE_MIGRATION.toRecord());
            } else {
                log.debug("Setting the ZK migration state to NONE since this is a de-novo KRaft cluster.");
                records.add(ZkMigrationState.NONE.toRecord());
            }
        } else {
            if (zkMigrationEnabled) {
                throw new RuntimeException("The bootstrap metadata.version " + bootstrapMetadata.metadataVersion() +
                    " does not support ZK migrations. Cannot continue with ZK migrations enabled.");
            }
        }

        if (metadataVersion.isMetadataTransactionSupported()) {
            records.add(new ApiMessageAndVersion(new EndTransactionRecord(), (short) 0));
            return ControllerResult.of(records, null);
        } else {
            return ControllerResult.atomicOf(records, null);
        }
    }

    static ControllerResult<Void> recordsForNonEmptyLog(
        Logger log,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        FeatureControlManager featureControl,
        MetadataVersion metadataVersion
    ) {
        // Logs have been replayed. We need to initialize some things here if upgrading from older KRaft versions
        List<ApiMessageAndVersion> records = new ArrayList<>();

        if (metadataVersion.equals(MetadataVersion.MINIMUM_KRAFT_VERSION)) {
            log.info("No metadata.version feature level record was found in the log. " +
                "Treating the log as version {}.", MetadataVersion.MINIMUM_KRAFT_VERSION);
        }

        if (metadataVersion.isMigrationSupported()) {
            log.info("Loaded ZK migration state of {}", featureControl.zkMigrationState());
            switch (featureControl.zkMigrationState()) {
                case NONE:
                    // Since this is the default state there may or may not be an actual NONE in the log. Regardless,
                    // it will eventually be persisted in a snapshot, so we don't need to explicitly write it here.
                    if (zkMigrationEnabled) {
                        throw new RuntimeException("Should not have ZK migrations enabled on a cluster that was " +
                            "created in KRaft mode.");
                    }
                    break;
                case PRE_MIGRATION:
                    log.warn("Activating pre-migration controller without empty log. There may be a partial migration");
                    break;
                case MIGRATION:
                    if (!zkMigrationEnabled) {
                        // This can happen if controller leadership transfers to a controller with migrations enabled
                        // after another controller had finalized the migration. For example, during a rolling restart
                        // of the controller quorum during which the migration config is being set to false.
                        log.warn("Completing the ZK migration since this controller was configured with " +
                            "'zookeeper.metadata.migration.enable' set to 'false'.");
                        records.add(ZkMigrationState.POST_MIGRATION.toRecord());
                    } else {
                        log.info("Staying in the ZK migration since 'zookeeper.metadata.migration.enable' is still " +
                            "'true'.");
                    }
                    break;
                case POST_MIGRATION:
                    if (zkMigrationEnabled) {
                        log.info("Ignoring 'zookeeper.metadata.migration.enable' value of 'true' since the ZK " +
                            "migration has been completed.");
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported ZkMigrationState " + featureControl.zkMigrationState());
            }
        } else {
            if (zkMigrationEnabled) {
                throw new RuntimeException("Should not have ZK migrations enabled on a cluster running " +
                    "metadata.version " + featureControl.metadataVersion());
            }
        }

        if (transactionStartOffset != -1L) {
            if (!metadataVersion.isMetadataTransactionSupported()) {
                throw new RuntimeException("Detected in-progress transaction at " + transactionStartOffset +
                    ", but the metadata.version " + metadataVersion +
                    " does not support transactions. Cannot continue.");
            } else {
                log.warn("Detected in-progress transaction at " + transactionStartOffset +
                    " during controller activation. Aborting this transaction.");
                records.add(new ApiMessageAndVersion(
                    new AbortTransactionRecord().setReason("Controller failover"), (short) 0));
            }
        }

        return ControllerResult.of(records, null);
    }

    /**
     * Generate the set of activation records.
     * </p>
     * If the log is empty, write the bootstrap records. If the log is not empty, do some validation and
     * possibly write some records to put the log into a valid state. For bootstrap records, if KIP-868
     * metadata transactions are supported, ues them. Otherwise, write the bootstrap records as an
     * atomic batch. The single atomic batch can be problematic if the bootstrap records are too large
     * (e.g., lots of SCRAM credentials). If ZK migrations are enabled, the activation records will
     * include a ZkMigrationState record regardless of whether the log was empty or not.
     */
    static ControllerResult<Void> generate(
        Logger log,
        long stableOffset,
        long transactionStartOffset,
        boolean zkMigrationEnabled,
        BootstrapMetadata bootstrapMetadata,
        FeatureControlManager featureControl
    ) {
        if (stableOffset == -1L) {
            return recordsForEmptyLog(log, transactionStartOffset, zkMigrationEnabled,
                bootstrapMetadata, bootstrapMetadata.metadataVersion());
        } else {
            return recordsForNonEmptyLog(log, transactionStartOffset, zkMigrationEnabled,
                featureControl, featureControl.metadataVersion());
        }
    }
}
