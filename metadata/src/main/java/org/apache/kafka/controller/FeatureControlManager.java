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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FeatureLevelListener;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.MetadataVersion;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;


public class FeatureControlManager {
    private final Logger log;

    /**
     * An immutable map containing the features supported by this controller's software.
     */
    private final QuorumFeatures quorumFeatures;

    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, Short> finalizedVersions;

    /**
     * The current metadata version
     */
    private final TimelineObject<MetadataVersion> metadataVersion;

    /**
     * Collection of listeners for when features change
     */
    private final Map<String, FeatureLevelListener> listeners;

    FeatureControlManager(LogContext logContext,
                          QuorumFeatures quorumFeatures,
                          SnapshotRegistry snapshotRegistry) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataVersion = new TimelineObject<>(snapshotRegistry, MetadataVersion.UNINITIALIZED);
        this.listeners = new HashMap<>();
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(
            Map<String, Short> updates,
            Map<String, FeatureUpdate.UpgradeType> upgradeTypes,
            Map<Integer, Map<String, VersionRange>> brokerFeatures,
            boolean validateOnly) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Entry<String, Short> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(),
                    upgradeTypes.getOrDefault(entry.getKey(), FeatureUpdate.UpgradeType.UPGRADE), brokerFeatures, records));
        }

        if (validateOnly) {
            return ControllerResult.of(Collections.emptyList(), results);
        } else {
            return ControllerResult.atomicOf(records, results);
        }
    }

    ControllerResult<Map<String, ApiError>> initializeMetadataVersion(short initVersion) {
        if (finalizedVersions.containsKey(MetadataVersion.FEATURE_NAME)) {
            return ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    MetadataVersion.FEATURE_NAME,
                    new ApiError(Errors.INVALID_UPDATE_VERSION,
                        "Cannot initialize metadata.version since it has already been initialized.")
            ));
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ApiError result = updateMetadataVersion(initVersion, records::add);
        return ControllerResult.atomicOf(records, Collections.singletonMap(MetadataVersion.FEATURE_NAME, result));
    }

    void registerFeatureListener(String listenerName, FeatureLevelListener listener) {
        this.listeners.putIfAbsent(listenerName, listener);
    }

    boolean canSupportVersion(String featureName, short versionRange) {
        return quorumFeatures.localSupportedFeature(featureName)
            .filter(localRange -> localRange.contains(versionRange))
            .isPresent();
    }

    boolean featureExists(String featureName) {
        return quorumFeatures.localSupportedFeature(featureName).isPresent();
    }


    MetadataVersion metadataVersion() {
        return metadataVersion.get();
    }

    private ApiError updateFeature(String featureName,
                                   short newVersion,
                                   FeatureUpdate.UpgradeType upgradeType,
                                   Map<Integer, Map<String, VersionRange>> brokersAndFeatures,
                                   List<ApiMessageAndVersion> records) {
        if (!featureExists(featureName)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given feature.");
        }

        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given upgrade type.");
        }


        final Short currentVersion = finalizedVersions.get(featureName);

        if (newVersion <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The upper value for the new range cannot be less than 1.");
        }

        if (!canSupportVersion(featureName, newVersion)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature range.");
        }

        for (Entry<Integer, Map<String, VersionRange>> brokerEntry : brokersAndFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null || !brokerRange.contains(newVersion)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Broker " + brokerEntry.getKey() + " does not support the given " +
                        "feature range.");
            }
        }

        if (currentVersion != null && newVersion < currentVersion) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without setting the upgrade type to safe or unsafe downgrade.");
            }
        }

        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            // Perform additional checks if we're updating metadata.version
            return updateMetadataVersion(newVersion, records::add);
        } else {
            records.add(new ApiMessageAndVersion(
                new FeatureLevelRecord()
                    .setName(featureName)
                    .setFeatureLevel(newVersion),
                FEATURE_LEVEL_RECORD.highestSupportedVersion()));
            return ApiError.NONE;
        }
    }

    /**
     * Perform some additional validation for metadata.version updates.
     */
    private ApiError updateMetadataVersion(short newVersion,
                                           Consumer<ApiMessageAndVersion> recordConsumer) {
        Optional<VersionRange> quorumSupported = quorumFeatures.quorumSupportedFeature(MetadataVersion.FEATURE_NAME);
        if (!quorumSupported.isPresent()) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The quorum can not support metadata.version!");
        }

        if (newVersion <= 0) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "metadata.version cannot be less than 1");
        }

        if (!quorumSupported.get().contains(newVersion)) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller quorum cannot support the given metadata.version " + newVersion);
        }

        Short currentVersion = finalizedVersions.get(MetadataVersion.FEATURE_NAME);
        if (currentVersion != null && currentVersion > newVersion) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Downgrading metadata.version is not yet supported.");
        }

        try {
            MetadataVersion.fromValue(newVersion);
        } catch (IllegalArgumentException e) {
            return new ApiError(Errors.INVALID_UPDATE_VERSION, "Unknown metadata.version " + newVersion);
        }

        recordConsumer.accept(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(newVersion), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));
        return ApiError.NONE;
    }

    FinalizedControllerFeatures finalizedFeatures(long lastCommittedOffset) {
        Map<String, Short> features = new HashMap<>();
        for (Entry<String, Short> entry : finalizedVersions.entrySet(lastCommittedOffset)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, lastCommittedOffset);
    }

    public void replay(FeatureLevelRecord record) {
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            log.info("Setting metadata.version to {}", record.featureLevel());
            metadataVersion.set(MetadataVersion.fromValue(record.featureLevel()));
        } else {
            log.info("Setting feature {} to {}", record.name(), record.featureLevel());
            finalizedVersions.put(record.name(), record.featureLevel());
        }
        for (Entry<String, FeatureLevelListener> listenerEntry : listeners.entrySet()) {
            try {
                listenerEntry.getValue().handle(record.name(), record.featureLevel());
            } catch (Throwable t) {
                log.error("Failure calling feature listener " + listenerEntry.getKey(), t);
            }
        }
    }

    class FeatureControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final Iterator<Entry<String, Short>> iterator;
        private final MetadataVersion metadataVersion;
        private boolean wroteVersion = false;

        FeatureControlIterator(long epoch) {
            this.iterator = finalizedVersions.entrySet(epoch).iterator();
            this.metadataVersion = FeatureControlManager.this.metadataVersion.get(epoch);
            if (this.metadataVersion.equals(MetadataVersion.UNINITIALIZED)) {
                this.wroteVersion = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !wroteVersion || iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            // Write the metadata.version first
            if (!wroteVersion) {
                wroteVersion = true;
                return Collections.singletonList(new ApiMessageAndVersion(new FeatureLevelRecord()
                    .setName(MetadataVersion.FEATURE_NAME)
                    .setFeatureLevel(metadataVersion.version()), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));
            }
            // Then write the rest of the features
            if (!hasNext()) throw new NoSuchElementException();
            Entry<String, Short> entry = iterator.next();
            return Collections.singletonList(new ApiMessageAndVersion(new FeatureLevelRecord()
                .setName(entry.getKey())
                .setFeatureLevel(entry.getValue()), FEATURE_LEVEL_RECORD.highestSupportedVersion()));
        }
    }

    FeatureControlIterator iterator(long epoch) {
        return new FeatureControlIterator(epoch);
    }
}
