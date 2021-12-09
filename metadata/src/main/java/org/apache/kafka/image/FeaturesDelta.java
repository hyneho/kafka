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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveFeatureLevelRecord;
import org.apache.kafka.metadata.MetadataVersionProvider;
import org.apache.kafka.metadata.MetadataVersion;
import org.apache.kafka.metadata.MetadataVersions;
import org.apache.kafka.metadata.VersionRange;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the cluster in the metadata image.
 */
public final class FeaturesDelta {
    private final FeaturesImage image;

    private final Map<String, Optional<VersionRange>> changes = new HashMap<>();

    private final MetadataVersionProvider metadataVersionProvider;

    private VersionRange metadataVersionChange = null;

    public FeaturesDelta(FeaturesImage image, MetadataVersionProvider metadataVersionProvider) {
        this.image = image;
        this.metadataVersionProvider = metadataVersionProvider;
    }

    public Map<String, Optional<VersionRange>> changes() {
        return changes;
    }

    public Optional<VersionRange> metadataVersionChange() {
        return Optional.ofNullable(metadataVersionChange);
    }

    public void finishSnapshot() {
        for (String featureName : image.finalizedVersions().keySet()) {
            if (!changes.containsKey(featureName)) {
                changes.put(featureName, Optional.empty());
            }
        }
    }

    public void replay(FeatureLevelRecord record) {
        VersionRange versionRange = VersionRange.of(record.minFeatureLevel(), record.maxFeatureLevel());
        changes.put(record.name(), Optional.of(versionRange));
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            metadataVersionChange = versionRange;
        }
    }

    public void replay(RemoveFeatureLevelRecord record) {
        changes.put(record.name(), Optional.empty());
    }

    public FeaturesImage apply() {
        Map<String, VersionRange> newFinalizedVersions =
            new HashMap<>(image.finalizedVersions().size());
        for (Entry<String, VersionRange> entry : image.finalizedVersions().entrySet()) {
            String name = entry.getKey();
            Optional<VersionRange> change = changes.get(name);
            if (change == null) {
                newFinalizedVersions.put(name, entry.getValue());
            } else if (change.isPresent()) {
                newFinalizedVersions.put(name, change.get());
            }
        }
        for (Entry<String, Optional<VersionRange>> entry : changes.entrySet()) {
            String name = entry.getKey();
            Optional<VersionRange> change = entry.getValue();
            if (!newFinalizedVersions.containsKey(name)) {
                if (change.isPresent()) {
                    newFinalizedVersions.put(name, change.get());
                }
            }
        }
        MetadataVersion metadataVersion = MetadataVersions.fromValue(newFinalizedVersions.get(MetadataVersion.FEATURE_NAME).max());
        return new FeaturesImage(newFinalizedVersions, metadataVersion);
    }

    @Override
    public String toString() {
        return "FeaturesDelta(" +
            "changes=" + changes +
            ')';
    }
}
