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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeaturesTest {

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testFromFeatureLevelAllFeatures(Features feature) {
        FeatureVersion[] featureImplementations = feature.featureVersions();
        int numFeatures = featureImplementations.length;
        for (short i = 1; i < numFeatures; i++) {
            assertEquals(featureImplementations[i - 1], feature.fromFeatureLevel(i));
        }
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testValidateVersionAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // Ensure that the feature is valid given the typical metadataVersionMapping and the dependencies.
            // Note: Other metadata versions are valid, but this one should always be valid.
            Features.validateVersion(featureImpl, featureImpl.bootstrapMetadataVersion(), featureImpl.dependencies());
        }
    }

    @Test
    public void testInvalidValidateVersion() {
        // Using too low of a MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Features.validateVersion(
                TestFeatureVersion.TEST_1,
                MetadataVersion.IBP_2_8_IV0,
                Collections.emptyMap()
            )
        );

        // Using a version that is lower than the dependency will fail.
        assertThrows(IllegalArgumentException.class,
             () -> Features.validateVersion(
                 TestFeatureVersion.TEST_2,
                 MetadataVersion.MINIMUM_BOOTSTRAP_VERSION,
                 Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())
             )
        );
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testDefaultValueAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            assertEquals(feature.defaultValue(Optional.of(featureImpl.bootstrapMetadataVersion())), featureImpl.featureLevel(),
                    "Failed to get the correct default for " + featureImpl);
        }
    }

    @ParameterizedTest
    @EnumSource(Features.class)
    public void testDefaultNoMetadataVersionSupplied(Features features) {
        if (features.latestProductionVersion() != null) {
            assertEquals(features.latestProductionVersion().featureLevel(), features.defaultValue(Optional.empty()));
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testDefaultTestVersion(MetadataVersion metadataVersion) {
        short expectedVersion;
        if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_8_IV0)) {
            expectedVersion = 2;
        } else if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_7_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, Features.TEST_VERSION.defaultValue(Optional.of(metadataVersion)));
    }

    @Test
    public void testTestVersionLatestProduction() {
        assertEquals(Features.TEST_VERSION.latestProductionVersion().featureLevel(), Features.TEST_VERSION.defaultValue(Optional.empty()));
    }
}
