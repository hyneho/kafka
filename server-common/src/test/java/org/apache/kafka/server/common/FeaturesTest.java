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
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.server.common.Features.validateDefaultValueAndLatestProductionValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FeaturesTest {
    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testV0SupportedInEarliestMV(Features feature) {
        assertTrue(feature.featureVersions().length >= 1);
        assertEquals(MetadataVersion.MINIMUM_KRAFT_VERSION,
            feature.featureVersions()[0].bootstrapMetadataVersion());
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testFromFeatureLevelAllFeatures(Features feature) {
        FeatureVersion[] featureImplementations = feature.featureVersions();
        int numFeatures = featureImplementations.length;
        short latestProductionLevel = feature.latestProduction();

        for (short i = 0; i < numFeatures; i++) {
            short level = i;
            if (latestProductionLevel < i) {
                assertEquals(featureImplementations[i], feature.fromFeatureLevel(level, true));
                assertThrows(IllegalArgumentException.class, () -> feature.fromFeatureLevel(level, false));
            } else {
                assertEquals(featureImplementations[i], feature.fromFeatureLevel(level, false));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testValidateVersionAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // Ensure the minimum bootstrap metadata version is included if no metadata version dependency.
            Map<String, Short> deps = new HashMap<>();
            deps.putAll(featureImpl.dependencies());
            if (!deps.containsKey(MetadataVersion.FEATURE_NAME)) {
                deps.put(MetadataVersion.FEATURE_NAME, MetadataVersion.MINIMUM_BOOTSTRAP_VERSION.featureLevel());
            }

            // Ensure that the feature is valid given the typical metadataVersionMapping and the dependencies.
            // Note: Other metadata versions are valid, but this one should always be valid.
            Features.validateVersion(featureImpl, deps);
        }
    }

    @Test
    public void testInvalidValidateVersion() {
        // No MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Features.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.emptyMap()
            )
        );

        // Using too low of a MetadataVersion is invalid
        assertThrows(IllegalArgumentException.class,
            () -> Features.validateVersion(
                TestFeatureVersion.TEST_1,
                Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_2_8_IV0.featureLevel())
            )
        );

        // Using a version that is lower than the dependency will fail.
        assertThrows(IllegalArgumentException.class,
             () -> Features.validateVersion(
                 TestFeatureVersion.TEST_2,
                 Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())
             )
        );
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testDefaultLevelAllFeatures(Features feature) {
        for (FeatureVersion featureImpl : feature.featureVersions()) {
            // If features have the same bootstrapMetadataVersion, the highest level feature should be chosen.
            short defaultLevel = feature.defaultLevel(featureImpl.bootstrapMetadataVersion());
            if (defaultLevel != featureImpl.featureLevel()) {
                FeatureVersion otherFeature = feature.fromFeatureLevel(defaultLevel, true);
                assertEquals(featureImpl.bootstrapMetadataVersion(), otherFeature.bootstrapMetadataVersion());
                assertTrue(defaultLevel > featureImpl.featureLevel());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionIsOneOfFeatureValues(Features features) {
        assertTrue(features.hasFeatureVersion(features.latestProduction));
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionIsNotBehindLatestMetadataVersion(Features features) {
        assertTrue(features.latestProduction() >= features.defaultLevel(MetadataVersion.latestProduction()));
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testLatestProductionDependencyIsProductionReady(Features features) {
        for (Map.Entry<String, Short> dependency: features.latestProduction.dependencies().entrySet()) {
            String featureName = dependency.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                Features dependencyFeature = Features.featureFromName(featureName);
                assertTrue(dependencyFeature.isProductionReady(dependency.getValue()));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Features.class, names = {
        "UNIT_TEST_VERSION_0",
        "UNIT_TEST_VERSION_1",
        "UNIT_TEST_VERSION_2",
        "UNIT_TEST_VERSION_3",
        "UNIT_TEST_VERSION_4",
        "UNIT_TEST_VERSION_5",
        "UNIT_TEST_VERSION_6",
        "UNIT_TEST_VERSION_7"}, mode = EnumSource.Mode.EXCLUDE)
    public void testDefaultVersionDependencyIsDefaultReady(Features features) {
        for (Map.Entry<String, Short> dependency: features.defaultVersion(MetadataVersion.LATEST_PRODUCTION).dependencies().entrySet()) {
            String featureName = dependency.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                Features dependencyFeature = Features.featureFromName(featureName);
                assertTrue(dependency.getValue() <= dependencyFeature.defaultLevel(MetadataVersion.LATEST_PRODUCTION));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    public void testDefaultTestVersion(MetadataVersion metadataVersion) {
        short expectedVersion;
        if (!metadataVersion.isLessThan(MetadataVersion.latestTesting())) {
            expectedVersion = 2;
        } else if (!metadataVersion.isLessThan(MetadataVersion.IBP_3_7_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, Features.TEST_VERSION.defaultLevel(metadataVersion));
    }

    @Test
    public void testUnstableTestVersion() {
        // If the latest MetadataVersion is stable, we don't throw an error. In that case, we don't worry about unstable feature
        // versions since all feature versions are stable.
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                Features.TEST_VERSION.fromFeatureLevel(Features.TEST_VERSION.latestTesting(), false));
        }
        Features.TEST_VERSION.fromFeatureLevel(Features.TEST_VERSION.latestTesting(), true);
    }

    @Test
    public void testValidateWithNonExistentLatestProduction() {
        assertThrows(IllegalArgumentException.class, () ->
            validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_0),
            "Feature UNIT_TEST_VERSION_0 has latest production version " +
                "test.feature.version=1 which is not one of its feature versions");
    }

    @Test
    public void testValidateWithLaggingLatestProduction() {
        assertThrows(IllegalArgumentException.class, () ->
            validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_1),
            "Feature UNIT_TEST_VERSION_1 has latest production value 0 smaller than its default value 1");
    }

    @Test
    public void testValidateWithDependencyNotProductionReady() {
        assertThrows(IllegalArgumentException.class, () ->
                validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_3),
            "Latest production FeatureVersion UNIT_TEST_VERSION_3=1 has a dependency " +
                "UNIT_TEST_VERSION_2=1 that is not production ready. (UNIT_TEST_VERSION_2 latest production: 0)");
    }

    @Test
    public void testValidateWithDefaultValueDependencyAheadOfItsDefaultLevel() {
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                    validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_5),
                "Default FeatureVersion UNIT_TEST_VERSION_5=1 has a dependency " +
                    "UNIT_TEST_VERSION_4=1 that is ahead of its default value 0");
        }
    }

    @Test
    public void testValidateWithMVDependencyNotProductionReady() {
        if (MetadataVersion.latestProduction().isLessThan(MetadataVersion.latestTesting())) {
            assertThrows(IllegalArgumentException.class, () ->
                    validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_6),
                "Latest production FeatureVersion UNIT_TEST_VERSION_6=1 has a dependency " +
                    "metadata.version=25 that is not production ready. (metadata.version latest production: 22)");
        }
    }

    @Test
    public void testValidateWithMVDependencyAheadOfBootstrapMV() {
        assertThrows(IllegalArgumentException.class, () ->
                validateDefaultValueAndLatestProductionValue(Features.UNIT_TEST_VERSION_7),
            "Default FeatureVersion UNIT_TEST_VERSION_7=0 has a dependency " +
                "metadata.version=15 that is ahead of its bootstrap MV 1");
    }
}
