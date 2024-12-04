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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public enum TestFeatureVersion implements FeatureVersion {
    TEST_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    // TEST_1 released right before MV 3.7-IVO was released, and it has no dependencies
    TEST_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap()),
    // TEST_2 is not yet set to be the default version and maps to the latest testing version, and it depends on this metadata version
    TEST_2(2, MetadataVersion.latestTesting(), Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.latestTesting().featureLevel())),

    /**
     * Test versions only used for unit test FeatureTest.java.
     */
    // For testing latest production is not one of the feature versions.
    UT_FV0_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV0_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap()),

    // For testing latest production lags behind the default value.
    UT_FV1_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV1_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap()),

    // For testing the dependency of the latest production that is not yet production ready.
    UT_FV2_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV2_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap()),
    UT_FV3_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV3_1(1, MetadataVersion.IBP_3_7_IV0, Collections.singletonMap("unit.test.feature.version.2", (short) 1)),

    // For testing the dependency of the default value that is not yet default ready.
    UT_FV4_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV4_1(1, MetadataVersion.latestTesting(), Collections.emptyMap()),
    UT_FV5_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV5_1(1, MetadataVersion.IBP_3_7_IV0, Collections.singletonMap("unit.test.feature.version.4", (short) 1)),

    // For testing the latest production has MV dependency that is not yet production ready.
    UT_FV6_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
    UT_FV6_1(1, MetadataVersion.latestTesting(), Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.latestTesting().featureLevel())),

    // For testing the default value has MV dependency that is behind the bootstrap MV.
    UT_FV7_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())),
    UT_FV7_1(1, MetadataVersion.IBP_3_8_IV0, Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_8_IV0.featureLevel()));

    private final short featureLevel;
    private final MetadataVersion metadataVersionMapping;
    private final Map<String, Short> dependencies;

    public static final String FEATURE_NAME = "test.feature.version";

    public static final TestFeatureVersion LATEST_PRODUCTION =
        MetadataVersion.latestProduction() == MetadataVersion.latestTesting() ? TEST_2 : TEST_1;

    TestFeatureVersion(int featureLevel, MetadataVersion metadataVersionMapping, Map<String, Short> dependencies) {
        this.featureLevel = (short) featureLevel;
        this.metadataVersionMapping = metadataVersionMapping;
        this.dependencies = dependencies;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public String featureName() {
        return FEATURE_NAME;
    }

    public MetadataVersion bootstrapMetadataVersion() {
        return metadataVersionMapping;
    }

    public Map<String, Short> dependencies() {
        return dependencies;
    }

    public static TestFeatureVersion[] valuesWithoutUnitTestVersions() {
        return Arrays.stream(values()).filter(v -> !v.name().startsWith("UT_FV")).toArray(TestFeatureVersion[]::new);
    }
}
