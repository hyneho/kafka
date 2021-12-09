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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FeatureMap;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.MetadataVersions;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class FeatureControlManagerTest {
    @SuppressWarnings("unchecked")
    private static Map<String, VersionRange> rangeMap(Object... args) {
        Map<String, VersionRange> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 3) {
            String feature = (String) args[i];
            Integer low = (Integer) args[i + 1];
            Integer high = (Integer) args[i + 2];
            result.put(feature, VersionRange.of(low.shortValue(), high.shortValue()));
        }
        return result;
    }

    public static QuorumFeatures features(Object... args) {
        return new QuorumFeatures(0, new ApiVersions(), rangeMap(args), Collections.emptyList());
    }

    private static Map<String, Short> updateMap(Object... args) {
        Map<String, Short> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 3) {
            String feature = (String) args[i];
            Integer low = (Integer) args[i + 1];
            Integer high = (Integer) args[i + 2];
            result.put(feature, high.shortValue());
        }
        return result;
    }

    @Test
    public void testUpdateFeatures() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        snapshotRegistry.getOrCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager(
            features("foo", 1, 2), snapshotRegistry, MetadataVersions::latest);
        assertEquals(new FeatureMapAndEpoch(new FeatureMap(Collections.emptyMap()), -1),
            manager.finalizedFeatures(-1));
        assertEquals(ControllerResult.atomicOf(Collections.emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "The controller does not support the given feature range."))),
            manager.updateFeatures(updateMap("foo", 1, 3),
                Collections.singleton("foo"),
                Collections.emptyMap(), false));
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
                updateMap("foo", 1, 2, "bar", 1, 1), Collections.emptySet(),
                Collections.emptyMap(), false);
        Map<String, ApiError> expectedMap = new HashMap<>();
        expectedMap.put("foo", ApiError.NONE);
        expectedMap.put("bar", new ApiError(Errors.INVALID_UPDATE_VERSION,
                "The controller does not support the given feature."));
        assertEquals(expectedMap, result.response());
        List<ApiMessageAndVersion> expectedMessages = new ArrayList<>();
        expectedMessages.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName("foo").setMinFeatureLevel((short) 1).setMaxFeatureLevel((short) 2),
            (short) 0));
        assertEquals(expectedMessages, result.records());
    }

    @Test
    public void testReplay() {
        FeatureLevelRecord record = new FeatureLevelRecord().
            setName("foo").setMinFeatureLevel((short) 1).setMaxFeatureLevel((short) 2);
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        snapshotRegistry.getOrCreateSnapshot(-1);
        FeatureControlManager manager = new FeatureControlManager(
            features("foo", 1, 2), snapshotRegistry, MetadataVersions::latest);
        manager.replay(record);
        snapshotRegistry.getOrCreateSnapshot(123);
        assertEquals(new FeatureMapAndEpoch(new FeatureMap(rangeMap("foo", 1, 2)), 123),
            manager.finalizedFeatures(123));
    }

    @Test
    public void testUpdateFeaturesErrorCases() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager manager = new FeatureControlManager(
            features("foo", 1, 5, "bar", 1, 2), snapshotRegistry, MetadataVersions::latest);

        assertEquals(
            ControllerResult.atomicOf(
                Collections.emptyList(),
                Collections.singletonMap(
                    "foo",
                    new ApiError(
                        Errors.INVALID_UPDATE_VERSION,
                        "Broker 5 does not support the given feature range."
                    )
                )
            ),
            manager.updateFeatures(
                updateMap("foo", 1, 3),
                Collections.singleton("foo"),
                Collections.singletonMap(5, rangeMap()),
                    false)
        );

        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
            updateMap("foo", 1, 3), Collections.emptySet(), Collections.emptyMap(), false);
        assertEquals(Collections.singletonMap("foo", ApiError.NONE), result.response());
        manager.replay((FeatureLevelRecord) result.records().get(0).message());
        snapshotRegistry.getOrCreateSnapshot(3);

        assertEquals(ControllerResult.atomicOf(Collections.emptyList(), Collections.
                singletonMap("foo", new ApiError(Errors.INVALID_UPDATE_VERSION,
                    "Can't downgrade the maximum version of this feature without " +
                    "setting downgradable to true."))),
            manager.updateFeatures(updateMap("foo", 1, 2),
                Collections.emptySet(), Collections.emptyMap(), false));

        assertEquals(
            ControllerResult.atomicOf(
                Collections.singletonList(
                    new ApiMessageAndVersion(
                        new FeatureLevelRecord()
                            .setName("foo")
                            .setMinFeatureLevel((short) 1)
                            .setMaxFeatureLevel((short) 2),
                        (short) 0
                    )
                ),
                Collections.singletonMap("foo", ApiError.NONE)
            ),
            manager.updateFeatures(
                updateMap("foo", 1, 2),
                Collections.singleton("foo"),
                Collections.emptyMap(),
                    false)
        );
    }

    @Test
    public void testFeatureControlIterator() throws Exception {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager manager = new FeatureControlManager(
            features("foo", 1, 5, "bar", 1, 2), snapshotRegistry, MetadataVersions::latest);
        ControllerResult<Map<String, ApiError>> result = manager.
            updateFeatures(updateMap("foo", 1, 5, "bar", 1, 1),
                Collections.emptySet(), Collections.emptyMap(), false);
        RecordTestUtils.replayAll(manager, result.records());
        RecordTestUtils.assertBatchIteratorContains(Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName("foo").
                setMinFeatureLevel((short) 1).
                setMaxFeatureLevel((short) 5), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName("bar").
                setMinFeatureLevel((short) 1).
                setMaxFeatureLevel((short) 1), (short) 0))),
            manager.iterator(Long.MAX_VALUE));
    }
}
