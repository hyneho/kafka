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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SharePartitionOffsetManagerTest {

    private SharePartitionOffsetManager manager;
    private static final SharePartitionKey KEY1 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 0);
    private static final SharePartitionKey KEY2 = SharePartitionKey.getInstance("gs2", Uuid.randomUuid(), 0);
    private static final SharePartitionKey KEY3 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 1);
    private static final SharePartitionKey KEY4 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 7);

    @BeforeEach
    public void setUp() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext(SharePartitionOffsetManagerTest.class.getName()));
        manager = new SharePartitionOffsetManager(snapshotRegistry);
    }


    @Test
    public void testNullRegistryArgInvocation() {
        assertThrows(NullPointerException.class, () -> new SharePartitionOffsetManager(null));
    }

    @Test
    public void testUpdateStateAddsToInternalState() {
        SharePartitionKey key1 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 0);
        SharePartitionKey key2 = SharePartitionKey.getInstance("gs1", Uuid.randomUuid(), 1);

        assertEquals(Optional.empty(), manager.updateState(key1, 0L));
        assertEquals(Optional.of(10L), manager.updateState(key1, 10L));
        assertEquals(Optional.empty(), manager.updateState(key2, 15L));

        assertEquals(10L, manager.offsets().get(key1));
        assertEquals(15L, manager.offsets().get(key2));
    }

    private static class ShareOffsetTestHolder {
        static class TestTuple {
            final SharePartitionKey key;
            final long offset;
            final Optional<Long> expectedOffset;

            private TestTuple(SharePartitionKey key, long offset, Optional<Long> expectedOffset) {
                this.key = key;
                this.offset = offset;
                this.expectedOffset = expectedOffset;
            }

            static TestTuple instance(SharePartitionKey key, long offset, Optional<Long> expectedOffset) {
                return new TestTuple(key, offset, expectedOffset);
            }
        }

        private final String testName;
        private final List<TestTuple> tuples;
        private final boolean shouldRun;

        ShareOffsetTestHolder(String testName, List<TestTuple> tuples) {
            this(testName, tuples, true);
        }

        ShareOffsetTestHolder(String testName, List<TestTuple> tuples, boolean shouldRun) {
            this.testName = testName;
            this.tuples = tuples;
            this.shouldRun = shouldRun;
        }
    }

    static Stream<ShareOffsetTestHolder> generateNoRedundantStateCases() {
        return Stream.of(
                new ShareOffsetTestHolder(
                        "no redundant state single key",
                        Collections.singletonList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty())
                        )
                ),

                new ShareOffsetTestHolder(
                        "no redundant state multiple keys",
                        Arrays.asList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY4, 11L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 13L, Optional.empty())
                        )
                )
        );
    }

    static Stream<ShareOffsetTestHolder> generateRedundantStateCases() {
        return Stream.of(
                new ShareOffsetTestHolder(
                        "redundant state single key",
                        Arrays.asList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 11L, Optional.of(11L)),
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 15L, Optional.of(15L))
                        )
                ),

                new ShareOffsetTestHolder(
                        "redundant state multiple keys",
                        // KEY1: 10 17
                        // KEY2: 11 16
                        // KEY2: 15
                        Arrays.asList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 16L, Optional.empty()),  // KEY2 11 redundant but should not be returned
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 17L, Optional.of(15L))
                        )
                )
        );

    }

    static Stream<ShareOffsetTestHolder> generateComplexCases() {
        return Stream.of(
                new ShareOffsetTestHolder(
                        "redundant state reverse key order",
                        // requests come in order KEY1, KEY2, KEY3, KEY3, KEY2, KEY1
                        Arrays.asList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 18L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 20L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 25L, Optional.of(18L))
                        )
                ),

                new ShareOffsetTestHolder(
                        "redundant state cold partition",
                        Arrays.asList(
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 10L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 11L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 15L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 18L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 20L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 22L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 25L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY2, 27L, Optional.empty()),  // last redundant
                                ShareOffsetTestHolder.TestTuple.instance(KEY3, 28L, Optional.empty()),
                                ShareOffsetTestHolder.TestTuple.instance(KEY1, 30L, Optional.of(27L))
                        )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("generateNoRedundantStateCases")
    public void testUpdateStateNoRedundantState(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> assertEquals(tuple.expectedOffset, manager.updateState(tuple.key, tuple.offset), holder.testName));
        }
    }

    @ParameterizedTest
    @MethodSource("generateRedundantStateCases")
    public void testUpdateStateRedundantState(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> assertEquals(tuple.expectedOffset, manager.updateState(tuple.key, tuple.offset), holder.testName));
        }
    }

    @ParameterizedTest
    @MethodSource("generateComplexCases")
    public void testUpdateStateComplexCases(ShareOffsetTestHolder holder) {
        if (holder.shouldRun) {
            holder.tuples.forEach(tuple -> assertEquals(tuple.expectedOffset, manager.updateState(tuple.key, tuple.offset), holder.testName));
        }
    }
}
