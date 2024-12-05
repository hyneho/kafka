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

import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Util class to track the offsets written into the internal topic
 * per share partition key.
 * It calculates the minimum offset globally up to which the records
 * in the internal partition are redundant i.e. they have been overridden
 * by newer records.
 */
public class ShareCoordinatorOffsetsManager {

    // Map to store share partition key => current partition offset
    // being written.
    private final TimelineHashMap<SharePartitionKey, Long> offsets;

    // Minimum offset representing the smallest necessary offset (non-redundant)
    // across the internal partition.
    private long minOffset = Long.MAX_VALUE;
    private final AtomicLong redundantOffset = new AtomicLong(0);

    public ShareCoordinatorOffsetsManager(SnapshotRegistry snapshotRegistry) {
        Objects.requireNonNull(snapshotRegistry);
        offsets = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Method updates internal state with the supplied offset for the provided
     * share partition key. It then calculates the minimum offset, if possible,
     * below which all offsets are redundant. This value is then returned as
     * an optional.
     * <p>
     * The value returned is exclusive, in that all offsets below it but not including
     * it are redundant.
     *
     * @param key    - represents {@link SharePartitionKey} whose offset needs updating
     * @param offset - represents the latest partition offset for provided key
     * @return Optional of last redundant offset, exclusive.
     */
    public Optional<Long> updateState(SharePartitionKey key, long offset) {
        minOffset = Math.min(minOffset, offset);
        offsets.put(key, offset);

        Optional<Long> deleteTillOffset = findRedundantOffset();
        deleteTillOffset.ifPresent(off -> {
            minOffset = off;
            redundantOffset.set(off);
        });
        return deleteTillOffset;
    }

    private Optional<Long> findRedundantOffset() {
        long soFar = Long.MAX_VALUE;
        if (offsets.isEmpty()) {
            return Optional.empty();
        }

        for (long offset : offsets.values()) {
            // Get min offset among latest offsets
            // for all share keys in the internal partition.
            soFar = Math.min(soFar, offset);

            // minOffset represents the smallest necessary offset
            // and if soFar equals it, we cannot proceed. This can happen
            // if a share partition key hasn't had records written for a while
            // For example,
            // <p>
            // key1:1
            // key2:2 4 6
            // key3:3 5 7
            // <p>
            // We can see in above that offsets 2, 4, 3, 5 are redundant,
            // but we do not have a contiguous prefix starting at minOffset
            // and we cannot proceed.
            if (soFar == minOffset) {
                return Optional.empty();
            }
        }

        return Optional.of(soFar);
    }

    public Optional<Long> lastRedundantOffset() {
        long value = redundantOffset.get();
        if (value <= 0 || value == Long.MAX_VALUE) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    // visible for testing
    TimelineHashMap<SharePartitionKey, Long> curState() {
        return offsets;
    }
}
