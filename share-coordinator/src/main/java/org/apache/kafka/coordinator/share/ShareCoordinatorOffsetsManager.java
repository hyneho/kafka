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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Util class to track the last redundant offset within a share group state
 * partition.
 * <p>
 * The class utilises a priority queue (min-heap) and a hashmap to store and
 * track the redundant offsets. It also maintains a long variable minOffset
 * to track the global minimum.
 * <p>
 * The map is mainly used to invalidate queue entries since JAVA priority queue
 * does not support heap.modifyKey operation. When the last redundant offset is
 * queried, the ACTIVE element at the top of the queue which is less than the
 * global minimum offset seen so far is returned, otherwise empty optional.
 * <p>
 * Callers supply the offset information via updateState and can fetch the current
 * redundant offset using a getter.
 */
public class ShareCoordinatorOffsetsManager {
    private enum State {
        ACTIVE,
        INACTIVE
    }

    static class Entry {
        private final SharePartitionKey key;
        private final long offset;
        private State state;

        private Entry(SharePartitionKey key, long offset) {
            this.key = key;
            this.offset = offset;
            this.state = State.ACTIVE;
        }

        public SharePartitionKey key() {
            return key;
        }

        public long offset() {
            return offset;
        }

        public State state() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public static Entry instance(SharePartitionKey key, long offset) {
            return new Entry(key, offset);
        }
    }

    private final Map<SharePartitionKey, Entry> entries = new HashMap<>();
    private final Queue<Entry> curState;
    private long minOffset = Long.MAX_VALUE;
    private final AtomicLong lastRedundantOffset = new AtomicLong(0);

    public ShareCoordinatorOffsetsManager() {
        curState = new PriorityQueue<>(new Comparator<Entry>() {
            @Override
            public int compare(Entry o1, Entry o2) {
                return Long.compare(o1.offset, o2.offset);
            }
        });
    }

    /**
     * Used to add new offset information to the
     * existing state. The offsets are key upon the
     * {@link SharePartitionKey} object.
     * @param key       {@link SharePartitionKey} object whose snapshot offset is being updated
     * @param offset    Long value representing the partition record offset
     * @return Optional of the last redundant offset value, if present
     */
    public Optional<Long> updateState(SharePartitionKey key, long offset) {
        minOffset = Math.min(minOffset, offset);
        if (entries.containsKey(key)) {
            entries.get(key).setState(State.INACTIVE);
        }
        Entry newEntry = Entry.instance(key, offset);
        curState.add(newEntry);
        entries.put(key, newEntry);

        purge();

        Optional<Long> deleteTillOpt = findLastRedundantOffset();
        deleteTillOpt.ifPresent(off -> minOffset = off);
        return deleteTillOpt;
    }

    private Optional<Long> findLastRedundantOffset() {
        if (curState.isEmpty()) {
            return Optional.empty();
        }

        Entry candidate = curState.peek();
        if (candidate == null) {
            return Optional.empty();
        }

        if (candidate.offset() <= 0 || candidate.offset() == minOffset) {
            return Optional.empty();
        }
        lastRedundantOffset.set(candidate.offset());
        return Optional.of(candidate.offset());
    }

    /**
     * Fetch the current last redundant offset.
     * @return  Optional of the long value representing the offset
     */
    public Optional<Long> lastRedundantOffset() {
        return Optional.of(lastRedundantOffset.get());
    }

    // test visibility
    void purge() {
        while (!curState.isEmpty() && curState.peek().state() == State.INACTIVE) {
            curState.poll();
        }
    }

    //test visibility
    Queue<Entry> curState() {
        return curState;
    }

    //test visibility
    Map<SharePartitionKey, Entry> entries() {
        return entries;
    }
}
