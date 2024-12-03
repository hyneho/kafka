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
