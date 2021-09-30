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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    private final String name;
    private final boolean copyOnRange;
    private final NavigableMap<Bytes, byte[]> map;

    private volatile boolean open = false;
    private long size = 0L; // SkipListMap#size is O(N) so we just do our best to track it

    // for tests-only
    public InMemoryKeyValueStore(final String name) {
        this(name, true);
    }

    public InMemoryKeyValueStore(final String name, final boolean copyOnRange) {
        this.name = name;
        this.copyOnRange = copyOnRange;
        // if we do not need to copy on range, then we should use concurrent skiplist map
        // to avoid concurrent modificiation exception
        this.map = copyOnRange ? new TreeMap<>() : new ConcurrentSkipListMap<>();
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        size = 0;
        if (root != null) {
            // register the store
            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
        }

        open = true;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        return map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        if (value == null) {
            size -= map.remove(key) == null ? 0 : 1;
        } else {
            size += map.put(key, value) == null ? 1 : 0;
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {

        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);
        final NavigableSet<Bytes> subSet = map.subMap(from, true, to, false).navigableKeySet();
        final InMemoryKeyValueIterator iter = new InMemoryKeyValueIterator(subSet, true);

        return new DelegatingPeekingKeyValueIterator<>(name, iter);
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        final byte[] oldValue = map.remove(key);
        size -= oldValue == null ? 0 : 1;
        return oldValue;
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward) {
        if (from == null && to == null) {
            return getKeyValueIterator(map, forward);
        } else if (from == null) {
            return getKeyValueIterator(map.headMap(to, true), forward);
        } else if (to == null) {
            return getKeyValueIterator(map.tailMap(from, true), forward);
        } else if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        } else {
            return getKeyValueIterator(map.subMap(from, true, to, true), forward);
        }
    }

    private KeyValueIterator<Bytes, byte[]> getKeyValueIterator(final NavigableMap<Bytes, byte[]> rangeMap, final boolean forward) {
        return new DelegatingPeekingKeyValueIterator<>(name, new InMemoryKeyValueIterator(rangeMap.navigableKeySet(), forward));
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        return range(null, null);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new DelegatingPeekingKeyValueIterator<>(name, new InMemoryKeyValueIterator(map.navigableKeySet(), false));
    }

    @Override
    public long approximateNumEntries() {
        return size;
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        map.clear();
        size = 0;
        open = false;
    }

    private class InMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> iter;

        private InMemoryKeyValueIterator(final NavigableSet<Bytes> set, final boolean forward) {
            final NavigableSet<Bytes> orderedSet = copyOnRange ? new TreeSet<>(set) : set;
            this.iter = forward ? orderedSet.iterator() : orderedSet.descendingIterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final Bytes key = iter.next();
            return new KeyValue<>(key, map.get(key));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}
