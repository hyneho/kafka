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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDBSegmentedBytesStore;
import org.apache.kafka.streams.state.internals.RocksDBSessionStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;
import org.apache.kafka.streams.state.internals.RocksDBTimestampedSegmentedBytesStore;
import org.apache.kafka.streams.state.internals.RocksDBTimestampedStore;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.junit.Test;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class StoresTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfPersistentKeyValueStoreStoreNameIsNull() {
        Stores.persistentKeyValueStore(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfPersistentTimestampedKeyValueStoreStoreNameIsNull() {
        Stores.persistentTimestampedKeyValueStore(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIMemoryKeyValueStoreStoreNameIsNull() {
        //noinspection ResultOfMethodCallIgnored
        Stores.inMemoryKeyValueStore(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfILruMapStoreNameIsNull() {
        Stores.lruMap(null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfILruMapStoreCapacityIsNegative() {
        Stores.lruMap("anyName", -1);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIPersistentWindowStoreStoreNameIsNull() {
        Stores.persistentWindowStore(null, ZERO, ZERO, false);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIPersistentTimestampedWindowStoreStoreNameIsNull() {
        Stores.persistentTimestampedWindowStore(null, ZERO, ZERO, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreRetentionPeriodIsNegative() {
        Stores.persistentWindowStore("anyName", ofMillis(-1L), ZERO, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentTimestampedWindowStoreRetentionPeriodIsNegative() {
        Stores.persistentTimestampedWindowStore("anyName", ofMillis(-1L), ZERO, false);
    }

    @Deprecated
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreIfNumberOfSegmentsSmallerThanOne() {
        Stores.persistentWindowStore("anyName", 0L, 1, 0L, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentWindowStoreIfWindowSizeIsNegative() {
        Stores.persistentWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentTimestampedWindowStoreIfWindowSizeIsNegative() {
        Stores.persistentTimestampedWindowStore("anyName", ofMillis(0L), ofMillis(-1L), false);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfIPersistentSessionStoreStoreNameIsNull() {
        Stores.persistentSessionStore(null, ofMillis(0));

    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfIPersistentSessionStoreRetentionPeriodIsNegative() {
        Stores.persistentSessionStore("anyName", ofMillis(-1));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForWindowStoreBuilder() {
        Stores.windowStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForKeyValueStoreBuilder() {
        Stores.keyValueStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfSupplierIsNullForSessionStoreBuilder() {
        Stores.sessionStoreBuilder(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test
    public void shouldCreateInMemoryKeyValueStore() {
        assertThat(Stores.inMemoryKeyValueStore("memory").get(), instanceOf(InMemoryKeyValueStore.class));
    }

    @Test
    public void shouldCreateMemoryNavigableCache() {
        assertThat(Stores.lruMap("map", 10).get(), instanceOf(MemoryNavigableLRUCache.class));
    }

    @Test
    public void shouldCreateRocksDbStore() {
        assertThat(
            Stores.persistentKeyValueStore("store").get(),
            allOf(not(instanceOf(RocksDBTimestampedStore.class)), instanceOf(RocksDBStore.class)));
    }

    @Test
    public void shouldCreateRocksDbTimestampedStore() {
        assertThat(Stores.persistentTimestampedKeyValueStore("store").get(), instanceOf(RocksDBTimestampedStore.class));
    }

    @Test
    public void shouldCreateRocksDbWindowStore() {
        final WindowStore store = Stores.persistentWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBWindowStore.class));
        assertThat(wrapped, allOf(not(instanceOf(RocksDBTimestampedSegmentedBytesStore.class)), instanceOf(RocksDBSegmentedBytesStore.class)));
    }

    @Test
    public void shouldCreateRocksDbTimestampedWindowStore() {
        final WindowStore store = Stores.persistentTimestampedWindowStore("store", ofMillis(1L), ofMillis(1L), false).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBWindowStore.class));
        assertThat(wrapped, instanceOf(RocksDBTimestampedSegmentedBytesStore.class));
    }

    @Test
    public void shouldCreateRocksDbSessionStore() {
        assertThat(Stores.persistentSessionStore("store", ofMillis(1)).get(), instanceOf(RocksDBSessionStore.class));
    }

    @Test
    public void shouldBuildKeyValueStore() {
        final KeyValueStore<String, String> store = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedKeyValueStoreThatWrapsKeyValueStore() {
        final TimestampedKeyValueStore<String, String> store = Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentKeyValueStore("name"),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildWindowStore() {
        final WindowStore<String, String> store = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildTimestampedWindowStore() {
        final TimestampedWindowStore<String, String> store = Stores.timestampedWindowStoreBuilder(
            Stores.persistentTimestampedWindowStore("store", ofMillis(3L), ofMillis(3L), true),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }

    @Test
    public void shouldBuildSessionStore() {
        final SessionStore<String, String> store = Stores.sessionStoreBuilder(
            Stores.persistentSessionStore("name", ofMillis(10)),
            Serdes.String(),
            Serdes.String()
        ).build();
        assertThat(store, not(nullValue()));
    }
}
