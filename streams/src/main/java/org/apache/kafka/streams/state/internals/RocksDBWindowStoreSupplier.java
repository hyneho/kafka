/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.StateStore;

import java.util.Map;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */

public class RocksDBWindowStoreSupplier<K, V> extends AbstractStoreSupplier<K, V> implements ForwardingStateStoreSupplier<Windowed<K>, V> {

    private final long retentionPeriod;
    private final boolean retainDuplicates;
    private final int numSegments;
    private final long windowSize;

    public RocksDBWindowStoreSupplier(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde, long windowSize, boolean logged, Map<String, String> logConfig) {
        this(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, null, windowSize, logged, logConfig);
    }

    public RocksDBWindowStoreSupplier(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde, Time time, long windowSize, boolean logged, Map<String, String> logConfig) {
        super(name, keySerde, valueSerde, time, logged, logConfig);
        this.retentionPeriod = retentionPeriod;
        this.retainDuplicates = retainDuplicates;
        this.numSegments = numSegments;
        this.windowSize = windowSize;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        RocksDBWindowStore<K, V> store = new RocksDBWindowStore<>(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, windowSize);
        return new MeteredWindowStore<>(store.enableLogging(), "rocksdb-window", time);
    }


    @Override
    public StateStore get(final CacheFlushListener<Windowed<K>, V> listener) {
        if (retainDuplicates) {
            throw new StreamsException("retainDuplicates=true is not supported when caching is enabled");
        }
        final RocksDBWindowStore<Bytes, byte[]> store = new RocksDBWindowStore<>(name, retentionPeriod, numSegments, false, Serdes.Bytes(), Serdes.ByteArray(), windowSize);
        return new CachingWindowStore<>(new MeteredWindowStore<>(store.enableLogging(), "rocksdb-window", time), keySerde, valueSerde, listener, windowSize);
    }

    public long retentionPeriod() {
        return retentionPeriod;
    }
}
