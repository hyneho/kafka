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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.ForwardingStateStoreSupplier;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;

/**
 * A KTable storage. It stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class KTableStoreSupplier<K, V> implements ForwardingStateStoreSupplier<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;

    protected KTableStoreSupplier(String name,
                                  Serde<K> keySerde,
                                  Serde<V> valueSerde,
                                  Time time) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    public String name() {
        return name;
    }

    public StateStore get() {
        return new MeteredKeyValueStore<>(new RocksDBStore<>(name, Serdes.Bytes(), Serdes.ByteArray()),
                "rocksdb-state", time);
    }

    @Override
    public StateStore get(final CacheFlushListener<K, V> listener) {
        final CachingKeyValueStore<K, V> store = new CachingKeyValueStore<>(new RocksDBStore<>(name, Serdes.Bytes(), Serdes.ByteArray()),
                                                                            keySerde, valueSerde, listener);
        return new MeteredKeyValueStore<>(store, "rocksdb-state", time);
    }

    @Override
    public boolean cachingEnabled() {
        return true;
    }
}
