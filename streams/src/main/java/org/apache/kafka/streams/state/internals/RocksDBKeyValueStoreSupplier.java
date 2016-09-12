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
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */

public class RocksDBKeyValueStoreSupplier<K, V> extends AbstractStoreSupplier<K, V> implements ForwardingStateStoreSupplier<K, V> {

    public RocksDBKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, boolean logged, Map<String, String> logConfig) {
        this(name, keySerde, valueSerde, null, logged, logConfig);
    }

    public RocksDBKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, Time time, boolean logged, Map<String, String> logConfig) {
        super(name, keySerde, valueSerde, time, logged, logConfig);
    }

    public StateStore get() {
        KeyValueStore<K, V> store = new RocksDBStore<>(name, keySerde, valueSerde).enableLogging();
        return new MeteredKeyValueStore<>(store, "rocksdb-state", time);
    }

    @Override
    public StateStore get(final CacheFlushListener<K, V> listener) {


        final KeyValueStore<Bytes, byte[]> rocksDbStore = new RocksDBStore<>(name, Serdes.Bytes(),
                                                                           Serdes.ByteArray()).enableLogging();
        return new CachingKeyValueStore<>(new MeteredKeyValueStore<>(rocksDbStore,
                                                                                                "rocksdb-state",
                                                                                                time),
                                                                     keySerde,
                                                                     valueSerde, listener);


    }

}
