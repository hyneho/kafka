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
package org.apache.kafka.streams.kstream.internals;

import java.time.Duration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbIndexedTimeOrderedWindowBytesStoreSupplier;

@SuppressWarnings("this-escape")
public class WindowStoreMaterializer<K, V> extends MaterializedStoreFactory<K, V, WindowStore<Bytes, byte[]>> {

    private final Windows<?> windows;
    private final EmitStrategy emitStrategy;
    private final long retentionPeriod;

    public WindowStoreMaterializer(
            final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materialized,
            final Windows<?> windows,
            final EmitStrategy emitStrategy
    ) {
        super(materialized);
        this.windows = windows;
        this.emitStrategy = emitStrategy;

        retentionPeriod = retentionPeriod();

        if ((windows.size() + windows.gracePeriodMs()) > retentionPeriod) {
            throw new IllegalArgumentException("The retention period of the window store "
                    + materialized.storeName() + " must be no smaller than its window size plus the grace period."
                    + " Got size=[" + windows.size() + "],"
                    + " grace=[" + windows.gracePeriodMs() + "],"
                    + " retention=[" + retentionPeriod + "]");
        }
    }

    @Override
    public StateStore build() {
        WindowBytesStoreSupplier supplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            switch (defaultStoreType) {
                case IN_MEMORY:
                    supplier = Stores.inMemoryWindowStore(
                            materialized.storeName(),
                            Duration.ofMillis(retentionPeriod),
                            Duration.ofMillis(windows.size()),
                            false
                    );
                    break;
                case ROCKS_DB:
                    supplier = emitStrategy.type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE ?
                            RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                                    materialized.storeName(),
                                    Duration.ofMillis(retentionPeriod),
                                    Duration.ofMillis(windows.size()),
                                    false,
                                    false
                            ) :
                            Stores.persistentTimestampedWindowStore(
                                    materialized.storeName(),
                                    Duration.ofMillis(retentionPeriod),
                                    Duration.ofMillis(windows.size()),
                                    false
                            );
                    break;
                default:
                    throw new IllegalStateException("Unknown store type: " + materialized.storeType());
            }
        }

        final StoreBuilder<TimestampedWindowStore<K, V>> builder = Stores.timestampedWindowStoreBuilder(
                supplier,
                materialized.keySerde(),
                materialized.valueSerde()
        );

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }

        return builder.build();
    }

    @Override
    public long retentionPeriod() {
        return materialized.retention() != null
                ? materialized.retention().toMillis()
                : windows.size() + windows.gracePeriodMs();
    }

    @Override
    public long historyRetention() {
        throw new IllegalStateException(
                "historyRetention is not supported when not a versioned store");
    }

    @Override
    public boolean isWindowStore() {
        return true;
    }

    @Override
    public boolean isVersionedStore() {
        return false;
    }
}
