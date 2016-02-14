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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Serdes;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class RocksDBStore<K, V> implements KeyValueStore<K, V> {

    private static final int TTL_NOT_USED = -1;

    // TODO: these values should be configurable
    private static final int DEFAULT_UNENCODED_CACHE_SIZE = 1000;
    private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
    private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    private static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
    private static final long BLOCK_CACHE_SIZE = 100 * 1024 * 1024L;
    private static final long BLOCK_SIZE = 4096L;
    private static final int TTL_SECONDS = TTL_NOT_USED;
    private static final int MAX_WRITE_BUFFERS = 3;
    private static final String DB_FILE_DIR = "rocksdb";

    private final String name;
    private final String parentDir;

    private final Options options;
    private final WriteOptions wOptions;
    private final FlushOptions fOptions;

    private ProcessorContext context;
    private Serdes<K, V> serdes;
    protected File dbDir;
    private RocksDB db;

    private boolean loggingEnabled = false;
    private int cacheSize = DEFAULT_UNENCODED_CACHE_SIZE;

    private Set<K> cacheDirtyKeys;
    private MemoryLRUCache<K, RocksDBCacheEntry> cache;
    private StoreChangeLogger<byte[], byte[]> changeLogger;
    private StoreChangeLogger.ValueGetter<byte[], byte[]> getter;

    public KeyValueStore<K, V> enableLogging() {
        loggingEnabled = true;

        return this;
    }

    public RocksDBStore<K, V> withCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;

        return this;
    }

    public RocksDBStore(String name, Serdes<K, V> serdes) {
        this(name, DB_FILE_DIR, serdes);
    }

    public RocksDBStore(String name, String parentDir, Serdes<K, V> serdes) {
        this.name = name;
        this.parentDir = parentDir;
        this.serdes = serdes;

        // initialize the rocksdb options
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
        tableConfig.setBlockSize(BLOCK_SIZE);

        options = new Options();
        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(WRITE_BUFFER_SIZE);
        options.setCompressionType(COMPRESSION_TYPE);
        options.setCompactionStyle(COMPACTION_STYLE);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        fOptions = new FlushOptions();
        fOptions.setWaitForFlush(true);
    }

    private class RocksDBCacheEntry {
        public V value;
        public boolean isDirty;

        public RocksDBCacheEntry(V value) {
            this(value, false);
        }

        public RocksDBCacheEntry(V value, boolean isDirty) {
            this.value = value;
            this.isDirty = isDirty;
        }
    }

    public void openDB(ProcessorContext context) {
        this.context = context;
        this.dbDir = new File(new File(this.context.stateDir(), parentDir), this.name);
        this.db = openDB(this.dbDir, this.options, TTL_SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // first open the DB dir
        openDB(context);

        this.changeLogger = this.loggingEnabled ? new RawStoreChangeLogger(name, context) : null;

        if (this.cacheSize > 0) {
            this.cache = new MemoryLRUCache<K, RocksDBCacheEntry>(name, cacheSize)
                    .whenEldestRemoved(new MemoryLRUCache.EldestEntryRemovalListener<K, RocksDBCacheEntry>() {
                        @Override
                        public void apply(K key, RocksDBCacheEntry entry) {
                            // flush all the dirty entries to RocksDB if this evicted entry is dirty
                            if (entry.isDirty) {
                                flush();
                            }
                        }
                    });


            this.cacheDirtyKeys = new HashSet<>();
        } else {
            this.cache = null;
            this.cacheDirtyKeys = null;
        }

        // value getter should always read directly from rocksDB
        // since it is only for values that are already flushed
        this.getter = new StoreChangeLogger.ValueGetter<byte[], byte[]>() {
            @Override
            public byte[] get(byte[] key) {
                return getInternal(key);
            }
        };

        context.register(root, loggingEnabled, new StateRestoreCallback() {

            @Override
            public void restore(byte[] key, byte[] value) {
                putInternal(key, value);
            }
        });
    }

    private RocksDB openDB(File dir, Options options, int ttl) {
        try {
            if (ttl == TTL_NOT_USED) {
                dir.getParentFile().mkdirs();
                return RocksDB.open(options, dir.toString());
            } else {
                throw new UnsupportedOperationException("Change log is not supported for store " + this.name + " since it is TTL based.");
                // TODO: support TTL with change log?
                // return TtlDB.open(options, dir.toString(), ttl, false);
            }
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + this.name + " at location " + dir.toString(), e);
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public V get(K key) {
        if (cache != null) {
            RocksDBCacheEntry entry = cache.get(key);

            if (entry == null) {
                byte[] rawKey = serdes.rawKey(key);
                V value = serdes.valueFrom(getInternal(serdes.rawKey(key)));
                cache.put(key, new RocksDBCacheEntry(value));

                return value;
            } else {
                return entry.value;
            }
        } else {
            return serdes.valueFrom(getInternal(serdes.rawKey(key)));
        }
    }

    private byte[] getInternal(byte[] rawKey) {
        try {
            return this.db.get(rawKey);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while getting value for key " + serdes.keyFrom(rawKey) +
                    " from store " + this.name, e);
        }
    }

    @Override
    public void put(K key, V value) {
        if (cache != null) {
            cache.put(key, new RocksDBCacheEntry(value, true));
            cacheDirtyKeys.add(key);
        } else {
            byte[] rawKey = serdes.rawKey(key);
            byte[] rawValue = serdes.rawValue(value);
            putInternal(rawKey, rawValue);

            if (loggingEnabled) {
                changeLogger.add(rawKey);
                changeLogger.maybeLogChange(this.getter);
            }
        }
    }

    @Override
    public void putIfAbsent(K key, V value) {
        if (get(key) == null) {
            put(key, value);
        }
    }

    private void putInternal(byte[] rawKey, byte[] rawValue) {
        if (rawValue == null) {
            try {
                db.remove(wOptions, rawKey);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while removing key " + serdes.keyFrom(rawKey) +
                        " from store " + this.name, e);
            }
        } else {
            try {
                db.put(wOptions, rawKey, rawValue);
            } catch (RocksDBException e) {
                throw new ProcessorStateException("Error while executing put key " + serdes.keyFrom(rawKey) +
                        " and value " + serdes.keyFrom(rawValue) + " from store " + this.name, e);
            }
        }
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    // this function is only called in flush()
    private void putAllInternal(List<KeyValue<byte[], byte[]>> entries) {
        WriteBatch batch = new WriteBatch();

        for (KeyValue<byte[], byte[]> entry : entries) {
            batch.put(entry.key, entry.value);
        }

        try {
            db.write(wOptions, batch);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while batch writing to store " + this.name, e);
        }
    }

    @Override
    public V delete(K key) {
        V value = get(key);
        put(key, null);
        return value;
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        // we need to flush the cache if necessary before returning the iterator
        if (cache != null)
            flush();

        return new RocksDBRangeIterator<K, V>(db.newIterator(), serdes, from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        // we need to flush the cache if necessary before returning the iterator
        if (cache != null)
            flush();

        RocksIterator innerIter = db.newIterator();
        innerIter.seekToFirst();
        return new RocksDbIterator<K, V>(innerIter, serdes);
    }

    @Override
    public void flush() {
        // flush of the cache entries if necessary
        if (cache != null) {
            List<KeyValue<byte[], byte[]>> putBatch = new ArrayList<>(cache.keys.size());
            List<byte[]> deleteBatch = new ArrayList<>(cache.keys.size());

            for (K key : cacheDirtyKeys) {
                RocksDBCacheEntry entry = cache.get(key);

                assert entry.isDirty;

                byte[] rawKey = serdes.rawKey(key);

                if (entry.value != null) {
                    putBatch.add(new KeyValue<>(rawKey, serdes.rawValue(entry.value)));
                } else {
                    deleteBatch.add(rawKey);
                }
            }

            putAllInternal(putBatch);

            if (loggingEnabled) {
                for (KeyValue<byte[], byte[]> kv : putBatch)
                    changeLogger.add(kv.key);
            }

            // check all removed entries and remove them in rocksDB
            // TODO: can this be done in batch as well?
            for (byte[] removedKey : deleteBatch) {
                try {
                    db.remove(wOptions, removedKey);
                } catch (RocksDBException e) {
                    throw new ProcessorStateException("Error while deleting with key " + serdes.keyFrom(removedKey) + " from store " + this.name, e);
                }

                if (loggingEnabled) {
                    changeLogger.delete(removedKey);
                }
            }

            // reset dirty set
            cacheDirtyKeys.clear();
        }

        flushInternal();

        if (loggingEnabled)
            changeLogger.logChange(getter);
    }

    public void flushInternal() {
        try {
            db.flush(fOptions);
        } catch (RocksDBException e) {
            throw new ProcessorStateException("Error while executing flush from store " + this.name, e);
        }
    }

    @Override
    public void close() {
        flush();
        db.close();
    }

    private static class RocksDbIterator<K, V> implements KeyValueIterator<K, V> {
        private final RocksIterator iter;
        private final Serdes<K, V> serdes;

        public RocksDbIterator(RocksIterator iter, Serdes<K, V> serdes) {
            this.iter = iter;
            this.serdes = serdes;
        }

        protected byte[] peekRawKey() {
            return iter.key();
        }

        protected KeyValue<K, V> getKeyValue() {
            return new KeyValue<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
        }

        @Override
        public boolean hasNext() {
            return iter.isValid();
        }

        @Override
        public KeyValue<K, V> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            KeyValue<K, V> entry = this.getKeyValue();
            iter.next();
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("RocksDB iterator does not support remove");
        }

        @Override
        public void close() {
            iter.dispose();
        }

    }

    private static class LexicographicComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] left, byte[] right) {
            for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                int leftByte = left[i] & 0xff;
                int rightByte = right[j] & 0xff;
                if (leftByte != rightByte) {
                    return leftByte - rightByte;
                }
            }
            return left.length - right.length;
        }
    }

    private static class RocksDBRangeIterator<K, V> extends RocksDbIterator<K, V> {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = new LexicographicComparator();
        byte[] rawToKey;

        public RocksDBRangeIterator(RocksIterator iter, Serdes<K, V> serdes,
                                    K from, K to) {
            super(iter, serdes);
            iter.seek(serdes.rawKey(from));
            this.rawToKey = serdes.rawKey(to);
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && comparator.compare(super.peekRawKey(), this.rawToKey) <= 0;
        }
    }

}
