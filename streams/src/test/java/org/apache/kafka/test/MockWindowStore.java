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
package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.ArrayList;

public class MockWindowStore implements WindowStore {

    private final String name;
    private final boolean persistent;

    public boolean closed = true;
    public boolean flushed = false;
    public final ArrayList<Integer> keys = new ArrayList<>();


    public MockWindowStore(final String name, final boolean persistent) {
        this.name = name;
        this.persistent = persistent;
    }

    @Override
    public void put(final Object key, final Object value) {

    }

    @Override
    public void put(final Object key, final Object value, final long windowStartTimestamp) {

    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        context.register(root, stateRestoreCallback);
        closed = false;
    }

    public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
        private final Deserializer<Integer> deserializer = new IntegerDeserializer();

        @Override
        public void restore(final byte[] key,
                            final byte[] value) {
            keys.add(deserializer.deserialize("", key));
        }
    };

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean persistent() {
        return persistent;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public Object fetch(final Object key, final long time) {
        return null;
    }

    @Override
    public WindowStoreIterator fetch(final Object key, final long timeFrom, final long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator fetch(final Object from, final Object to, final long timeFrom, final long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator all() {
        return null;
    }

    @Override
    public KeyValueIterator fetchAll(final long timeFrom, final long timeTo) {
        return null;
    }
}
