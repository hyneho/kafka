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

import org.apache.kafka.streams.KeyValue;

import java.util.Objects;

/**
 * Combines a key from a {@link KeyValue} with a boolean value referencing if the key is
 * part of the left join (true) or right join (false). This class is only useful when a state
 * store needs to be shared between left and right processors, and each processor needs to
 * access the key of the other processor.
 */
public class KeyAndJoinSide<K> {
    private final K key;
    private final boolean thisJoin;

    private KeyAndJoinSide(final boolean thisJoin, final K key) {
        this.key = Objects.requireNonNull(key, "key is null");
        this.thisJoin = thisJoin;
    }

    /**
     * Create a new {@link KeyAndJoinSide} instance if the provide {@code key} is not {@code null}.
     *
     * @param thisJoin True if the key is part of the left topic (reference as thisJoin in {@code KStreamImplJoin})
     * @param key      the key
     * @param <K>      the type of the key
     * @return a new {@link KeyAndJoinSide} instance if the provide {@code key} is not {@code null}
     */
    public static <K> KeyAndJoinSide<K> make(final boolean thisJoin, final K key) {
        return new KeyAndJoinSide<>(thisJoin, key);
    }

    public boolean isThisJoin() {
        return thisJoin;
    }

    public K getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "<" + thisJoin + "," + key + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyAndJoinSide<?> that = (KeyAndJoinSide<?>) o;
        return thisJoin == that.thisJoin &&
            Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thisJoin, key);
    }
}