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

import java.util.ListIterator;
import org.apache.kafka.streams.state.ValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;

public class VersionedRecordIterator<V> implements ValueIterator<VersionedRecord<V>> {

    protected final ListIterator<VersionedRecord<V>> iterator;

    public VersionedRecordIterator(final ListIterator<VersionedRecord<V>> iterator) {
        this.iterator = iterator;
    }


    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public VersionedRecord<V> next() {
        return iterator.next();
    }
}


