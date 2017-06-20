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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Collection;

public class StateRestorer {

    static final int NO_CHECKPOINT = -1;

    private final Long checkpoint;
    private final long offsetLimit;
    private final boolean persistent;
    private final TopicPartition partition;
    private final String storeName;
    private final RestoreCallbackAdapter callbackAdapter;
    private long restoredOffset;
    private long startingOffset;
    private boolean restoreStarted;

    StateRestorer(final TopicPartition partition,
                  final StateRestoreCallback stateRestoreCallback,
                  final Long checkpoint,
                  final long offsetLimit,
                  final boolean persistent,
                  final String storeName) {
        this.partition = partition;
        this.callbackAdapter = new RestoreCallbackAdapter(stateRestoreCallback);
        this.checkpoint = checkpoint;
        this.offsetLimit = offsetLimit;
        this.persistent = persistent;
        this.storeName = storeName;
    }

    public TopicPartition partition() {
        return partition;
    }

    long checkpoint() {
        return checkpoint == null ? NO_CHECKPOINT : checkpoint;
    }

    void maybeNotifyRestoreStarted(long startingOffset, long endingOffset) {
        if (!restoreStarted) {
            callbackAdapter.onRestoreStart(storeName, startingOffset, endingOffset);
            restoreStarted = true;
        }
    }

    void restoreDone() {
        callbackAdapter.onRestoreEnd(storeName, restoredNumRecords());
        restoreStarted = false;
    }

    void restoreBatchCompleted(long currentRestoredOffset, int numRestored) {
        callbackAdapter.onBatchRestored(storeName, currentRestoredOffset, numRestored);
    }

    void restore(final Collection<KeyValue<byte[], byte[]>> records) {
        callbackAdapter.restoreAll(records);
    }

    boolean isPersistent() {
        return persistent;
    }

    void setStateRestoreListener(StateRestoreListener stateRestoreListener) {
        this.callbackAdapter.setStateRestoreListener(stateRestoreListener);
    }

    void setRestoredOffset(final long restoredOffset) {
        this.restoredOffset = Math.min(offsetLimit, restoredOffset);
    }

    void setStartingOffset(final long startingOffset) {
        this.startingOffset = Math.min(offsetLimit, startingOffset);
    }

    long startingOffset() {
        return startingOffset;
    }

    boolean hasCompleted(final long recordOffset, final long endOffset) {
        return endOffset == 0 || recordOffset >= readTo(endOffset);
    }

    Long restoredOffset() {
        return restoredOffset;
    }

    long restoredNumRecords() {
        return restoredOffset - startingOffset;
    }

    long offsetLimit() {
        return offsetLimit;
    }

    private Long readTo(final long endOffset) {
        return endOffset < offsetLimit ? endOffset : offsetLimit;
    }
}
