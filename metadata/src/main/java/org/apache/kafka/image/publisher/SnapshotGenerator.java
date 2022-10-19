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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.PreVersionChangeManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.fault.FaultHandler;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;


/**
 * A metadata publisher that generates snapshots when appropriate.
 */
public class SnapshotGenerator implements MetadataPublisher {
    public static class Builder {
        private int nodeId = 0;
        private Time time = Time.SYSTEM;
        private Emitter emitter = null;
        private FaultHandler faultHandler = (m, e) -> null;
        private long minBytesSinceLastSnapshot = 100 * 1024L * 1024L;
        private long minTimeSinceLastSnapshotNs = TimeUnit.DAYS.toNanos(1);

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setEmitter(Emitter emitter) {
            this.emitter = emitter;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public Builder setMinBytesSinceLastSnapshot(long minBytesSinceLastSnapshot) {
            this.minBytesSinceLastSnapshot = minBytesSinceLastSnapshot;
            return this;
        }

        public Builder setMinTimeSinceLastSnapshotNs(long minTimeSinceLastSnapshotNs) {
            this.minTimeSinceLastSnapshotNs = minTimeSinceLastSnapshotNs;
            return this;
        }

        public SnapshotGenerator build() {
            if (emitter == null) throw new RuntimeException("You must set a snapshot emitter.");
            return new SnapshotGenerator(
                nodeId,
                time,
                emitter,
                faultHandler,
                minBytesSinceLastSnapshot,
                minTimeSinceLastSnapshotNs
            );
        }
    }

    /**
     * The callback which actually generates the snapshot.
     */
    public interface Emitter {
        void emit(MetadataImage image);
    }

    /**
     * The node ID.
     */
    private final int nodeId;

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The emitter callback, which actually generates the snapshot.
     */
    private final Emitter emitter;

    /**
     * The slf4j logger to use.
     */
    private final Logger log;

    /**
     * The fault handler to use.
     */
    private final FaultHandler faultHandler;

    /**
     * The minimum number of bytes we will wait to see before emitting a snapshot.
     */
    private final long minBytesSinceLastSnapshot;

    /**
     * The minimum amount of time we will wait before emitting a snapshot, or 0 to disable
     * time-based snapshotting.
     */
    private final long minTimeSinceLastSnapshotNs;

    /**
     * If non-null, the reason why snapshots have been disabled.
     */
    private volatile String disabledReason;

    /**
     * The event queue used to schedule emitting snapshots.
     */
    private final EventQueue eventQueue;

    /**
     * The log bytes that we have read since the last snapshot.
     */
    private long bytesSinceLastSnapshot;

    /**
     * The time at which we created the last snapshot.
     */
    private long lastSnapshotTimeNs;

    private SnapshotGenerator(
        int nodeId,
        Time time,
        Emitter emitter,
        FaultHandler faultHandler,
        long minBytesSinceLastSnapshot,
        long minTimeSinceLastSnapshotNs
    ) {
        this.nodeId = nodeId;
        this.time = time;
        this.emitter = emitter;
        this.faultHandler = faultHandler;
        this.minBytesSinceLastSnapshot = minBytesSinceLastSnapshot;
        this.minTimeSinceLastSnapshotNs = minTimeSinceLastSnapshotNs;
        LogContext logContext = new LogContext("[SnapshotGenerator " + nodeId + "] ");
        this.log = logContext.logger(SnapshotGenerator.class);
        this.disabledReason = null;
        this.eventQueue = new KafkaEventQueue(time, logContext, "");
        resetSnapshotCounters();
    }

    @Override
    public String name() {
        return "SnapshotGenerator";
    }

    void resetSnapshotCounters() {
        this.bytesSinceLastSnapshot = 0L;
        this.lastSnapshotTimeNs = time.nanoseconds();
    }

    @Override
    public void publishSnapshot(
        MetadataDelta delta,
        MetadataImage newImage,
        SnapshotManifest manifest
    ) {
        // Reset the snapshot counters because we just read a snapshot.
        resetSnapshotCounters();
    }

    @Override
    public void publishPreVersionChangeImage(
        MetadataDelta delta,
        MetadataImage preVersionChangeImage,
        PreVersionChangeManifest manifest
    ) {
        scheduleEmit("the metadata version changed", preVersionChangeImage);
    }

    @Override
    public void publishLogDelta(
        MetadataDelta delta,
        MetadataImage newImage,
        LogDeltaManifest manifest
    ) {
        bytesSinceLastSnapshot += manifest.numBytes();
        if (bytesSinceLastSnapshot >= minBytesSinceLastSnapshot) {
            if (eventQueue.isEmpty()) {
                scheduleEmit("we have replayed at least " + minBytesSinceLastSnapshot +
                    " bytes", newImage);
            }
        } else if (minTimeSinceLastSnapshotNs != 0 &&
                (time.nanoseconds() - lastSnapshotTimeNs >= minTimeSinceLastSnapshotNs)) {
            if (eventQueue.isEmpty()) {
                scheduleEmit("we have waited at least " +
                    TimeUnit.NANOSECONDS.toMinutes(minTimeSinceLastSnapshotNs) + " minute(s)", newImage);
            }
        }
    }

    void scheduleEmit(
        String reason,
        MetadataImage image
    ) {
        resetSnapshotCounters();
        eventQueue.append(() -> {
            String currentDisabledReason = disabledReason;
            if (currentDisabledReason != null) {
                log.error("Not emitting {} despite the fact that {} because snapshots are " +
                    "disabled; {}", image.provenance().snapshotName(), reason,
                        currentDisabledReason);
            } else {
                log.info("Creating new KRaft snapshot file {} because {}.",
                        image.provenance().snapshotName(), reason);
                try {
                    emitter.emit(image);
                } catch (Throwable e) {
                    faultHandler.handleFault("KRaft snapshot file generation error", e);
                }
            }
        });
    }

    public void beginShutdown() {
        this.disabledReason = "we are shutting down";
        eventQueue.beginShutdown("beginShutdown");
    }

    public void disable(String disabledReason) {
        log.error("Disabling periodic KRaft snapshot generation because {}.", disabledReason);
        this.disabledReason = disabledReason;
    }

    @Override
    public void close() throws InterruptedException {
        eventQueue.beginShutdown("close");
        eventQueue.close();
    }
}
