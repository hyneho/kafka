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

package org.apache.kafka.metadata.util;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * Reads a log file containing KRaft record batches.
 */
public final class KRaftBatchFileReader implements Iterator<KRaftBatchFileReader.KRaftBatch>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KRaftBatchFileReader.class);

    public static class Builder {
        private String path = null;

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public KRaftBatchFileReader build() throws Exception {
            if (path == null) {
                throw new RuntimeException("You must specify a path.");
            }
            FileRecords fileRecords = FileRecords.open(new File(path), false);
            try {
                return new KRaftBatchFileReader(fileRecords);
            } catch (Throwable e) {
                Utils.closeQuietly(fileRecords, "fileRecords");
                throw e;
            }
        }
    }

    public static class KRaftBatch {
        private final Batch<ApiMessageAndVersion> batch;
        private final boolean isControl;

        public KRaftBatch(Batch<ApiMessageAndVersion> batch, boolean isControl) {
            this.batch = batch;
            this.isControl = isControl;
        }

        public Batch<ApiMessageAndVersion> batch() {
            return batch;
        }

        public boolean isControl() {
            return isControl;
        }
    }

    private final FileRecords fileRecords;
    private Iterator<FileChannelRecordBatch> batchIterator;
    private final MetadataRecordSerde serde;

    private KRaftBatchFileReader(FileRecords fileRecords) {
        this.fileRecords = fileRecords;
        this.batchIterator = fileRecords.batchIterator();
        this.serde = new MetadataRecordSerde();
    }

    @Override
    public boolean hasNext() {
        return this.batchIterator.hasNext();
    }

    @Override
    public KRaftBatch next() {
        FileChannelRecordBatch input = batchIterator.next();
        if (input.isControlBatch()) {
            return nextControlBatch(input);
        } else {
            return nextMetadataBatch(input);
        }
    }

    private KRaftBatch nextControlBatch(FileChannelRecordBatch input) {
        List<ApiMessageAndVersion> messages = new ArrayList<>();
        for (Iterator<Record> iter = input.iterator(); iter.hasNext(); ) {
            Record record = iter.next();
            try {
                short typeId = ControlRecordType.parseTypeId(record.key());
                ControlRecordType type = ControlRecordType.fromTypeId(typeId);
                switch (type) {
                    case LEADER_CHANGE: {
                        LeaderChangeMessage message = new LeaderChangeMessage();
                        message.read(new ByteBufferAccessor(record.value()), (short) 0);
                        messages.add(new ApiMessageAndVersion(message, (short) 0));
                        break;
                    }
                    case SNAPSHOT_HEADER: {
                        SnapshotHeaderRecord message = new SnapshotHeaderRecord();
                        message.read(new ByteBufferAccessor(record.value()), (short) 0);
                        messages.add(new ApiMessageAndVersion(message, (short) 0));
                        break;
                    }
                    case SNAPSHOT_FOOTER: {
                        SnapshotFooterRecord message = new SnapshotFooterRecord();
                        message.read(new ByteBufferAccessor(record.value()), (short) 0);
                        messages.add(new ApiMessageAndVersion(message, (short) 0));
                        break;
                    }
                    default:
                        throw new RuntimeException("Unsupported control record type " + type + " at offset " +
                                record.offset());
                }
            } catch (Throwable e) {
                throw new RuntimeException("Unable to read control record at offset " + record.offset(), e);
            }
        }
        return new KRaftBatch(Batch.data(
            input.baseOffset(),
            input.partitionLeaderEpoch(),
            input.maxTimestamp(),
            input.sizeInBytes(),
            messages), true);
    }

    private KRaftBatch nextMetadataBatch(FileChannelRecordBatch input) {
        List<ApiMessageAndVersion> messages = new ArrayList<>();
        for (Record record : input) {
            ByteBufferAccessor accessor = new ByteBufferAccessor(record.value());
            try {
                ApiMessageAndVersion messageAndVersion = serde.read(accessor, record.valueSize());
                messages.add(messageAndVersion);
            } catch (Throwable e) {
                log.error("unable to read metadata record at offset {}", record.offset(), e);
            }
        }
        return new KRaftBatch(Batch.data(
            input.baseOffset(),
            input.partitionLeaderEpoch(),
            input.maxTimestamp(),
            input.sizeInBytes(),
            messages), false);
    }

    @Override
    public void close() {
        try {
            fileRecords.closeHandlers();
        } catch (Exception e) {
            log.error("Error closing fileRecords", e);
        }
        this.batchIterator = Collections.<FileChannelRecordBatch>emptyList().iterator();
    }
}
