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
package org.apache.kafka.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.raft.OffsetAndEpoch;

public final class FileSnapshotReader implements SnapshotReader {
    private final FileRecords fileRecords;
    private final OffsetAndEpoch snapshotId;

    private FileSnapshotReader(FileRecords fileRecords, OffsetAndEpoch snapshotId) {
        this.fileRecords = fileRecords;
        this.snapshotId = snapshotId;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        return fileRecords.sizeInBytes();
    }

    @Override
    public Iterator<RecordBatch> iterator() {
        return new Iterator<RecordBatch>() {
            private final AbstractIterator<FileChannelRecordBatch> iterator = fileRecords.batchIterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public RecordBatch next() {
                return iterator.next();
            }
        };
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        return fileRecords.channel().read(buffer, position);
    }

    @Override
    public void close() throws IOException {
        fileRecords.close();
    }

    public static FileSnapshotReader open(Path logDir, OffsetAndEpoch snapshotId) throws IOException {
        FileRecords fileRecords = FileRecords.open(
            Snapshots.snapshotPath(logDir, snapshotId).toFile(),
            false, // mutable
            true, // fileAlreadyExists
            0, // initFileSize
            false // preallocate
        );

        return new FileSnapshotReader(fileRecords, snapshotId);
    }
}
