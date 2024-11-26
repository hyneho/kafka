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
package org.apache.kafka.common.compress;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SnappyCompressionTest {

    @Test
    public void testCompressionDecompression() throws IOException {
        byte[] data = String.join("", Collections.nCopies(256, "data")).getBytes(StandardCharsets.UTF_8);

        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            for (int blockSize : new Random().ints(4, CompressionType.SNAPPY_MIN_BLOCK, CompressionType.SNAPPY_MAX_BLOCK).toArray()) {
                SnappyCompression compression = Compression.snappy().blockSize(blockSize).build();
                ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(4);
                try (OutputStream out = compression.wrapForOutput(bufferStream, magic)) {
                    out.write(data);
                    out.flush();
                }
                bufferStream.buffer().flip();

                try (InputStream inputStream = compression.wrapForInput(bufferStream.buffer(), magic, BufferSupplier.create())) {
                    byte[] result = new byte[data.length];
                    int read = inputStream.read(result);
                    assertEquals(data.length, read);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    @Test
    public void testCompressionBlockSizes() {
        SnappyCompression.Builder builder = Compression.snappy();

        assertThrows(IllegalArgumentException.class, () -> builder.blockSize(CompressionType.SNAPPY_MIN_BLOCK - 1));
        assertThrows(IllegalArgumentException.class, () -> builder.blockSize(CompressionType.SNAPPY_MAX_BLOCK + 1));

        builder.blockSize(CompressionType.SNAPPY_MIN_BLOCK);
        builder.blockSize(CompressionType.SNAPPY_MAX_BLOCK);
    }

    @Test
    public void testBlockSizeValidator() {
        ConfigDef.Validator validator = CompressionType.SNAPPY_BLOCK_VALIDATOR;
        for (int blockSize : new Random().ints(32, CompressionType.SNAPPY_MIN_BLOCK, CompressionType.SNAPPY_MAX_BLOCK).toArray()) {
            validator.ensureValid("", blockSize);
        }
        validator.ensureValid("", CompressionType.SNAPPY_DEFAULT_BLOCK);
        assertThrows(ConfigException.class, () -> validator.ensureValid("", CompressionType.SNAPPY_MIN_BLOCK - 1));
        assertThrows(ConfigException.class, () -> validator.ensureValid("", CompressionType.SNAPPY_MAX_BLOCK + 1));
    }
}
