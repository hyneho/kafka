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
package org.apache.kafka.common.record;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Either;

import java.util.zip.Deflater;

import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Range.of;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE((byte) 0, "none", 1.0f),

    // Shipped with the JDK
    GZIP((byte) 1, "gzip", 1.0f),

    // We should only load classes from a given compression library when we actually use said compression library. This
    // is because compression libraries include native code for a set of platforms and we want to avoid errors
    // in case the platform is not supported and the compression library is not actually used.
    // To ensure this, we only reference compression library code from classes that are only invoked when actual usage
    // happens.
    SNAPPY((byte) 2, "snappy", 1.0f),
    LZ4((byte) 3, "lz4", 1.0f),
    ZSTD((byte) 4, "zstd", 1.0f);

    public static final int GZIP_MIN_LEVEL = Deflater.BEST_SPEED;
    public static final int GZIP_MAX_LEVEL = Deflater.BEST_COMPRESSION;
    // As of KIP-390, Deflater.DEFAULT_COMPRESSION (-1) means "I will follow Gzip's default compression level, which is currently 3."
    // That is, if a user sets the compression.gzip.level to -1, the actual compression level may be changed following the future release of gzip.
    public static final int GZIP_DEFAULT_LEVEL = Deflater.DEFAULT_COMPRESSION;
    public static final ConfigDef.Validator GZIP_LEVEL_VALIDATOR = Either.of(of(GZIP_DEFAULT_LEVEL), between(GZIP_MIN_LEVEL, GZIP_MAX_LEVEL));

    // These values come from net.jpountz.lz4.LZ4Constants
    // We may need to update them if the lz4 library changes these values.
    public static final int LZ4_MIN_LEVEL = 1;
    public static final int LZ4_MAX_LEVEL = 17;
    public static final int LZ4_DEFAULT_LEVEL = 9;
    public static final ConfigDef.Validator LZ4_LEVEL_VALIDATOR = between(LZ4_MIN_LEVEL, LZ4_MAX_LEVEL);

    // These values come from the zstd library. We don't use the Zstd.minCompressionLevel(),
    // Zstd.maxCompressionLevel() and Zstd.defaultCompressionLevel() methods to not load the Zstd library
    // while parsing configuration.
    // See ZSTD_minCLevel in https://github.com/facebook/zstd/blob/dev/lib/compress/zstd_compress.c#L6987
    // and ZSTD_TARGETLENGTH_MAX https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L1249
    public static final int ZSTD_MIN_LEVEL = -131072;
    // See ZSTD_MAX_CLEVEL in https://github.com/facebook/zstd/blob/dev/lib/compress/clevels.h#L19
    public static final int ZSTD_MAX_LEVEL = 22;
    // See ZSTD_CLEVEL_DEFAULT in https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L129
    public static final int ZSTD_DEFAULT_LEVEL = 3;
    public static final ConfigDef.Validator ZSTD_LEVEL_VALIDATOR = between(ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL);

    // compression type is represented by two bits in the attributes field of the record batch header, so `byte` is
    // large enough
    public final byte id;
    public final String name;
    public final float rate;

    CompressionType(byte id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }

    @Override
    public String toString() {
        return name;
    }

}
