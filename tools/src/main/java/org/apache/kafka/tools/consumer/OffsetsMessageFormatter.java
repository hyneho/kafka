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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValueJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
 */
public class OffsetsMessageFormatter extends ApiMessageFormatter {

    @Override
    protected Optional<ApiMessage> readToKeyMessage(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitKey.LOWEST_SUPPORTED_VERSION
                && version <= OffsetCommitKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new OffsetCommitKey(new ByteBufferAccessor(byteBuffer), version));
        } else if (version >= GroupMetadataKey.LOWEST_SUPPORTED_VERSION && version <= GroupMetadataKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new GroupMetadataKey(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    @Override
    protected JsonNode transferKeyMessageToJsonNode(ApiMessage logKey, short keyVersion) {
        if (logKey instanceof OffsetCommitKey) {
            return OffsetCommitKeyJsonConverter.write((OffsetCommitKey) logKey, keyVersion);
        } else if (logKey instanceof GroupMetadataKey) {
            return NullNode.getInstance();
        } else {
            return new TextNode(UNKNOWN);
        }
    }

    @Override
    protected Optional<ApiMessage> readToValueMessage(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitValue.LOWEST_SUPPORTED_VERSION
                && version <= OffsetCommitValue.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new OffsetCommitValue(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    @Override
    protected JsonNode transferValueMessageToJsonNode(ApiMessage message, short version) {
        return OffsetCommitValueJsonConverter.write((OffsetCommitValue) message, version);
    }
}
