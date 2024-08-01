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
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKeyJsonConverter;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValueJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.ByteBuffer;
import java.util.Optional;

public class TransactionLogMessageFormatter extends ApiMessageFormatter {

    @Override
    protected Optional<ApiMessage> readToKeyMessage(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= TransactionLogKey.LOWEST_SUPPORTED_VERSION
                && version <= TransactionLogKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new TransactionLogKey(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    @Override
    protected JsonNode transferKeyMessageToJsonNode(ApiMessage message, short version) {
        return TransactionLogKeyJsonConverter.write((TransactionLogKey) message, version);
    }

    @Override
    protected Optional<ApiMessage> readToValueMessage(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= TransactionLogValue.LOWEST_SUPPORTED_VERSION
                && version <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new TransactionLogValue(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    @Override
    protected JsonNode transferValueMessageToJsonNode(ApiMessage message, short version) {
        return TransactionLogValueJsonConverter.write((TransactionLogValue) message, version);
    }
}
