/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;

import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockSerializer implements ClusterResourceListener, Serializer<byte[]> {
    public static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    public static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);
    public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();
    public static final AtomicBoolean IS_CLUSTER_ID_PRESENT_BEFORE_SERIALIZE = new AtomicBoolean();



    public MockSerializer() {
        INIT_COUNT.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        if (CLUSTER_META.get() != null
                && CLUSTER_META.get().getClusterId() != null
                && CLUSTER_META.get().getClusterId().length() == 48)
            IS_CLUSTER_ID_PRESENT_BEFORE_SERIALIZE.set(true);
        return data;
    }

    @Override
    public void close() {
        CLOSE_COUNT.incrementAndGet();
    }

    @Override
    public void onClusterUpdate(ClusterResource clusterMetadata) {
        CLUSTER_META.set(clusterMetadata);
    }
}
