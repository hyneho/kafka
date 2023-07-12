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

package org.apache.kafka.clients.consumer.internals.events;

/**
 * Event for resetting offsets for all assigned partitions that require it. This is an
 * asynchronous event that may complete right away if no partitions need reset, or may issue an
 * asynchronous request to ListOffsets and then update positions in memory when a response is
 * received.
 */
public class ResetPositionsApplicationEvent extends CompletableApplicationEvent<Void> {

    public ResetPositionsApplicationEvent() {
        super(Type.RESET_POSITIONS);
    }

    @Override
    public String toString() {
        return "ResetPositions{" +
                "future=" + future +
                ", type=" + type +
                '}';
    }
}
