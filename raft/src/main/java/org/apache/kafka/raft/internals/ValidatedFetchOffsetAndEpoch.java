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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.OffsetAndEpoch;

public final class ValidatedFetchOffsetAndEpoch {
    final private Type type;
    final private OffsetAndEpoch offsetAndEpoch;

    ValidatedFetchOffsetAndEpoch(Type type, OffsetAndEpoch offsetAndEpoch) {
        this.type = type;
        this.offsetAndEpoch = offsetAndEpoch;
    }

    public Type type() {
        return type;
    }

    public OffsetAndEpoch offsetAndEpoch() {
        return offsetAndEpoch;
    }

    public static enum Type {
        DIVERGING, SNAPSHOT, VALID
    }

    public static ValidatedFetchOffsetAndEpoch diverging(OffsetAndEpoch offsetAndEpoch) {
        return new ValidatedFetchOffsetAndEpoch(Type.DIVERGING, offsetAndEpoch);
    }

    public static ValidatedFetchOffsetAndEpoch snapshot(OffsetAndEpoch offsetAndEpoch) {
        return new ValidatedFetchOffsetAndEpoch(Type.SNAPSHOT, offsetAndEpoch);
    }

    public static ValidatedFetchOffsetAndEpoch valid(OffsetAndEpoch offsetAndEpoch) {
        return new ValidatedFetchOffsetAndEpoch(Type.VALID, offsetAndEpoch);
    }
}
