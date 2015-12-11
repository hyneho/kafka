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

package org.apache.kafka.streams.kstream.internals;

public abstract class AbstractWindows {

    private static final long DEFAULT_EMIT_DURATION = 1000L;

    private static final long DEFAULT_MAINTAIN_DURATION = 24 * 60 * 60 * 1000L;   // one day

    private long emitDuration;

    private long maintainDuration;

    protected AbstractWindows() {
        this.emitDuration = DEFAULT_EMIT_DURATION;
        this.maintainDuration = DEFAULT_MAINTAIN_DURATION;
    }

    /**
     * Set the window emit duration in milliseconds of system time
     */
    public AbstractWindows emit(long duration) {
        this.emitDuration = duration;

        return this;
    }

    /**
     * Set the window maintain duration in milliseconds of system time
     */
    public AbstractWindows until(long duration) {
        this.maintainDuration = duration;

        return this;
    }
}