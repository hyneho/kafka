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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Objects;

class RequestState {

    private final Logger log;
    protected final ExponentialBackoff exponentialBackoff;
    protected long lastSentMs = -1;
    protected long lastReceivedMs = -1;
    protected int numAttempts = 0;
    protected long backoffMs = 0;

    public RequestState(final LogContext logContext, final ExponentialBackoff exponentialBackoff) {
        this.log = logContext.logger(getClass());
        this.exponentialBackoff = Objects.requireNonNull(exponentialBackoff);
    }

    public boolean canSendRequest(final long currentTimeMs) {
        if (this.lastSentMs == -1) {
            // no request has been sent
            return true;
        }

        if (requestInFlight()) {
            log.trace("An inflight request already exists for {}", this);
            return false;
        }

        long remainingBackoffMs = remainingBackoffMs(currentTimeMs);

        if (remainingBackoffMs <= 0) {
            return true;
        } else {
            log.trace("{} ms remain before another request should be sent for {}", remainingBackoffMs, this);
            return false;
        }
    }

    /**
     * @return True if no response has been received after the last send, indicating that there
     * is a request in-flight.
     */
    public boolean requestInFlight() {
        return this.lastSentMs > -1 && this.lastReceivedMs < this.lastSentMs;
    }

    public void onSendAttempt(final long currentTimeMs) {
        // Here we update the timer everytime we try to send a request.
        this.lastSentMs = currentTimeMs;
    }

    /**
     * Callback invoked after a successful send. This resets the number of attempts
     * to 0, but the minimal backoff will still be enforced prior to allowing a new
     * send.
     *
     * @param currentTimeMs Current time in milliseconds
     */
    public void onSuccessfulAttempt(final long currentTimeMs) {
        this.lastReceivedMs = currentTimeMs;
        this.backoffMs = exponentialBackoff.backoff(0);
        this.numAttempts = 0;
    }

    /**
     * Callback invoked after a failed send. The number of attempts
     * will be incremented, which may increase the backoff before allowing
     * the next send attempt.
     *
     * @param currentTimeMs Current time in milliseconds
     */
    public void onFailedAttempt(final long currentTimeMs) {
        this.lastReceivedMs = currentTimeMs;
        this.backoffMs = exponentialBackoff.backoff(numAttempts);
        this.numAttempts++;
    }

    long remainingBackoffMs(final long currentTimeMs) {
        long timeSinceLastReceiveMs = currentTimeMs - this.lastReceivedMs;
        return Math.max(0, backoffMs - timeSinceLastReceiveMs);
    }

    /**
     * This method appends the instance variables together in a simple String of comma-separated key value pairs.
     * This allows subclasses to include these values and not have to duplicate each variable, helping to prevent
     * any variables from being omitted when new ones are added.
     *
     * @return String version of instance variables.
     */
    protected String toStringBase() {
        return ", exponentialBackoff=" + exponentialBackoff +
                ", lastSentMs=" + lastSentMs +
                ", lastReceivedMs=" + lastReceivedMs +
                ", numAttempts=" + numAttempts +
                ", backoffMs=" + backoffMs;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{" + toStringBase() + '}';
    }
}