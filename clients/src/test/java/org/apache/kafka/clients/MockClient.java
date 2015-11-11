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
package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;

/**
 * A mock network client for use testing code
 */
public class MockClient implements KafkaClient {
    public static final RequestMatcher ALWAYS_TRUE = new RequestMatcher() {
        @Override
        public boolean matches(ClientRequest request) {
            return true;
        }
    };

    private class FutureResponse {
        public final Struct responseBody;
        public final boolean disconnected;
        public final RequestMatcher requestMatcher;

        public FutureResponse(Struct responseBody, boolean disconnected, RequestMatcher requestMatcher) {
            this.responseBody = responseBody;
            this.disconnected = disconnected;
            this.requestMatcher = requestMatcher;
        }

    }

    private final Time time;
    private int correlation = 0;
    private Node node = null;
    private final Set<Integer> ready = new HashSet<>();
    private final Queue<ClientRequest> requests = new ArrayDeque<>();
    private final Queue<ClientResponse> responses = new ArrayDeque<>();
    private final Queue<FutureResponse> futureResponses = new ArrayDeque<>();

    public MockClient(Time time) {
        this.time = time;
    }

    @Override
    public boolean isReady(Node node, long now) {
        return ready.contains(node.id());
    }

    @Override
    public boolean ready(Node node, long now) {
        ready.add(node.id());
        return true;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    @Override
    public boolean connectionFailed(Node node) {
        return false;
    }

    public void disconnect(String node) {
        long now = time.milliseconds();
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.request().destination() == node) {
                responses.add(new ClientResponse(request, now, true, null));
                iter.remove();
            }
        }
        ready.remove(node);
    }

    @Override
    public void send(ClientRequest request, long now) {
        if (!futureResponses.isEmpty()) {
            FutureResponse futureResp = futureResponses.poll();
            if (!futureResp.requestMatcher.matches(request))
                throw new IllegalStateException("Next in line response did not match expected request");

            ClientResponse resp = new ClientResponse(request, time.milliseconds(), futureResp.disconnected, futureResp.responseBody);
            responses.add(resp);
        } else {
            request.setSendTimeMs(now);
            this.requests.add(request);
        }
    }

    @Override
    public List<ClientResponse> poll(long timeoutMs, long now) {
        List<ClientResponse> copy = new ArrayList<ClientResponse>(this.responses);

        while (!this.responses.isEmpty()) {
            ClientResponse response = this.responses.poll();
            if (response.request().hasCallback())
                response.request().callback().onComplete(response);
        }

        return copy;
    }

    public Queue<ClientRequest> requests() {
        return this.requests;
    }

    public void respond(Struct body) {
        respond(body, false);
    }

    public void respond(Struct body, boolean disconnected) {
        ClientRequest request = requests.remove();
        responses.add(new ClientResponse(request, time.milliseconds(), disconnected, body));
    }

    public void prepareResponse(Struct body) {
        prepareResponse(ALWAYS_TRUE, body, false);
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, {@link #send(ClientRequest, long)} will throw IllegalStateException
     * @param matcher The matcher to apply
     * @param body The response body
     */
    public void prepareResponse(RequestMatcher matcher, Struct body) {
        prepareResponse(matcher, body, false);
    }

    public void prepareResponse(Struct body, boolean disconnected) {
        prepareResponse(ALWAYS_TRUE, body, disconnected);
    }

    /**
     * Prepare a response for a request matching the provided matcher. If the matcher does not
     * match, {@link #send(ClientRequest, long)} will throw IllegalStateException
     * @param matcher The matcher to apply
     * @param body The response body
     * @param disconnected Whether the request was disconnected
     */
    public void prepareResponse(RequestMatcher matcher, Struct body, boolean disconnected) {
        futureResponses.add(new FutureResponse(body, disconnected, matcher));
    }

    public void setNode(Node node) {
        this.node = node;
    }

    @Override
    public int inFlightRequestCount() {
        return requests.size();
    }

    @Override
    public int inFlightRequestCount(String nodeId) {
        return requests.size();
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, "mock", correlation++);
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key, short version) {
        return new RequestHeader(key.id, version, "mock", correlation++);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    @Override
    public void close(String nodeId) {
        ready.remove(Integer.valueOf(nodeId));
    }

    @Override
    public Node leastLoadedNode(long now) {
        return this.node;
    }

    /**
     * The RequestMatcher provides a way to match a particular request to a response prepared
     * through {@link #prepareResponse(RequestMatcher, Struct)}. Basically this allows testers
     * to inspect the request body for the type of the request or for specific fields that should be set,
     * and to fail the test if it doesn't match.
     */
    public interface RequestMatcher {
        boolean matches(ClientRequest request);
    }

}
