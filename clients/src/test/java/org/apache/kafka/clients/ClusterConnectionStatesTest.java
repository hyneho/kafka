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

package org.apache.kafka.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Before;
import org.junit.Test;

public class ClusterConnectionStatesTest {

    protected final MockTime time = new MockTime();
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxTest = 60 * 1000;
    protected final double reconnectBackoffJitter = 0.2;
    private final String nodeId1 = "1001";
    private final String nodeId2 = "2002";

    private ClusterConnectionStates connectionStates;

    @Before
    public void setup() {
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMsTest, reconnectBackoffMaxTest);
    }

    @Test
    public void testClusterConnectionStateChanges() {
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));

        // Start connecting to Node and check state
        connectionStates.connecting(nodeId1, time.milliseconds());
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.CONNECTING);
        assertTrue(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isReady(nodeId1));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes());

        time.sleep(100);

        // Successful connection
        connectionStates.ready(nodeId1);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.READY);
        assertTrue(connectionStates.isReady(nodeId1));
        assertTrue(connectionStates.hasReadyNodes());
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.milliseconds()), Long.MAX_VALUE);

        time.sleep(15000);

        // Disconnected from broker
        connectionStates.disconnected(nodeId1, time.milliseconds());
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.DISCONNECTED);
        assertTrue(connectionStates.isDisconnected(nodeId1));
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isConnecting(nodeId1));
        assertFalse(connectionStates.hasReadyNodes());
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));

        // After disconnecting we expect a backoff value equal to the reconnect.backoff.ms setting (plus minus 20% jitter)
        long backoffTolerance = Math.round(reconnectBackoffMsTest * reconnectBackoffJitter);
        long currentBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
        assertEquals(reconnectBackoffMsTest, currentBackoff, backoffTolerance);

        time.sleep(currentBackoff + 1);
        // after waiting for the current backoff value we should be allowed to connect again
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
    }

    @Test
    public void testMultipleNodeConnectionStates() {
        // Check initial state, allowed to connect to all nodes, but no nodes shown as ready
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        assertTrue(connectionStates.canConnect(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes());

        // Start connecting one node and check that the pool only shows ready nodes after
        // successful connect
        connectionStates.connecting(nodeId2, time.milliseconds());
        assertFalse(connectionStates.hasReadyNodes());
        time.sleep(1000);
        connectionStates.ready(nodeId2);
        assertTrue(connectionStates.hasReadyNodes());

        // Connect second node and check that both are shown as ready, pool should immediately
        // show ready nodes, since node2 is already connected
        connectionStates.connecting(nodeId1, time.milliseconds());
        assertTrue(connectionStates.hasReadyNodes());
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        assertTrue(connectionStates.hasReadyNodes());

        time.sleep(12000);

        // disconnect nodes and check proper state of pool throughout
        connectionStates.disconnected(nodeId2, time.milliseconds());
        assertTrue(connectionStates.hasReadyNodes());
        assertTrue(connectionStates.isBlackedOut(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        time.sleep(connectionStates.connectionDelay(nodeId2, time.milliseconds()));
        // by the time node1 disconnects node2 should have been unblocked again
        connectionStates.disconnected(nodeId1, time.milliseconds() + 1);
        assertTrue(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId2, time.milliseconds()));
        assertFalse(connectionStates.hasReadyNodes());
    }

    @Test
    public void testAuthorizationFailed() {
        // Try connecting
        connectionStates.connecting(nodeId1, time.milliseconds());

        time.sleep(100);

        connectionStates.authenticationFailed(nodeId1, time.milliseconds(), new AuthenticationException("No path to CA for certificate!"));
        time.sleep(1000);
        assertEquals(connectionStates.connectionState(nodeId1), ConnectionState.AUTHENTICATION_FAILED);
        assertTrue(connectionStates.authenticationException(nodeId1) instanceof AuthenticationException);
        assertFalse(connectionStates.hasReadyNodes());
        assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));

        time.sleep(connectionStates.connectionDelay(nodeId1, time.milliseconds()) + 1);

        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
    }


    @Test
    public void testRemoveNode() {
        connectionStates.connecting(nodeId1, time.milliseconds());
        time.sleep(1000);
        connectionStates.ready(nodeId1);
        time.sleep(10000);

        connectionStates.disconnected(nodeId1, time.milliseconds());
        // Node is disconnected and blocked, removing it from the list should reset all blocks
        connectionStates.remove(nodeId1);
        assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
        assertFalse(connectionStates.isBlackedOut(nodeId1, time.milliseconds()));
        assertEquals(connectionStates.connectionDelay(nodeId1, time.milliseconds()), 0L);
    }

    @Test
    public void testMaxReconnectBackoff() {
        long effectiveMaxReconnectBackoff = Math.round(reconnectBackoffMaxTest * (1 + reconnectBackoffJitter));
        connectionStates.connecting(nodeId1, time.milliseconds());
        time.sleep(1000);
        connectionStates.disconnected(nodeId1, time.milliseconds());

        // Do 100 reconnect attempts and check that MaxReconnectBackoff (plus jitter) is not exceeded
        for (int i = 0; i < 100; i++) {
            long reconnectBackoff = connectionStates.connectionDelay(nodeId1, time.milliseconds());
            assertTrue(reconnectBackoff <= effectiveMaxReconnectBackoff);
            assertFalse(connectionStates.canConnect(nodeId1, time.milliseconds()));
            time.sleep(reconnectBackoff + 1);
            assertTrue(connectionStates.canConnect(nodeId1, time.milliseconds()));
            connectionStates.connecting(nodeId1, time.milliseconds());
            time.sleep(10);
            connectionStates.disconnected(nodeId1, time.milliseconds());
        }
    }
}