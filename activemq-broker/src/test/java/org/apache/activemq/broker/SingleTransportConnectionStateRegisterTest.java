/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import java.util.List;
import java.util.Map;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleTransportConnectionStateRegisterTest {

    private static final String CONNECTION_ID = "test-connection-id";
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID);
    private SingleTransportConnectionStateRegister register;

    @Before
    public void setUp() {
        register = new SingleTransportConnectionStateRegister();
    }

    @Test
    public void testRegisterConnectionState() {
        TransportConnectionState state = createState(connectionId);
        TransportConnectionState result = register.registerConnectionState(connectionId, state);

        assertEquals("Should return the registered state", state, result);
        assertEquals("Should have one connection state", 1, register.listConnectionStates().size());
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterConnectionStateNullConnectionId() {
        TransportConnectionState state = createState(connectionId);
        register.registerConnectionState(null, state);
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterConnectionStateNullState() {
        register.registerConnectionState(connectionId, null);
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterConnectionStateNullInfo() {
        register.registerConnectionState(connectionId,
                new TransportConnectionState(null, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterConnectionStateMismatch() {
        register.registerConnectionState(connectionId, createState(new ConnectionId("connection-id-2")));
    }

    @Test
    public void testUnregisterConnectionState() {
        TransportConnectionState state = createState(connectionId);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.unregisterConnectionState(connectionId);

        assertEquals("Should return the unregistered state", state, result);
        assertTrue("Should be empty after unregistering", register.isEmpty());
    }

    @Test
    public void testUnregisterConnectionStateNotFound() {
        TransportConnectionState result = register.unregisterConnectionState(connectionId);
        assertNull("Should return null when unregistering non-existent connection", result);
    }

    @Test
    public void testListConnectionStatesEmpty() {
        List<TransportConnectionState> result = register.listConnectionStates();
        assertTrue("List should be empty", result.isEmpty());
    }

    @Test
    public void testListConnectionStates() {
        TransportConnectionState state = createState(connectionId);

        register.registerConnectionState(connectionId, state);
        List<TransportConnectionState> result = register.listConnectionStates();

        assertEquals("List should contain one state", 1, result.size());
        assertEquals("State should be the registered one", state, result.get(0));
    }

    @Test
    public void testLookupConnectionStateString() {
        TransportConnectionState state = createState(connectionId);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.lookupConnectionState(CONNECTION_ID);

        assertEquals("Should return the registered state", state, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLookupConnectionStateStringNotFound() {
        register.lookupConnectionState(CONNECTION_ID);
    }

    @Test
    public void testLookupConnectionStateConsumerId() {
        TransportConnectionState state = createState(connectionId);
        ConsumerId consumerId = new ConsumerId();
        consumerId.setConnectionId(CONNECTION_ID);
        consumerId.setSessionId(1);
        consumerId.setValue(1);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.lookupConnectionState(consumerId);

        assertEquals("Should return the registered state", state, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLookupConnectionStateConsumerIdNotFound() {
        ConsumerId consumerId = new ConsumerId();
        consumerId.setConnectionId(CONNECTION_ID);

        register.lookupConnectionState(consumerId);
    }

    @Test
    public void testLookupConnectionStateProducerId() {
        TransportConnectionState state = createState(connectionId);
        ProducerId producerId = new ProducerId();
        producerId.setConnectionId(CONNECTION_ID);
        producerId.setSessionId(1);
        producerId.setValue(1);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.lookupConnectionState(producerId);

        assertEquals("Should return the registered state", state, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLookupConnectionStateProducerIdNotFound() {
        ProducerId producerId = new ProducerId();
        producerId.setConnectionId(CONNECTION_ID);

        register.lookupConnectionState(producerId);
    }

    @Test
    public void testLookupConnectionStateSessionId() {
        TransportConnectionState state = createState(connectionId);
        SessionId sessionId = new SessionId();
        sessionId.setConnectionId(CONNECTION_ID);
        sessionId.setValue(1);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.lookupConnectionState(sessionId);

        assertEquals("Should return the registered state", state, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLookupConnectionStateSessionIdNotFound() {
        SessionId sessionId = new SessionId();
        sessionId.setConnectionId(CONNECTION_ID);

        register.lookupConnectionState(sessionId);
    }

    @Test
    public void testLookupConnectionStateConnectionId() {
        TransportConnectionState state = createState(connectionId);

        register.registerConnectionState(connectionId, state);
        TransportConnectionState result = register.lookupConnectionState(connectionId);

        assertEquals("Should return the registered state", state, result);
    }

    @Test
    public void testLookupConnectionStateConnectionIdNotFound() {
        TransportConnectionState result = register.lookupConnectionState(connectionId);
        assertNull("Should return null when not registered", result);
    }

    @Test
    public void testDoesHandleMultipleConnectionStates() {
        assertFalse("Should not handle multiple connection states", register.doesHandleMultipleConnectionStates());
    }

    @Test
    public void testIsEmpty() {
        assertTrue("Should be empty initially", register.isEmpty());
        register.registerConnectionState(connectionId, createState(connectionId));
        assertFalse("Should not be empty after registering", register.isEmpty());
    }

    @Test
    public void testClear() {
        register.registerConnectionState(connectionId, createState(connectionId));
        register.clear();

        assertTrue("Should be empty after clear", register.isEmpty());
    }

    @Test
    public void testInitializeEmpty() {
        SingleTransportConnectionStateRegister other = new SingleTransportConnectionStateRegister();
        register.intialize(other);

        assertTrue("Should be empty after initializing from empty register", register.isEmpty());
    }

    @Test
    public void testInitializeFromOther() {
        SingleTransportConnectionStateRegister other = new SingleTransportConnectionStateRegister();
        TransportConnectionState state = createState(connectionId);

        other.registerConnectionState(connectionId, state);
        register.intialize(other);

        assertEquals("Should have the state from other register", state,
                register.lookupConnectionState(connectionId));
        assertEquals("Should have one connection state", 1, register.listConnectionStates().size());
    }

    @Test
    public void testMapStatesEmpty() {
        Map<ConnectionId, TransportConnectionState> result = register.mapStates();
        assertTrue("Map should be empty", result.isEmpty());
    }

    @Test
    public void testMapStatesWithConnection() {
        TransportConnectionState state = createState(connectionId);

        register.registerConnectionState(connectionId, state);
        Map<ConnectionId, TransportConnectionState> result = register.mapStates();

        assertEquals("Map should contain one entry", 1, result.size());
        assertEquals("State should be the registered one", state, result.get(connectionId));
    }

    private TransportConnectionState createState(ConnectionId connectionId) {
        return new TransportConnectionState(new ConnectionInfo(connectionId), null);
    }

}
