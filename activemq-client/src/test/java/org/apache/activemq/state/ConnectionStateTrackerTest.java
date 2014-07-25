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
package org.apache.activemq.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.SessionId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionStateTrackerTest {

    private final ActiveMQQueue queue = new ActiveMQQueue("Test");
    private ConnectionId testConnectionId;
    private SessionId testSessionId;

    private int connectionId = 0;
    private int sessionId = 0;
    private int consumerId = 0;

    @Before
    public void setUp() throws Exception {
        testConnectionId = createConnectionId();
        testSessionId = createSessionId(testConnectionId);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCacheSizeWithMessagePulls() throws IOException {

        final ConsumerId consumer1 = createConsumerId(testSessionId);

        ConnectionStateTracker tracker = new ConnectionStateTracker();

        assertEquals(0, tracker.getCurrentCacheSize());

        MessagePull pullCommand = createPullCommand(consumer1);
        tracker.track(pullCommand);

        assertEquals(0, tracker.getCurrentCacheSize());

        tracker.trackBack(pullCommand);
        long currentSize = tracker.getCurrentCacheSize();

        assertTrue(currentSize > 0);

        pullCommand = createPullCommand(consumer1);
        tracker.track(pullCommand);
        tracker.trackBack(pullCommand);

        assertEquals(currentSize, tracker.getCurrentCacheSize());
    }

    private MessagePull createPullCommand(ConsumerId id) {
        MessagePull pullCommand = new MessagePull();
        pullCommand.setDestination(queue);
        pullCommand.setConsumerId(id);
        return pullCommand;
    }

    private ConnectionId createConnectionId() {
        ConnectionId id = new ConnectionId();
        id.setValue(UUID.randomUUID() + ":" + connectionId++);
        return id;
    }

    private SessionId createSessionId(ConnectionId connectionId) {
        return new SessionId(connectionId, sessionId++);
    }

    private ConsumerId createConsumerId(SessionId sessionId) {
        return new ConsumerId(sessionId, consumerId++);
    }
}
