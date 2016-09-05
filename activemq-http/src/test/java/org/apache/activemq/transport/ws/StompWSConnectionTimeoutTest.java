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
package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.util.Wait;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that a STOMP WS connection drops if not CONNECT or STOMP frame sent in time.
 */
public class StompWSConnectionTimeoutTest extends WSTransportTestSupport {

    protected WebSocketClient wsClient;
    protected StompWSConnection wsStompConnection;

    protected Vector<Throwable> exceptions = new Vector<Throwable>();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        wsStompConnection = new StompWSConnection();

        wsClient = new WebSocketClient();
        wsClient.start();

        wsClient.connect(wsStompConnection, wsConnectUri);
        if (!wsStompConnection.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to STOMP WS endpoint");
        }
    }

    protected String getConnectorScheme() {
        return "ws";
    }

    @Test(timeout = 90000)
    public void testInactivityMonitor() throws Exception {

        assertTrue("one connection", Wait.waitFor(new Wait.Condition() {
             @Override
             public boolean isSatisified() throws Exception {
                 return 1 == broker.getTransportConnectorByScheme(getConnectorScheme()).connectionCount();
             }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(250)));

        // and it should be closed due to inactivity
        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == broker.getTransportConnectorByScheme(getConnectorScheme()).connectionCount();
            }
        }, TimeUnit.SECONDS.toMillis(60), TimeUnit.MILLISECONDS.toMillis(500)));

        assertTrue("no exceptions", exceptions.isEmpty());
    }
}
