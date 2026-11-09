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
package org.apache.activemq.transport.vm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The connection count of a VMTransportServer decides when an on-demand vm connector disposes
 * itself. Each connection must be released exactly once, whichever stop variant is used and
 * however many times stop is called, or the count drifts and the connector is disposed while
 * connections are still in use.
 */
@Category(ParallelTest.class)
public class VMTransportServerConnectionCountTest {

    private VMTransportServer server;
    private final List<Transport> accepted = new CopyOnWriteArrayList<>();

    @Before
    public void setUp() throws Exception {
        server = new VMTransportServer(new URI("vm://connection-count-test"), true);
        server.setAcceptListener(new TransportAcceptListener() {
            @Override
            public void onAccept(Transport transport) {
                transport.setTransportListener(new DefaultTransportListener() {});
                try {
                    transport.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                accepted.add(transport);
            }

            @Override
            public void onAcceptError(Exception error) {
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        for (var transport : accepted) {
            transport.stop();
        }
        server.stop();
    }

    @Test
    public void testRepeatedStopReleasesConnectionOnce() throws Exception {
        var first = server.connect();
        var second = server.connect();
        assertEquals(2, server.getConnectionCount());

        first.stop();
        assertEquals(1, server.getConnectionCount());
        assertTrue(first.isDisposed());

        first.stop();
        assertEquals("a second stop must not release the connection again", 1, server.getConnectionCount());
        first.stop(new IOException("stopped again with an error"));
        assertEquals("a later stop with an error must not release it either", 1, server.getConnectionCount());
        assertFalse(second.isDisposed());

        // the server is still live and accepting
        server.connect();
        assertEquals(2, server.getConnectionCount());
    }

    @Test
    public void testStopWithErrorReleasesConnection() throws Exception {
        var first = server.connect();
        var second = server.connect();
        assertEquals(2, server.getConnectionCount());

        first.stop(new IOException("connection failed"));
        assertEquals("stop(Throwable) must release the connection like stop()", 1, server.getConnectionCount());
        assertTrue(first.isDisposed());

        second.stop();
        assertEquals(0, server.getConnectionCount());
    }

    @Test
    public void testLastReleaseDisposesServer() throws Exception {
        var client = server.connect();
        assertEquals(1, server.getConnectionCount());

        client.stop();
        assertEquals(0, server.getConnectionCount());
        try {
            server.connect();
            fail("server should have been disposed when its last connection was released");
        } catch (IOException expected) {
        }
    }
}
