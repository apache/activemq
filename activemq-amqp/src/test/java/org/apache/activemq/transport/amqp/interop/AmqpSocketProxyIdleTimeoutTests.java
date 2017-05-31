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
package org.apache.activemq.transport.amqp.interop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpConnectionListener;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for idle timeout processing using SocketProxy to interrupt coms.
 */
public class AmqpSocketProxyIdleTimeoutTests extends AmqpClientTestSupport {

    private final int TEST_IDLE_TIMEOUT = 3000;

    private SocketProxy socketProxy;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        socketProxy = new SocketProxy(super.getBrokerAmqpConnectionURI());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (socketProxy != null) {
            socketProxy.close();
            socketProxy = null;
        }

        super.tearDown();
    }

    @Override
    public URI getBrokerAmqpConnectionURI() {
        return socketProxy.getUrl();
    }

    @Override
    protected String getAdditionalConfig() {
        return "&transport.wireFormat.idleTimeout=" + TEST_IDLE_TIMEOUT;
    }

    @Test(timeout = 60000)
    public void testBrokerSendsRequestedHeartbeats() throws Exception {

        final CountDownLatch disconnected = new CountDownLatch(1);

        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setIdleTimeout(TEST_IDLE_TIMEOUT);
        assertNotNull(connection);

        connection.setListener(new AmqpConnectionListener() {

            @Override
            public void onException(Throwable ex) {
                disconnected.countDown();
            }
        });

        connection.connect();

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());
        assertFalse(disconnected.await(5, TimeUnit.SECONDS));
        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        socketProxy.pause();

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());
        assertTrue(disconnected.await(10, TimeUnit.SECONDS));

        socketProxy.goOn();

        connection.close();

        assertTrue("Connection should get cleaned up.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testClientWithoutHeartbeatsGetsDropped() throws Exception {

        final CountDownLatch disconnected = new CountDownLatch(1);

        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setCloseTimeout(1000);  // Socket will have silently gone away, don't wait to long.
        assertNotNull(connection);

        connection.setListener(new AmqpConnectionListener() {

            @Override
            public void onException(Throwable ex) {
                disconnected.countDown();
            }
        });

        connection.connect();

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        socketProxy.pause();

        // Client still sends ok but broker doesn't see them.
        assertFalse(disconnected.await(5, TimeUnit.SECONDS));
        socketProxy.halfClose();
        assertTrue(disconnected.await(15, TimeUnit.SECONDS));
        socketProxy.close();

        connection.close();

        assertTrue("Connection should get cleaned up.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }
}
