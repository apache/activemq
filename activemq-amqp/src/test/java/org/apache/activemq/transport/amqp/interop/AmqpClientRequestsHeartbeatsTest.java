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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpConnectionListener;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.activemq.util.Wait;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests that cover broker behavior when the client requests heartbeats
 */
@RunWith(Parameterized.class)
public class AmqpClientRequestsHeartbeatsTest extends AmqpClientTestSupport {

    private final int TEST_IDLE_TIMEOUT = 1000;

    @Parameters(name="connector={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"amqp", false},
            {"amqp+ws", false},
        });
    }

    public AmqpClientRequestsHeartbeatsTest(String connectorScheme, boolean secure) {
        super(connectorScheme, secure);
    }

    @Override
    protected String getAdditionalConfig() {
        return "&transport.wireFormat.idleTimeout=0";
    }

    @Test(timeout = 60000)
    public void testBrokerWitZeroIdleTimeDoesNotAdvertise() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        client.setValidator(new AmqpValidator() {

            @Override
            public void inspectOpenedResource(Connection connection) {
                assertEquals(0, connection.getTransport().getRemoteIdleTimeout());
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        assertNotNull(connection);

        connection.getStateInspector().assertValid();
        connection.close();
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

        connection.close();

        assertTrue("Connection should get cleaned up.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }
}
