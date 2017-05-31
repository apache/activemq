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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test for the transportConnector maximumConnections URI option.
 */
@RunWith(Parameterized.class)
public class AmqpConfiguredMaxConnectionsTest extends AmqpClientTestSupport {

    private static final int MAX_CONNECTIONS = 10;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"amqp", false},
                {"amqp+nio", false},
            });
    }

    public AmqpConfiguredMaxConnectionsTest(String connectorScheme, boolean useSSL) {
        super(connectorScheme, useSSL);
    }

    @Test(timeout = 60000)
    public void testMaxConnectionsSettingIsHonored() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        List<AmqpConnection> connections = new ArrayList<AmqpConnection>();

        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            AmqpConnection connection = client.connect();
            assertNotNull(connection);

            connections.add(connection);
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());

        try {
            AmqpConnection connection = trackConnection(client.createConnection());
            connection.setConnectTimeout(3000);
            connection.connect();
            fail("Should not be able to create one more connection");
        } catch (Exception ex) {
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());

        for (AmqpConnection connection : connections) {
            connection.close();
        }

        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Override
    protected String getAdditionalConfig() {
        return "&maximumConnections=" + MAX_CONNECTIONS;
    }
}
