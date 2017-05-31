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

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Test;

/**
 * Test broker behaviors around Idle timeout when the inactivity monitor is disabled.
 */
public class AmqpDisabledInactivityMonitorTest extends AmqpClientTestSupport {

    private final int TEST_IDLE_TIMEOUT = 3000;

    @Override
    protected String getAdditionalConfig() {
        return "&transport.useInactivityMonitor=false&transport.wireFormat.idleTimeout=" + TEST_IDLE_TIMEOUT;
    }

    @Test(timeout = 60000)
    public void testBrokerDoesNotRequestIdleTimeout() throws Exception {
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
    public void testClientWithIdleTimeoutIsRejected() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setIdleTimeout(TEST_IDLE_TIMEOUT);
        assertNotNull(connection);

        try {
            connection.connect();
            fail("Connection should be rejected when idle frames can't be met.");
        } catch (Exception ex) {
        }
    }
}
