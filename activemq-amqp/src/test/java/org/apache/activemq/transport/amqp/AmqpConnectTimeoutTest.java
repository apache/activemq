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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that connection attempts that don't send the connect performative
 * get cleaned up by the inactivity monitor.
 */
@RunWith(Parameterized.class)
public class AmqpConnectTimeoutTest extends AmqpTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectTimeoutTest.class);

    private Socket connection;
    protected boolean useSSL;
    protected String connectorScheme;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"amqp", false},
                {"amqp+ssl", true},
                {"amqp+nio", false},
                {"amqp+nio+ssl", true}
            });
    }

    public AmqpConnectTimeoutTest(String connectorScheme, boolean useSSL) {
        this.connectorScheme = connectorScheme;
        this.useSSL = useSSL;
    }

    protected String getConnectorScheme() {
        return connectorScheme;
    }

    protected boolean isUseSSL() {
        return useSSL;
    }

    @Override
    protected boolean isUseSslConnector() {
        return isUseSSL();
    }

    @Override
    protected boolean isUseNioConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return isUseSSL();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Throwable e) {}
            connection = null;
        }
        super.tearDown();
    }

    @Override
    public String getAdditionalConfig() {
        return "&transport.connectAttemptTimeout=1200";
    }

    @Test(timeout = 30000)
    public void testInactivityMonitor() throws Exception {

        Thread t1 = new Thread() {

            @Override
            public void run() {
                try {
                    connection = createConnection();
                    connection.getOutputStream().write('A');
                    connection.getOutputStream().flush();
                } catch (Exception ex) {
                    LOG.error("unexpected exception on connect/disconnect", ex);
                    exceptions.add(ex);
                }
            }
        };

        t1.start();

        assertTrue("one connection", Wait.waitFor(new Wait.Condition() {
             @Override
             public boolean isSatisified() throws Exception {
                 return 1 == brokerService.getTransportConnectorByScheme(getConnectorScheme()).connectionCount();
             }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(250)));

        // and it should be closed due to inactivity
        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == brokerService.getTransportConnectorByScheme(getConnectorScheme()).connectionCount();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));

        assertTrue("no exceptions", exceptions.isEmpty());
    }

    protected Socket createConnection() throws IOException {

        int port = 0;
        switch (connectorScheme) {
            case "amqp":
                port = this.amqpPort;
                break;
            case "amqp+ssl":
                port = this.amqpSslPort;
                break;
            case "amqp+nio":
                port = this.amqpNioPort;
                break;
            case "amqp+nio+ssl":
                port = this.amqpNioPlusSslPort;
                break;
            default:
                throw new IOException("Invalid AMQP connector scheme passed to test.");
        }

        if (isUseSSL()) {
            return SSLSocketFactory.getDefault().createSocket("localhost", port);
        } else {
            return new Socket("localhost", port);
        }
    }
}
