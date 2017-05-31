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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.util.Wait;
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
public class StompConnectTimeoutTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompConnectTimeoutTest.class);

    private Socket connection;
    protected String connectorScheme;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"stomp"},
                {"stomp+ssl"},
                {"stomp+nio"},
                {"stomp+nio+ssl"}
            });
    }

    public StompConnectTimeoutTest(String connectorScheme) {
        this.connectorScheme = connectorScheme;
    }

    protected String getConnectorScheme() {
        return connectorScheme;
    }

    @Override
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
        return "?transport.connectAttemptTimeout=1200";
    }

    @Test(timeout = 15000)
    public void testInactivityMonitor() throws Exception {

        Thread t1 = new Thread() {

            @Override
            public void run() {
                try {
                    connection = createSocket();
                    connection.getOutputStream().write('S');
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

    @Override
    protected boolean isUseTcpConnector() {
        return connectorScheme.equalsIgnoreCase("stomp");
    }

    @Override
    protected boolean isUseSslConnector() {
        return connectorScheme.equalsIgnoreCase("stomp+ssl");
    }

    @Override
    protected boolean isUseNioConnector() {
        return connectorScheme.equalsIgnoreCase("stomp+nio");
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return connectorScheme.equalsIgnoreCase("stomp+nio+ssl");
    }

    @Override
    protected Socket createSocket() throws IOException {

        boolean useSSL = false;
        int port = 0;

        switch (connectorScheme) {
            case "stomp":
                port = this.port;
                break;
            case "stomp+ssl":
                useSSL = true;
                port = this.sslPort;
                break;
            case "stomp+nio":
                port = this.nioPort;
                break;
            case "stomp+nio+ssl":
                useSSL = true;
                port = this.nioSslPort;
                break;
            default:
                throw new IOException("Invalid STOMP connector scheme passed to test.");
        }

        if (useSSL) {
            return SSLSocketFactory.getDefault().createSocket("localhost", port);
        } else {
            return new Socket("localhost", port);
        }
    }
}
