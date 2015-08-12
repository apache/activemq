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
package org.apache.activemq.transport.stomp.auto;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.transport.auto.AutoTcpTransportServer;
import org.apache.activemq.transport.stomp.StompTestSupport;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that connection attempts that don't send the connect get cleaned by
 * by the protocolDetectionTimeOut property
 */
@RunWith(Parameterized.class)
public class AutoStompConnectTimeoutTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AutoStompConnectTimeoutTest.class);

    private Socket connection;
    protected String connectorScheme;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"auto"},
                {"auto+ssl"},
                {"auto+nio"},
                {"auto+nio+ssl"}
            });
    }

    public AutoStompConnectTimeoutTest(String connectorScheme) {
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
        return "?protocolDetectionTimeOut=1500";
    }

    @Test(timeout = 15000)
    public void testInactivityMonitor() throws Exception {

        Thread t1 = new Thread() {

            @Override
            public void run() {
                try {
                    connection = createSocket();
                    connection.getOutputStream().write('C');
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
                 AutoTcpTransportServer server = (AutoTcpTransportServer) brokerService.getTransportConnectorByScheme(getConnectorScheme()).getServer();
                 return 1 == server.getCurrentTransportCount().get();
             }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(250)));

        // and it should be closed due to inactivity
        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                AutoTcpTransportServer server = (AutoTcpTransportServer) brokerService.getTransportConnectorByScheme(getConnectorScheme()).getServer();
                return 0 == server.getCurrentTransportCount().get();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(500)));

        assertTrue("no exceptions", exceptions.isEmpty());
    }

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }
    @Override
    protected boolean isUseAutoConnector() {
        return connectorScheme.equalsIgnoreCase("auto");
    }

    @Override
    protected boolean isUseAutoSslConnector() {
        return connectorScheme.equalsIgnoreCase("auto+ssl");
    }

    @Override
    protected boolean isUseAutoNioConnector() {
        return connectorScheme.equalsIgnoreCase("auto+nio");
    }

    @Override
    protected boolean isUseAutoNioPlusSslConnector() {
        return connectorScheme.equalsIgnoreCase("auto+nio+ssl");
    }

    @Override
    protected Socket createSocket() throws IOException {

        boolean useSSL = false;
        int port = 0;

        switch (connectorScheme) {
            case "auto":
                port = this.autoPort;
                break;
            case "auto+ssl":
                useSSL = true;
                port = this.autoSslPort;
                break;
            case "auto+nio":
                port = this.autoNioPort;
                break;
            case "auto+nio+ssl":
                useSSL = true;
                port = this.autoNioSslPort;
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
