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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that failed SSL Handshakes don't leave the transport in a bad sate.
 */
@RunWith(Parameterized.class)
public class JMSMaxConnectionsSSLHandshakeFailsTest extends JMSClientTestSupport {

    private static final int MAX_CONNECTIONS = 10;

    private final String connectorScheme;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"amqp+ssl"},
                {"amqp+nio+ssl"},
            });
    }

    public JMSMaxConnectionsSSLHandshakeFailsTest(String connectorScheme) {
        this.connectorScheme = connectorScheme;
    }

    @Test(timeout = 60000)
    public void testFailedSSLConnectionAttemptsDoesNotBreakTransport() throws Exception {

        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            try {
                createFailingConnection();
                fail("Should not be able to connect.");
            } catch (Exception ex) {
                LOG.debug("Connection failed as expected");
            }
        }

        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            try {
                createNonSslConnection().start();;
                fail("Should not be able to connect.");
            } catch (Exception ex) {
                LOG.debug("Connection failed as expected");
            }
        }

        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            try {
                createGoodConnection();
                LOG.debug("Connection created as expected");
            } catch (Exception ex) {
                fail("Should be able to connect: " + ex.getMessage());
            }
        }

        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    protected Connection createNonSslConnection() throws Exception {
        return new JmsConnectionFactory(getGoodClientConnectURI(false)).createConnection();
    }

    protected Connection createFailingConnection() throws Exception {
        return new JmsConnectionFactory(getBadClientConnectURI()).createConnection();
    }

    protected Connection createGoodConnection() throws Exception {
        return new JmsConnectionFactory(getGoodClientConnectURI(true)).createConnection();
    }

    protected URI getGoodClientConnectURI(boolean useSsl) throws Exception {
        URI brokerURI = getBrokerURI();

        String amqpURI = (useSsl ? "amqps://" : "amqp://") + brokerURI.getHost() + ":" + brokerURI.getPort();

        if (useSsl) {
            amqpURI = amqpURI + "?transport.verifyHost=false";
        }

        return new URI(amqpURI);
    }

    protected URI getBadClientConnectURI() throws Exception {
        URI brokerURI = getBrokerURI();

        String amqpURI = "amqps://" + brokerURI.getHost() + ":" + brokerURI.getPort() +
                         "?transport.verifyHost=false" +
                         "&transport.keyStoreLocation=" + getUntrustedKeyStoreLocation();

        return new URI(amqpURI);
    }

    protected String getUntrustedKeyStoreLocation() {
        File brokerKeyStore = new File(System.getProperty("javax.net.ssl.keyStore"));
        File untrustedStore = new File(brokerKeyStore.getParent(), "alternative.keystore");

        return untrustedStore.toString();
    }

    //----- Configure the test support plumbing for this test ----------------//

    @Override
    protected String getAdditionalConfig() {
        return "&transport.needClientAuth=true&maximumConnections=" + MAX_CONNECTIONS;
    }

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseSslConnector() {
        return connectorScheme.equals("amqp+ssl");
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return connectorScheme.equals("amqp+nio+ssl");
    }

    @Override
    protected URI getBrokerURI() {
        if (connectorScheme.equals("amqp+ssl")) {
            return amqpSslURI;
        } else {
            return amqpNioPlusSslURI;
        }
    }
}
