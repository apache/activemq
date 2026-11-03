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
package org.apache.activemq.xbean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.net.URI;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;

/**
 * Spring XML wiring of the JMX connector's sslContext: either a reference to the
 * broker's own context, or a nested context of its own. The handshake is checked
 * with a plain TLS client against the RMI registry port, which avoids the JVM wide
 * default context that javax.rmi.ssl clients rely on.
 */
public class ManagementContextSslXBeanConfigTest {

    private static final String BROKER_DEFAULT_XML = "org/apache/activemq/xbean/management-context-ssl-broker-default.xml";
    private static final String CUSTOM_XML = "org/apache/activemq/xbean/management-context-ssl-custom.xml";
    private static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    private static final String JMX_KEYSTORE = "src/test/resources/org/apache/activemq/security/activemq-revoke.jks";

    private BrokerService broker;

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testJmxReferencesTheBrokerSslContext() throws Exception {
        broker = start(BROKER_DEFAULT_XML);
        ManagementContext management = broker.getManagementContext();

        assertNotNull("broker sslContext from xml", broker.getSslContext());
        assertSame("the managementContext must reuse the broker's sslContext bean", broker.getSslContext(), management.getSslContext());
        assertEquals("registry presents the broker certificate", certificate(SERVER_KEYSTORE, "broker"),
                registryCertificate(management, SERVER_KEYSTORE));
    }

    @Test(timeout = 60000)
    public void testJmxUsesItsOwnSslContext() throws Exception {
        broker = start(CUSTOM_XML);
        ManagementContext management = broker.getManagementContext();

        SslContext jmxContext = management.getSslContext();
        assertNotNull("nested sslContext from xml", jmxContext);
        assertNotSame("the managementContext must not fall back to the broker's sslContext", broker.getSslContext(), jmxContext);
        assertTrue("xbean wires a SpringSslContext", jmxContext instanceof SpringSslContext);
        assertTrue("configured keystore: " + ((SpringSslContext) jmxContext).getKeyStore(),
                ((SpringSslContext) jmxContext).getKeyStore().endsWith("activemq-revoke.jks"));
        assertEquals("registry presents the JMX certificate, not the broker's", certificate(JMX_KEYSTORE, "broker"),
                registryCertificate(management, JMX_KEYSTORE));
    }

    private BrokerService start(String xml) throws Exception {
        BrokerService service = BrokerFactory.createBroker(new URI("xbean:" + xml));
        service.start();
        service.waitUntilStarted();
        ManagementContext management = service.getManagementContext();
        assertTrue("JMX connector should bind", Wait.waitFor(management::isConnectorActive, 20000, 10));
        assertTrue("ephemeral port resolved", management.getConnectorPort() > 0);
        return service;
    }

    private static KeyStore keyStore(String path) throws Exception {
        KeyStore store = KeyStore.getInstance("jks");
        try (FileInputStream in = new FileInputStream(path)) {
            store.load(in, "password".toCharArray());
        }
        return store;
    }

    private static X509Certificate certificate(String path, String alias) throws Exception {
        X509Certificate certificate = (X509Certificate) keyStore(path).getCertificate(alias);
        assertNotNull("alias " + alias + " in " + path, certificate);
        return certificate;
    }

    /** completes a TLS handshake with the RMI registry, trusting only the given keystore, and returns the certificate it presented */
    private static X509Certificate registryCertificate(ManagementContext management, String trustStorePath) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore(trustStorePath));
        SSLContext client = SSLContext.getInstance("TLS");
        client.init(null, tmf.getTrustManagers(), null);
        try (SSLSocket socket = (SSLSocket) client.getSocketFactory().createSocket("localhost", management.getConnectorPort())) {
            socket.setSoTimeout(10000);
            socket.startHandshake();
            return (X509Certificate) socket.getSession().getPeerCertificates()[0];
        }
    }
}
