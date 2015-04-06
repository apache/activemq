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
package org.apache.activemq.transport.amqp.joram;

import java.io.File;
import java.security.SecureRandom;

import javax.naming.NamingException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.transport.amqp.DefaultTrustManager;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQNIOPlusSSLAdmin extends ActiveMQAdmin {

    private static final String AMQP_NIO_PLUS_SSL_URL = "amqp+nio+ssl://localhost:0";
    protected static final Logger LOG = LoggerFactory.getLogger(ActiveMQNIOPlusSSLAdmin.class);

    @Override
    public void startServer() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);

        // Setup SSL context...
        final File classesDir = new File(ActiveMQNIOPlusSSLAdmin.class.getProtectionDomain().getCodeSource().getLocation().getFile());
        File keystore = new File(classesDir, "../../src/test/resources/keystore");
        final SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStore(keystore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(keystore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();

        if (broker != null) {
            stopServer();
        }
        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }
        broker = createBroker();
        broker.setSslContext(sslContext);

        String connectorURI = getConnectorURI();
        TransportConnector connector = broker.addConnector(connectorURI);
        port = connector.getConnectUri().getPort();
        LOG.info("nio+ssl port is {}", port);

        broker.start();
    }

    @Override
    protected String getConnectorURI() {
        return AMQP_NIO_PLUS_SSL_URL;
    }

    @Override
    public void createConnectionFactory(String name) {
        try {
            LOG.debug("Creating a connection factory using port {}", port);
            final JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + port);

            SpringSslContext sslContext = (SpringSslContext) broker.getSslContext();

            System.setProperty("javax.net.ssl.trustStore", sslContext.getTrustStore());
            System.setProperty("javax.net.ssl.trustStorePassword", "password");
            System.setProperty("javax.net.ssl.trustStoreType", "jks");
            System.setProperty("javax.net.ssl.keyStore", sslContext.getKeyStore());
            System.setProperty("javax.net.ssl.keyStorePassword", "password");
            System.setProperty("javax.net.ssl.keyStoreType", "jks");

            context.bind(name, factory);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }
}
