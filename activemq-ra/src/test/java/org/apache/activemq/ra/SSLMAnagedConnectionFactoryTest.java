/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SSLMAnagedConnectionFactoryTest {

    private static final String KAHADB_DIRECTORY = "target/activemq-data/";
    private static final String DEFAULT_HOST = "ssl://localhost:0";

    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private ConnectionFactory connectionFactory;
    private ManagedConnectionProxy connection;
    private ActiveMQManagedConnection managedConnection;
    private SslBrokerService broker;
    private String connectionURI;

    @Before
    public void setUp() throws Exception {
        createAndStartBroker();

        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl(connectionURI);
        managedConnectionFactory.setTrustStore("server.keystore");
        managedConnectionFactory.setTrustStorePassword("password");
        managedConnectionFactory.setKeyStore("client.keystore");
        managedConnectionFactory.setKeyStorePassword("password");

        connectionFactory = (ConnectionFactory)managedConnectionFactory.createConnectionFactory(connectionManager);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test(timeout = 60000)
    public void testSSLManagedConnection() throws Exception {
        connection = (ManagedConnectionProxy)connectionFactory.createConnection();
        managedConnection = connection.getManagedConnection();
        //do some stuff
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue t = session.createQueue("TEST");
        MessageProducer producer = session.createProducer(t);
        producer.send(session.createTextMessage("test message."));
        managedConnection.destroy();
        connection.close();
    }

    private void createAndStartBroker() throws Exception {
        broker = new SslBrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(false);
        broker.setBrokerName("BROKER");
        broker.setDataDirectory(KAHADB_DIRECTORY);
        KeyManager[] km = SSLTest.getKeyManager();
        TrustManager[] tm = SSLTest.getTrustManager();
        TransportConnector connector = broker.addSslConnector(DEFAULT_HOST, km, tm, null);
        broker.start();
        broker.waitUntilStarted();

        connectionURI = connector.getPublishableConnectString();

        SslTransportFactory sslFactory = new SslTransportFactory();
        SslContext ctx = new SslContext(km, tm, null);
        SslContext.setCurrentSslContext(ctx);
        TransportFactory.registerTransportFactory("ssl", sslFactory);
    }
}
