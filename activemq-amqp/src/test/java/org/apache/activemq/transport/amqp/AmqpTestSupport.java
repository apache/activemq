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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.transport.amqp.protocol.AmqpConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport {

    public static final String MESSAGE_NUMBER = "MessageNumber";
    public static final String KAHADB_DIRECTORY = "target/activemq-data/";

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);

    protected BrokerService brokerService;
    protected Vector<Throwable> exceptions = new Vector<>();
    protected int numberOfMessages;

    protected URI amqpURI;
    protected int amqpPort;
    protected URI amqpSslURI;
    protected int amqpSslPort;
    protected URI amqpNioURI;
    protected int amqpNioPort;
    protected URI amqpNioPlusSslURI;
    protected int amqpNioPlusSslPort;

    protected URI amqpWsURI;
    protected int amqpWsPort;
    protected URI amqpWssURI;
    protected int amqpWssPort;

    protected URI autoURI;
    protected int autoPort;
    protected URI autoSslURI;
    protected int autoSslPort;
    protected URI autoNioURI;
    protected int autoNioPort;
    protected URI autoNioPlusSslURI;
    protected int autoNioPlusSslPort;

    protected URI openwireURI;
    protected int openwirePort;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getTestName() + " ==========");
        exceptions.clear();

        startBroker();

        this.numberOfMessages = 2000;
    }

    protected void createBroker(boolean deleteAllMessages) throws Exception {
        brokerService = new BrokerService();

        brokerService.setPersistent(isPersistent());
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        if (isPersistent()) {
            KahaDBStore kaha = new KahaDBStore();
            kaha.setDirectory(new File(KAHADB_DIRECTORY + getTestName()));
            brokerService.setPersistenceAdapter(kaha);
        }
        brokerService.setSchedulerSupport(isSchedulerEnabled());
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(isUseJmx());
        brokerService.getManagementContext().setCreateConnector(false);

        performAdditionalConfiguration(brokerService);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);

        // Setup SSL context...
        final File classesDir = new File(AmqpConnection.class.getProtectionDomain().getCodeSource().getLocation().getFile());
        File keystore = new File(classesDir, "../../src/test/resources/keystore");
        final SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStore(keystore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(keystore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();
        brokerService.setSslContext(sslContext);

        System.setProperty("javax.net.ssl.trustStore", keystore.getCanonicalPath());
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", keystore.getCanonicalPath());
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        ArrayList<BrokerPlugin> plugins = new ArrayList<>();

        addAdditionalPlugins(plugins);

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

        addTranportConnectors();
    }

    protected void performAdditionalConfiguration(BrokerService brokerService) throws Exception {

    }

    protected void addAdditionalPlugins(List<BrokerPlugin> plugins) throws Exception {

    }

    protected void addTranportConnectors() throws Exception {
        TransportConnector connector = null;

        if (isUseOpenWireConnector()) {
            connector = brokerService.addConnector(
                "tcp://0.0.0.0:" + openwirePort);
            openwirePort = connector.getConnectUri().getPort();
            openwireURI = connector.getPublishableConnectURI();
            LOG.debug("Using openwire port " + openwirePort);
        }
        if (isUseTcpConnector()) {
            connector = brokerService.addConnector(
                "amqp://0.0.0.0:" + amqpPort + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpPort = connector.getConnectUri().getPort();
            amqpURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp port " + amqpPort);
        }
        if (isUseSslConnector()) {
            connector = brokerService.addConnector(
                "amqp+ssl://0.0.0.0:" + amqpSslPort + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpSslPort = connector.getConnectUri().getPort();
            amqpSslURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp+ssl port " + amqpSslPort);
        }
        if (isUseNioConnector()) {
            connector = brokerService.addConnector(
                "amqp+nio://0.0.0.0:" + amqpNioPort + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpNioPort = connector.getConnectUri().getPort();
            amqpNioURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp+nio port " + amqpNioPort);
        }
        if (isUseNioPlusSslConnector()) {
            connector = brokerService.addConnector(
                "amqp+nio+ssl://0.0.0.0:" + amqpNioPlusSslPort + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpNioPlusSslPort = connector.getConnectUri().getPort();
            amqpNioPlusSslURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp+nio+ssl port " + amqpNioPlusSslPort);
        }
        if (isUseAutoConnector()) {
            connector = brokerService.addConnector(
                "auto://0.0.0.0:" + autoPort + getAdditionalConfig());
            autoPort = connector.getConnectUri().getPort();
            autoURI = connector.getPublishableConnectURI();
            LOG.debug("Using auto port " + autoPort);
        }
        if (isUseAutoSslConnector()) {
            connector = brokerService.addConnector(
                "auto+ssl://0.0.0.0:" + autoSslPort + getAdditionalConfig());
            autoSslPort = connector.getConnectUri().getPort();
            autoSslURI = connector.getPublishableConnectURI();
            LOG.debug("Using auto+ssl port " + autoSslPort);
        }
        if (isUseAutoNioConnector()) {
            connector = brokerService.addConnector(
                "auto+nio://0.0.0.0:" + autoNioPort + getAdditionalConfig());
            autoNioPort = connector.getConnectUri().getPort();
            autoNioURI = connector.getPublishableConnectURI();
            LOG.debug("Using auto+nio port " + autoNioPort);
        }
        if (isUseAutoNioPlusSslConnector()) {
            connector = brokerService.addConnector(
                "auto+nio+ssl://0.0.0.0:" + autoNioPlusSslPort + getAdditionalConfig());
            autoNioPlusSslPort = connector.getConnectUri().getPort();
            autoNioPlusSslURI = connector.getPublishableConnectURI();
            LOG.debug("Using auto+nio+ssl port " + autoNioPlusSslPort);
        }
        if (isUseWsConnector()) {
            connector = brokerService.addConnector(
                "ws://0.0.0.0:" + getProxyPort(amqpWsPort) + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpWsPort = connector.getConnectUri().getPort();
            amqpWsURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp+ws port " + amqpWsPort);
        }
        if (isUseWssConnector()) {
            connector = brokerService.addConnector(
                "wss://0.0.0.0:" + getProxyPort(amqpWssPort) + "?transport.tcpNoDelay=true&transport.transformer=" + getAmqpTransformer() + getAdditionalConfig());
            amqpWssPort = connector.getConnectUri().getPort();
            amqpWssURI = connector.getPublishableConnectURI();
            LOG.debug("Using amqp+wss port " + amqpWssPort);
        }
    }

    protected boolean isPersistent() {
        return false;
    }

    protected boolean isUseJmx() {
        return true;
    }

    protected boolean isSchedulerEnabled() {
        return false;
    }

    protected boolean isUseOpenWireConnector() {
        return false;
    }

    protected boolean isUseTcpConnector() {
        return true;
    }

    protected boolean isUseSslConnector() {
        return false;
    }

    protected boolean isUseNioConnector() {
        return false;
    }

    protected boolean isUseNioPlusSslConnector() {
        return false;
    }

    protected boolean isUseAutoConnector() {
        return false;
    }

    protected boolean isUseAutoSslConnector() {
        return false;
    }

    protected boolean isUseAutoNioConnector() {
        return false;
    }

    protected boolean isUseAutoNioPlusSslConnector() {
        return false;
    }

    protected boolean isUseWsConnector() {
        return false;
    }

    protected boolean isUseWssConnector() {
        return false;
    }

    protected String getAmqpTransformer() {
        return "jms";
    }

    protected String getAdditionalConfig() {
        return "";
    }

    public void startBroker() throws Exception {
        if (brokerService != null) {
            throw new IllegalStateException("Broker is already created.");
        }

        createBroker(true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void restartBroker() throws Exception {
        restartBroker(false);
    }

    public void restartBroker(boolean deleteAllOnStartup) throws Exception {
        stopBroker();
        createBroker(deleteAllOnStartup);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void stopBroker() throws Exception {
        LOG.debug("entering AmqpTestSupport.stopBroker");
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
        LOG.debug("exiting AmqpTestSupport.stopBroker");
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
        LOG.info("========== tearDown " + getTestName() + " ==========");
    }

    public Connection createJMSConnection() throws JMSException {
        if (!isUseOpenWireConnector()) {
            throw new javax.jms.IllegalStateException("OpenWire TransportConnector was not configured.");
        }

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(openwireURI);

        return factory.createConnection();
    }

    public void sendMessages(String destinationName, int count, boolean topic) throws Exception {
        Connection connection = createJMSConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = null;
            if (topic) {
                destination = session.createTopic(destinationName);
            } else {
                destination = session.createQueue(destinationName);
            }

            sendMessages(connection, destination, count);
        } finally {
            connection.close();
        }
    }

    public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            MessageProducer p = session.createProducer(destination);

            for (int i = 1; i <= count; i++) {
                TextMessage message = session.createTextMessage();
                message.setText("TextMessage: " + i);
                message.setIntProperty(MESSAGE_NUMBER, i);
                p.send(message);
            }
        } finally {
            session.close();
        }
    }

    public String getTestName() {
        return name.getMethodName();
    }

    protected int getProxyPort(int proxyPort) {
        if (proxyPort == 0) {
            ServerSocket ss = null;
            try {
                ss = ServerSocketFactory.getDefault().createServerSocket(0);
                proxyPort = ss.getLocalPort();
            } catch (IOException e) { // ignore
            } finally {
                try {
                    if (ss != null ) {
                        ss.close();
                    }
                } catch (IOException e) { // ignore
                }
            }
        }

        return proxyPort;
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost,connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);

        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected SubscriptionViewMBean getProxyToQueueSubscriber(String name) throws MalformedObjectNameException, JMSException, IOException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        SubscriptionViewMBean subscription = null;
        for (ObjectName subscriber : proxy.getSubscriptions()) {
            subscription = (SubscriptionViewMBean) brokerService.getManagementContext()
                .newProxyInstance(subscriber, SubscriptionViewMBean.class, true);
        }

        return subscription;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }
}