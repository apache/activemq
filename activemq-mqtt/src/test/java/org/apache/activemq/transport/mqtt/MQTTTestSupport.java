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

package org.apache.activemq.transport.mqtt;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.transport.mqtt.util.ResourceLoadingSslContext;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTTestSupport.class);

    public static final String KAHADB_DIRECTORY = "target/activemq-data/";

    protected BrokerService brokerService;
    protected int port;
    protected String jmsUri = "vm://localhost";
    protected ActiveMQConnectionFactory cf;
    protected LinkedList<Throwable> exceptions = new LinkedList<Throwable>();
    protected boolean persistent;
    protected String protocolConfig;
    protected String protocolScheme;
    protected boolean useSSL;

    public static final int AT_MOST_ONCE = 0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE = 2;

    @Rule public TestName name = new TestName();

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    public MQTTTestSupport() {
        this.protocolScheme = "mqtt";
        this.useSSL = false;
    }

    public MQTTTestSupport(String connectorScheme, boolean useSSL) {
        this.protocolScheme = connectorScheme;
        this.useSSL = useSSL;
    }

    public String getTestName() {
        return name.getMethodName();
    }

    @Before
    public void setUp() throws Exception {

        String basedir = basedir().getPath();
        System.setProperty("javax.net.ssl.trustStore", basedir + "/src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", basedir + "/src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        exceptions.clear();
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    public void startBroker() throws Exception {
        brokerService = createBroker(true);

        configureBroker(brokerService);

        brokerService.start();
        brokerService.waitUntilStarted();
        port = brokerService.getTransportConnectorByName("mqtt").getConnectUri().getPort();
        jmsUri = brokerService.getTransportConnectorByName("openwire").getPublishableConnectString();
        cf = new ActiveMQConnectionFactory(jmsUri);
    }

    public void restartBroker() throws Exception {
        stopBroker();

        brokerService = createBroker(false);

        configureBroker(brokerService);

        brokerService.start();
        brokerService.waitUntilStarted();
        port = brokerService.getTransportConnectorByName("mqtt").getConnectUri().getPort();
        jmsUri = brokerService.getTransportConnectorByName("openwire").getPublishableConnectString();
        cf = new ActiveMQConnectionFactory(jmsUri);
    }

    protected BrokerService createBroker(boolean deleteAllMessages) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setPersistent(isPersistent());
        if (isPersistent()) {
            KahaDBStore kaha = new KahaDBStore();
            kaha.setDirectory(new File(KAHADB_DIRECTORY + getTestName()));
            brokerService.setPersistenceAdapter(kaha);
        }
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setSchedulerSupport(isSchedulerSupportEnabled());
        brokerService.setPopulateJMSXUserID(true);

        return brokerService;
    }

    protected void configureBroker(BrokerService brokerService) throws Exception {
        applyBrokerPolicies(brokerService);
        applyMemoryLimitPolicy(brokerService);

        // Setup SSL context...
        File keyStore = new File(basedir(), "src/test/resources/server.keystore");
        File trustStore = new File(basedir(), "src/test/resources/client.keystore");

        final ResourceLoadingSslContext sslContext = new ResourceLoadingSslContext();
        sslContext.setKeyStore(keyStore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(trustStore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();
        brokerService.setSslContext(sslContext);

        addMQTTConnector(brokerService);
        addOpenWireConnector(brokerService);

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();
        createPlugins(plugins);

        BrokerPlugin authenticationPlugin = configureAuthentication();
        if (authenticationPlugin != null) {
            plugins.add(configureAuthorization());
        }

        BrokerPlugin authorizationPlugin = configureAuthorization();
        if (authorizationPlugin != null) {
            plugins.add(configureAuthentication());
        }

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }
    }

    /**
     * Allows a subclass to add additional broker plugins during the broker startup
     * process.  This method should not add Authorization or Authentication plugins
     * as those are handled by the configureAuthentication and configureAuthorization
     * methods later.
     *
     * @param plugins
     *        The List object to add Plugins for installation into the new Broker.
     *
     * @throws Exception if an error occurs during the plugin creation process.
     */
    protected void createPlugins(List<BrokerPlugin> plugins) throws Exception {
        // NOOP
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        return null;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {
        return null;
    }

    protected void applyBrokerPolicies(BrokerService brokerService) throws Exception {
        // NOOP here
    }

    protected void applyMemoryLimitPolicy(BrokerService brokerService) throws Exception {
    }

    protected void addOpenWireConnector(BrokerService brokerService) throws Exception {
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("openwire");
        brokerService.addConnector(connector);
    }

    protected void addMQTTConnector(BrokerService brokerService) throws Exception {
        // Overrides of this method can add additional configuration options or add multiple
        // MQTT transport connectors as needed, the port variable is always supposed to be
        // assigned the primary MQTT connector's port.

        StringBuilder connectorURI = new StringBuilder();
        connectorURI.append(getProtocolScheme());
        connectorURI.append("://0.0.0.0:").append(port);
        String protocolConfig = getProtocolConfig();
        if (protocolConfig != null && !protocolConfig.isEmpty()) {
            connectorURI.append("?").append(protocolConfig);
        }

        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI(connectorURI.toString()));
        connector.setName("mqtt");
        brokerService.addConnector(connector);

        LOG.info("Added connector {} to broker", getProtocolScheme());
    }

    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    protected String getQueueName() {
        return getClass().getName() + "." + name.getMethodName();
    }

    protected String getTopicName() {
        //wildcard characters are illegal in publish
        //replace a + with something else, like _ which is allowed
        return (getClass().getName() + "." + name.getMethodName()).replace("+", "_");
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName topicViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }

    /**
     * Initialize an MQTTClientProvider instance.  By default this method uses the port that's
     * assigned to be the TCP based port using the base version of addMQTTConnector.  A subclass
     * can either change the value of port or override this method to assign the correct port.
     *
     * @param provider
     *        the MQTTClientProvider instance to initialize.
     *
     * @throws Exception if an error occurs during initialization.
     */
    protected void initializeConnection(MQTTClientProvider provider) throws Exception {
        if (!isUseSSL()) {
            provider.connect("tcp://localhost:" + port);
        } else {
            // Setup SSL context...
            File trustStore = new File(basedir(), "src/test/resources/server.keystore");
            File keyStore = new File(basedir(), "src/test/resources/client.keystore");

            final ResourceLoadingSslContext sslContext = new ResourceLoadingSslContext();
            sslContext.setKeyStore(keyStore.getCanonicalPath());
            sslContext.setKeyStorePassword("password");
            sslContext.setTrustStore(trustStore.getCanonicalPath());
            sslContext.setTrustStorePassword("password");
            sslContext.afterPropertiesSet();

            provider.setSslContext(sslContext.getSSLContext());
            provider.connect("ssl://localhost:" + port);
        }
    }

    public String getProtocolScheme() {
        return protocolScheme;
    }

    public void setProtocolScheme(String scheme) {
        this.protocolScheme = scheme;
    }

    public String getProtocolConfig() {
        return protocolConfig;
    }

    public void setProtocolConfig(String config) {
        this.protocolConfig = config;
    }

    public boolean isUseSSL() {
        return this.useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public int getPort() {
        return this.port;
    }

    public boolean isSchedulerSupportEnabled() {
        return false;
    }

    protected static interface Task {
        public void run() throws Exception;
    }

    protected  void within(int time, TimeUnit unit, Task task) throws InterruptedException {
        long timeMS = unit.toMillis(time);
        long deadline = System.currentTimeMillis() + timeMS;
        while (true) {
            try {
                task.run();
                return;
            } catch (Throwable e) {
                long remaining = deadline - System.currentTimeMillis();
                if( remaining <=0 ) {
                    if( e instanceof RuntimeException ) {
                        throw (RuntimeException)e;
                    }
                    if( e instanceof Error ) {
                        throw (Error)e;
                    }
                    throw new RuntimeException(e);
                }
                Thread.sleep(Math.min(timeMS/10, remaining));
            }
        }
    }

    protected MQTTClientProvider getMQTTClientProvider() {
        return new FuseMQTTClientProvider();
    }

    protected MQTT createMQTTConnection() throws Exception {
        return createMQTTConnection(null, false);
    }

    protected MQTT createMQTTConnection(String clientId, boolean clean) throws Exception {
        if (isUseSSL()) {
            return createMQTTSslConnection(clientId, clean);
        } else {
            return createMQTTTcpConnection(clientId, clean);
        }
    }

    private MQTT createMQTTTcpConnection(String clientId, boolean clean) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setTracer(createTracer());
        if (clientId != null) {
            mqtt.setClientId(clientId);
        }
        mqtt.setCleanSession(clean);
        mqtt.setHost("localhost", port);
        return mqtt;
    }

    private MQTT createMQTTSslConnection(String clientId, boolean clean) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setTracer(createTracer());
        mqtt.setHost("ssl://localhost:" + port);
        if (clientId != null) {
            mqtt.setClientId(clientId);
        }
        mqtt.setCleanSession(clean);

        // Setup SSL context...
        File trustStore = new File(basedir(), "src/test/resources/server.keystore");
        File keyStore = new File(basedir(), "src/test/resources/client.keystore");

        final ResourceLoadingSslContext sslContext = new ResourceLoadingSslContext();
        sslContext.setKeyStore(keyStore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(trustStore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();

        mqtt.setSslContext(sslContext.getSSLContext());
        return mqtt;
    }

    protected Tracer createTracer() {
        return new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.debug("Client Received:\n" + frame);
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.debug("Client Sent:\n" + frame);
            }

            @Override
            public void debug(String message, Object... args) {
                LOG.debug(String.format(message, args));
            }
        };
    }
}
