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

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

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
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.transport.stomp.util.ResourceLoadingSslContext;
import org.apache.activemq.transport.stomp.util.XStreamBrokerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(StompTestSupport.class);

    protected BrokerService brokerService;
    protected int openwirePort;
    protected int port;
    protected int sslPort;
    protected int nioPort;
    protected int nioSslPort;
    protected int autoPort;
    protected int autoSslPort;
    protected int autoNioPort;
    protected int autoNioSslPort;
    protected String jmsUri = "vm://localhost";
    protected StompConnection stompConnection;
    protected ActiveMQConnectionFactory cf;
    protected Vector<Throwable> exceptions = new Vector<Throwable>();

    @Rule public TestName name = new TestName();

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    public static void main(String[] args) throws Exception {
        final StompTestSupport s = new StompTestSupport();

        s.sslPort = 5675;
        s.port = 5676;
        s.nioPort = 5677;
        s.nioSslPort = 5678;
        s.autoPort = 5679;
        s.autoSslPort = 5680;
        s.autoNioPort = 5681;
        s.autoNioSslPort = 5682;

        s.startBroker();
        while(true) {
            Thread.sleep(100000);
        }
    }

    public String getName() {
        return name.getMethodName();
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getName() + " ==========");
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        try {
            stompDisconnect();
        } catch (Exception ex) {
            // its okay if the stomp connection is already closed.
        } finally {
            stopBroker();
        }
        LOG.info("========== finished " + getName() + " ==========");
    }

    public void startBroker() throws Exception {
        if (brokerService != null) {
            stopBroker();
        }
        createBroker(true);

        XStreamBrokerContext context = new XStreamBrokerContext();
        brokerService.setBrokerContext(context);

        applyBrokerPolicies();
        applyMemoryLimitPolicy();

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

        System.setProperty("javax.net.ssl.trustStore", keyStore.getCanonicalPath());
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", trustStore.getCanonicalPath());
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        addTranportConnectors();
        addOpenWireConnector();

        BrokerPlugin authenticationPlugin = configureAuthentication();
        if (authenticationPlugin != null) {
            plugins.add(configureAuthorization());
        }

        BrokerPlugin authorizationPlugin = configureAuthorization();
        if (authorizationPlugin != null) {
            plugins.add(configureAuthentication());
        }

        addAdditionalPlugins(plugins);

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
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

    protected void createBroker(boolean deleteAllOnStartup) throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(isPersistent());
        brokerService.setDeleteAllMessagesOnStartup(deleteAllOnStartup);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(true);
        brokerService.setPopulateJMSXUserID(true);
        brokerService.setSchedulerSupport(true);
        brokerService.setUseJmx(isUseJmx());
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.getManagementContext().setCreateMBeanServer(false);
    }

    protected void addAdditionalPlugins(List<BrokerPlugin> plugins) throws Exception {
    }

    protected void applyMemoryLimitPolicy() throws Exception {
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        users.add(new AuthenticationUser("user", "password", "users"));
        users.add(new AuthenticationUser("guest", "password", "guests"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        return authenticationPlugin;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins");
        tempEntry.setWrite("admins");
        tempEntry.setAdmin("admins");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }

    protected void applyBrokerPolicies() throws Exception {
        // NOOP here
    }

    public void addOpenWireConnector() throws Exception {
        cf = new ActiveMQConnectionFactory(jmsUri);
    }

    protected void addTranportConnectors() throws Exception {
        TransportConnector connector = null;

        if (isUseTcpConnector()) {
            connector = brokerService.addConnector(
                "stomp://0.0.0.0:" + port + getAdditionalConfig());
            port = connector.getConnectUri().getPort();
            LOG.debug("Using stomp port " + port);
        }
        if (isUseSslConnector()) {
            connector = brokerService.addConnector(
                "stomp+ssl://0.0.0.0:" + sslPort + getAdditionalConfig());
            sslPort = connector.getConnectUri().getPort();
            LOG.debug("Using stomp+ssl port " + sslPort);
        }
        if (isUseNioConnector()) {
            connector = brokerService.addConnector(
                "stomp+nio://0.0.0.0:" + nioPort + getAdditionalConfig());
            nioPort = connector.getConnectUri().getPort();
            LOG.debug("Using stomp+nio port " + nioPort);
        }
        if (isUseNioPlusSslConnector()) {
            connector = brokerService.addConnector(
                "stomp+nio+ssl://0.0.0.0:" + nioSslPort + getAdditionalConfig());
            nioSslPort = connector.getConnectUri().getPort();
            LOG.debug("Using stomp+nio+ssl port " + nioSslPort);
        }
        if (isUseAutoConnector()) {
            connector = brokerService.addConnector(
                "auto://0.0.0.0:" + autoPort + getAdditionalConfig());
            autoPort = connector.getConnectUri().getPort();
            LOG.debug("Using auto port " + autoPort);
        }
        if (isUseAutoSslConnector()) {
            connector = brokerService.addConnector(
                "auto+ssl://0.0.0.0:" + autoSslPort + getAdditionalConfig());
            autoSslPort = connector.getConnectUri().getPort();
            LOG.debug("Using auto+ssl port " + autoSslPort);
        }
        if (isUseAutoNioConnector()) {
            connector = brokerService.addConnector(
                "auto+nio://0.0.0.0:" + autoNioPort + getAdditionalConfig());
            autoNioPort = connector.getConnectUri().getPort();
            LOG.debug("Using auto+nio port " + autoNioPort);
        }
        if (isUseAutoNioPlusSslConnector()) {
            connector = brokerService.addConnector(
                "auto+nio+ssl://0.0.0.0:" + autoNioSslPort + getAdditionalConfig());
            autoNioSslPort = connector.getConnectUri().getPort();
            LOG.debug("Using auto+nio+ssl port " + autoNioSslPort);
        }
    }

    protected boolean isPersistent() {
        return false;
    }

    protected boolean isUseJmx() {
        return true;
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

    protected String getAdditionalConfig() {
        return "";
    }

    protected StompConnection stompConnect() throws Exception {
        if (stompConnection == null) {
            stompConnection = new StompConnection();
        }
        stompConnection.open(createSocket());
        return stompConnection;
    }

    protected StompConnection stompConnect(StompConnection connection) throws Exception {
        connection.open(createSocket());
        return stompConnection;
    }

    protected Socket createSocket() throws IOException {
        return new Socket("127.0.0.1", this.port);
    }

    protected String getQueueName() {
        return getClass().getName() + "." + name.getMethodName();
    }

    protected String getTopicName() {
        return getClass().getName() + "." + name.getMethodName();
    }

    protected void stompDisconnect() throws Exception {
        if (stompConnection != null) {
            stompConnection.disconnect();
            stompConnection.close();
            stompConnection = null;
        }
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
}
