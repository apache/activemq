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
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
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

    protected BrokerService brokerService;
    protected int port;
    protected int sslPort;
    protected int nioPort;
    protected int nioSslPort;
    protected String jmsUri = "vm://localhost";
    protected ActiveMQConnectionFactory cf;
    protected LinkedList<Throwable> exceptions = new LinkedList<Throwable>();
    protected int numberOfMessages;

    public static final int AT_MOST_ONCE = 0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE = 2;

    @Rule public TestName name = new TestName();

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    public static void main(String[] args) throws Exception {
        final MQTTTestSupport s = new MQTTTestSupport();

        s.sslPort = 5675;
        s.port = 5676;
        s.nioPort = 5677;
        s.nioSslPort = 5678;

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
        exceptions.clear();
        numberOfMessages = 1000;
        startBroker();
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    public void startBroker() throws Exception {

        createBroker();

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

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        addMQTTConnector();
        addOpenWireConnector();

        cf = new ActiveMQConnectionFactory(jmsUri);

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

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    protected void applyMemoryLimitPolicy() throws Exception {
    }

    protected void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(isPersistent());
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(true);
        brokerService.setPopulateJMSXUserID(true);
        brokerService.setSchedulerSupport(true);

        JobSchedulerStoreImpl jobStore = new JobSchedulerStoreImpl();
        jobStore.setDirectory(new File("activemq-data"));

        brokerService.setJobSchedulerStore(jobStore);
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

    protected void addOpenWireConnector() throws Exception {
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        jmsUri = connector.getPublishableConnectString();
    }

    protected void addMQTTConnector() throws Exception {
        // Overrides of this method can add additional configuration options or add multiple
        // MQTT transport connectors as needed, the port variable is always supposed to be
        // assigned the primary MQTT connector's port.
        TransportConnector connector = brokerService.addConnector(getProtocolScheme() + "://0.0.0.0:" + port);
        port = connector.getConnectUri().getPort();
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
        return getClass().getName() + "." + name.getMethodName();
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
     * assigned to be the TCP based port using the base version of addMQTTConnector.  A sbuclass
     * can either change the value of port or override this method to assign the correct port.
     *
     * @param provider
     *        the MQTTClientProvider instance to initialize.
     *
     * @throws Exception if an error occurs during initialization.
     */
    protected void initializeConnection(MQTTClientProvider provider) throws Exception {
        provider.connect("tcp://localhost:" + port);
    }

    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected boolean isPersistent() {
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
        return new FuseMQQTTClientProvider();
    }

    protected MQTT createMQTTConnection() throws Exception {
        return createMQTTConnection(null, false);
    }

    protected MQTT createMQTTConnection(String clientId, boolean clean) throws Exception {
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

    protected Tracer createTracer() {
        return new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client Received:\n" + frame);
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client Sent:\n" + frame);
            }

            @Override
            public void debug(String message, Object... args) {
                LOG.info(String.format(message, args));
            }
        };
    }
}
