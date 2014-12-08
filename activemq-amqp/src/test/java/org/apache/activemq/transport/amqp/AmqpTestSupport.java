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
import java.security.SecureRandom;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport {

    public static final String MESSAGE_NUMBER = "MessageNumber";

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    protected BrokerService brokerService;
    protected Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    protected int port;
    protected int sslPort;
    protected int nioPort;
    protected int nioPlusSslPort;
    protected int openwirePort;

    public static void main(String[] args) throws Exception {
        final AmqpTestSupport s = new AmqpTestSupport();
        s.sslPort = 5671;
        s.port = 5672;
        s.startBroker();
        while (true) {
            Thread.sleep(100000);
        }
    }

    @Before
    public void setUp() throws Exception {
        exceptions.clear();
        if (killHungThreads("setUp")) {
            LOG.warn("HUNG THREADS in setUp");
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new SetUpTask());
        try {
            LOG.debug("SetUpTask started.");
            future.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new Exception("startBroker timed out");
        }
        executor.shutdownNow();

        this.numberOfMessages = 2000;
    }

    protected void createBroker(boolean deleteAllMessages) throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setUseJmx(true);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);

        // Setup SSL context...
        final File classesDir = new File(AmqpProtocolConverter.class.getProtectionDomain().getCodeSource().getLocation().getFile());
        File keystore = new File(classesDir, "../../src/test/resources/keystore");
        final SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStore(keystore.getCanonicalPath());
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(keystore.getCanonicalPath());
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();
        brokerService.setSslContext(sslContext);

        addAMQPConnector();
    }

    protected void addAMQPConnector() throws Exception {
        TransportConnector connector = null;

        if (isUseOpenWireConnector()) {
            connector = brokerService.addConnector(
                "tcp://0.0.0.0:" + openwirePort);
            openwirePort = connector.getConnectUri().getPort();
            LOG.debug("Using openwire port " + openwirePort);
        }
        if (isUseTcpConnector()) {
            connector = brokerService.addConnector(
                "amqp://0.0.0.0:" + port + "?transport.transformer=" + getAmqpTransformer());
            port = connector.getConnectUri().getPort();
            LOG.debug("Using amqp port " + port);
        }
        if (isUseSslConnector()) {
            connector = brokerService.addConnector(
                "amqp+ssl://0.0.0.0:" + sslPort + "?transport.transformer=" + getAmqpTransformer());
            sslPort = connector.getConnectUri().getPort();
            LOG.debug("Using amqp+ssl port " + sslPort);
        }
        if (isUseNioConnector()) {
            connector = brokerService.addConnector(
                "amqp+nio://0.0.0.0:" + nioPort + "?transport.transformer=" + getAmqpTransformer());
            nioPort = connector.getConnectUri().getPort();
            LOG.debug("Using amqp+nio port " + nioPort);
        }
        if (isUseNioPlusSslConnector()) {
            connector = brokerService.addConnector(
                "amqp+nio+ssl://0.0.0.0:" + nioPlusSslPort + "?transport.transformer=" + getAmqpTransformer());
            nioPlusSslPort = connector.getConnectUri().getPort();
            LOG.debug("Using amqp+nio+ssl port " + nioPlusSslPort);
        }
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

    protected String getAmqpTransformer() {
        return "jms";
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
        stopBroker();
        createBroker(false);
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
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new TearDownTask());
        try {
            LOG.debug("tearDown started.");
            future.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new Exception("stopBroker timed out");
        } finally {
            executor.shutdownNow();

            if (killHungThreads("tearDown")) {
                LOG.warn("HUNG THREADS in tearDown");
            }
        }
    }

    private boolean killHungThreads(String stage) throws Exception{
        Thread.sleep(500);
        if (Thread.activeCount() == 1) {
            return false;
        }
        LOG.warn("Hung Thread(s) on {} entry threadCount {} ", stage, Thread.activeCount());

        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        for (int i=0; i < threads.length; i++) {
            Thread t = threads[i];
            if (!t.getName().equals("main")) {
                LOG.warn("KillHungThreads: Interrupting thread {}", t.getName());
                t.interrupt();
            }
        }

        LOG.warn("Hung Thread on {} exit threadCount {} ", stage, Thread.activeCount());
        return true;
    }

    public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = session.createProducer(destination);

        for (int i = 1; i <= count; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("TextMessage: " + i);
            message.setIntProperty(MESSAGE_NUMBER, i);
            p.send(message);
        }

        session.close();
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

    protected QueueViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    public class SetUpTask implements Callable<Boolean> {
        @SuppressWarnings("unused")
        private String testName;

        @Override
        public Boolean call() throws Exception {
            LOG.debug("in SetUpTask.call, calling startBroker");
            startBroker();

            return Boolean.TRUE;
        }
    }

    public class TearDownTask implements Callable<Boolean> {
        @SuppressWarnings("unused")
        private String testName;

        @Override
        public Boolean call() throws Exception {
            LOG.debug("in TearDownTask.call(), calling stopBroker");
            stopBroker();

            return Boolean.TRUE;
        }
    }
}