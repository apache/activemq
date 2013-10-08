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
import java.util.Vector;

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

import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    protected BrokerService brokerService;
    protected Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    AutoFailTestSupport autoFailTestSupport = new AutoFailTestSupport() {};
    protected int port;
    protected int sslPort;
    protected int nioPort;
    protected int nioPlusSslPort;

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
        autoFailTestSupport.startAutoFailThread();
        exceptions.clear();
        startBroker();
    }

    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);

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
        brokerService.start();
        this.numberOfMessages = 2000;
    }

    protected void addAMQPConnector() throws Exception {
        TransportConnector connector = brokerService.addConnector("amqp+ssl://0.0.0.0:" + sslPort);
        sslPort = connector.getConnectUri().getPort();
        LOG.debug("Using amqp+ssl port " + sslPort);
        connector = brokerService.addConnector("amqp://0.0.0.0:" + port);
        port = connector.getConnectUri().getPort();
        LOG.debug("Using amqp port " + port);
        connector = brokerService.addConnector("amqp+nio://0.0.0.0:" + nioPort);
        nioPort = connector.getConnectUri().getPort();
        LOG.debug("Using amqp+nio port " + nioPort);
        connector = brokerService.addConnector("amqp+nio+ssl://0.0.0.0:" + nioPlusSslPort);
        nioPlusSslPort = connector.getConnectUri().getPort();
        LOG.debug("Using amqp+nio+ssl port " + nioPlusSslPort);
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }
        autoFailTestSupport.stopAutoFailThread();
    }

    public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = session.createProducer(destination);

        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("TextMessage: " + i);
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
}