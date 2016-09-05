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
package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ServerSocketFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic infrastructure for test WebSocket connections.
 */
public class WSTransportTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(WSTransportTestSupport.class);

    @Rule
    public TestName name = new TestName();

    private int proxyPort = 0;

    protected BrokerService broker;
    protected URI wsConnectUri;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== Starting test: {} ==========", name.getMethodName());
        broker = createBroker(true, true);
    }

    @After
    public void tearDown() throws Exception {
        try {
            stopBroker();
        } catch(Exception e) {
            LOG.warn("Error on Broker stop.");
        }

        LOG.info("========== Finished test: {} ==========", name.getMethodName());
    }

    protected String getWSConnectionURI() {
        return "ws://127.0.0.1:" + getProxyPort();
    }

    protected String getWSConnectorURI() {
        return "ws://127.0.0.1:" + getProxyPort() +
               "?allowLinkStealing=" + isAllowLinkStealing() +
               "&websocket.maxTextMessageSize=99999" +
               "&transport.idleTimeout=1001" +
               "&trace=true&transport.trace=true";
    }

    protected boolean isAllowLinkStealing() {
        return false;
    }

    protected void addAdditionalConnectors(BrokerService service) throws Exception {

    }

    protected BrokerService createBroker(boolean deleteMessages, boolean advisorySupport) throws Exception {

        BrokerService broker = new BrokerService();

        SpringSslContext context = new SpringSslContext();
        context.setKeyStore("src/test/resources/server.keystore");
        context.setKeyStoreKeyPassword("password");
        context.setTrustStore("src/test/resources/client.keystore");
        context.setTrustStorePassword("password");
        context.afterPropertiesSet();
        broker.setSslContext(context);

        wsConnectUri = broker.addConnector(getWSConnectorURI()).getPublishableConnectURI();

        broker.setAdvisorySupport(advisorySupport);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setPersistent(isPersistent());
        broker.setDeleteAllMessagesOnStartup(deleteMessages);
        broker.start();
        broker.waitUntilStarted();

        addAdditionalConnectors(broker);

        return broker;
    }

    protected boolean isPersistent() {
        return false;
    }

    protected String getTestName() {
        return name.getMethodName();
    }

    protected int getProxyPort() {
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

    protected void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean proxy = (BrokerViewMBean) broker.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName topicViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        TopicViewMBean proxy = (TopicViewMBean) broker.getManagementContext()
                .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }
}
