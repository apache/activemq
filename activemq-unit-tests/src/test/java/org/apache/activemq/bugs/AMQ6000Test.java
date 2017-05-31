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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for AMQ-6000 issue with pause / resume feature.
 */
public class AMQ6000Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ6000Test.class);

    private ActiveMQConnection connection;
    private BrokerService broker;
    private String connectionUri;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setPersistent(false);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        defaultEntry.setUseCache(false);
        policyMap.setDefaultEntry(defaultEntry);

        broker.setDestinationPolicy(policyMap);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connection = createConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {}
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 30000)
    public void testResumeNotDispatching() throws Exception {
        sendMessage();

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());

        QueueViewMBean queueView = getProxyToQueue(name.getMethodName());
        LOG.info("Pausing Queue");
        queueView.pause();

        MessageConsumer consumer = session.createConsumer(destination);
        assertNull(consumer.receive(100));

        LOG.info("Resuming Queue");
        queueView.resume();

        assertNotNull(consumer.receive(2000));
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    protected ActiveMQConnection createConnection() throws Exception {
        return (ActiveMQConnection) createConnectionFactory().createConnection();
    }

    private void sendMessage() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createMessage());
        session.close();
    }
}
