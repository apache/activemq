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
package org.apache.activemq.jmx;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="http://tmielke.blogspot.com">Torsten Mielke</a>
 */
public class TotalMessageCountTest {
    private static final Logger LOG = LoggerFactory.getLogger(TotalMessageCountTest.class);

    private BrokerService brokerService;
    private final String TESTQUEUE = "testQueue";
    private ActiveMQConnectionFactory connectionFactory;
    private final String BROKER_ADDRESS = "tcp://localhost:0";
    private final ActiveMQQueue queue = new ActiveMQQueue(TESTQUEUE);

    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        startBroker(true);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }


    @Test
    public void testNegativeTotalMessageCount() throws Exception {

        LOG.info("Running test testNegativeTotalMessageCount()");
        // send one msg first
        sendMessage();

        // restart the broker
        restartBroker();

        // receive one msg
        receiveMessage();

        // assert TotalMessageCount JMX property > 0
        long totalMessageCount = getTotalMessageCount();
        if (totalMessageCount < 0 ) {
            LOG.error("Unexpected negative TotalMessageCount: " + totalMessageCount);
        } else {
            LOG.info("TotalMessageCount: " +  totalMessageCount);
        }

        assertTrue("Non negative TotalMessageCount " + totalMessageCount, totalMessageCount > -1);
        LOG.info("Test testNegativeTotalMessageCount() completed.");
    }


    /**
     * Sends one persistent TextMessage to the TESTQUEUE.
     * Initializes a new JMS connection, session and consumer and closes them
     * after use.
     * @throws JMSException on failure
     */
    private void sendMessage() throws JMSException {
        Connection conn = connectionFactory.createConnection();
        try {
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(TESTQUEUE);
            TextMessage msg = session.createTextMessage("This is a message.");
            MessageProducer producer = session.createProducer(queue);
            producer.send(queue, msg);
            LOG.info("Message sent to " + TESTQUEUE);
        } finally {
            conn.close();
        }
    }


    /**
     * Receives a single JMS message from the broker.
     * Initializes a new JMS connection, session and consumer and closes them
     * after use.
     * @return
     * @throws JMSException
     */
    private Message receiveMessage() throws JMSException {
        Connection conn = connectionFactory.createConnection();
        Message msg = null;
        try {
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(TESTQUEUE);
            MessageConsumer consumer = session.createConsumer(queue);
            msg = consumer.receive(TimeUnit.SECONDS.toMillis(10));
            if (msg != null) {
                LOG.info("Message received from " + TESTQUEUE);
            }
            consumer.close();
            session.close();
        } finally {
            conn.close();
        }
        return msg;
    }

    /**
     * restarts the broker
     *
     * @return true if restart was successful
     * @throws Exception if restart failed.
     */
    private boolean restartBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
        return startBroker(false);
    }


    /**
     * starts the broker
     *
     * @return true if start was successful
     * @throws Exception if restart failed.
     */
    private boolean startBroker(boolean deleteMessagesOnStartup) throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(true);
        brokerService.setDeleteAllMessagesOnStartup(deleteMessagesOnStartup);
        brokerService.setUseJmx(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        LOG.info("Broker started.");
        return true;
    }
    /**
     * Reads the brokers TotalMessageCount property on the JMX Broker MBean.
     * @return the total message count for the broker
     * @throws Exception if the JMX operation fails
     */
    private long getTotalMessageCount() throws Exception {

        ObjectName brokerViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean brokerMBean = (BrokerViewMBean)
                brokerService.getManagementContext().newProxyInstance(brokerViewMBeanName, BrokerViewMBean.class, true);
        LOG.debug("Broker TotalMessageCount: " + brokerMBean.getTotalMessageCount());
        return brokerMBean.getTotalMessageCount();
    }
}