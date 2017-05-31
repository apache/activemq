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

import java.net.URI;
import java.util.Date;
import java.util.Enumeration;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;

public class AMQ4595Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4595Test.class);

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        TransportConnector connector = broker.addConnector("vm://localhost");
        broker.deleteAllMessages();

        //PolicyMap pMap = new PolicyMap();
        //PolicyEntry policyEntry = new PolicyEntry();
        //policyEntry.setMaxBrowsePageSize(10000);
        //pMap.put(new ActiveMQQueue(">"), policyEntry);
        // when no policy match, browserSub has maxMessages==0
        //broker.setDestinationPolicy(pMap);

        broker.getSystemUsage().getMemoryUsage().setLimit(256 * 1024 * 1024);
        broker.start();
        broker.waitUntilStarted();
        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout=120000)
    public void testBrowsingSmallBatch() throws JMSException {
        doTestBrowsing(100);
    }

    @Test(timeout=160000)
    public void testBrowsingMediumBatch() throws JMSException {
        doTestBrowsing(1000);
    }

    @Test(timeout=300000)
    public void testBrowsingLargeBatch() throws JMSException {
        doTestBrowsing(10000);
    }

    private void doTestBrowsing(int messageToSend) throws JMSException {
        ActiveMQQueue queue = new ActiveMQQueue("TEST");

        // Send the messages to the Queue.
        ActiveMQConnection producerConnection = (ActiveMQConnection) factory.createConnection();
        producerConnection.setUseAsyncSend(true);
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 1; i <= messageToSend; i++) {
            String msgStr = provideMessageText(i, 8192);
            producer.send(producerSession.createTextMessage(msgStr));
            if ((i % 1000) == 0) {
                LOG.info("P&C: {}", msgStr.substring(0, 100));
            }
        }
        producerConnection.close();

        LOG.info("Mem usage after producer done: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage() + "%");

        // Browse the queue.
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int browsed = 0;
        while (enumeration.hasMoreElements()) {
            TextMessage m = (TextMessage) enumeration.nextElement();
            browsed++;
            if ((browsed % 1000) == 0) {
                LOG.info("B[{}]: {}", browsed, m.getText().substring(0, 100));
            }
        }
        browser.close();
        session.close();
        connection.close();

        LOG.info("Mem usage after browser closed: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage() + "%");

        // The number of messages browsed should be equal to the number of messages sent.
        assertEquals(messageToSend, browsed);

        browser.close();
    }

    public String provideMessageText(int messageNumber, int messageSize) {
        StringBuilder buf = new StringBuilder();
        buf.append("Message: ");
        if (messageNumber > 0) {
            buf.append(messageNumber);
        }
        buf.append(" sent at: ").append(new Date());

        if (buf.length() > messageSize) {
            return buf.substring(0, messageSize);
        }
        for (int i = buf.length(); i < messageSize; i++) {
            buf.append(' ');
        }
        return buf.toString();
    }

}