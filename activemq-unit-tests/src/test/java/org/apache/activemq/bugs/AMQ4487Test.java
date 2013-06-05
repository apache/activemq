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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4487Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4487Test.class);

    private final String destinationName = "TEST.QUEUE";
    private BrokerService broker;
    private ActiveMQConnectionFactory factory;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.deleteAllMessages();
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);

        PolicyEntry policy = new PolicyEntry();
        policy.setQueue(">");
        policy.setMaxProducersToAudit(75);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);

        broker.start();
        broker.waitUntilStarted();
        factory = new ActiveMQConnectionFactory("vm://localhost");
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    private void sendMessages(int messageToSend) throws Exception {
        String data = "";
        for (int i = 0; i < 1024 * 2; i++) {
            data += "x";
        }

        Connection connection = factory.createConnection();
        connection.start();

        for (int i = 0; i < messageToSend; i++) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(destinationName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(data));
            session.close();
        }

        connection.close();
    }

    @Test
    public void testBrowsingWithLessThanMaxAuditDepth() throws Exception {
        doTestBrowsing(75);
    }

    @Test
    public void testBrowsingWithMoreThanMaxAuditDepth() throws Exception {
        doTestBrowsing(300);
    }

    @SuppressWarnings("rawtypes")
    private void doTestBrowsing(int messagesToSend) throws Exception {

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(destinationName);

        sendMessages(messagesToSend);

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            assertNotNull(m);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Browsed Message: {}", m.getJMSMessageID());
            }

            received++;
            if (received > messagesToSend) {
                break;
            }
        }

        browser.close();

        assertEquals(messagesToSend, received);
    }
}