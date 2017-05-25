/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.bugs;


import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PfcTimeoutTest {

    private static final Logger LOG = LoggerFactory.getLogger(PfcTimeoutTest.class);

    private static final String TRANSPORT_URL = "tcp://0.0.0.0:0";
    private static final String DESTINATION = "testQ1";

    protected BrokerService createBroker() throws Exception {

        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setAdvisorySupport(false);


        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        pe.setProducerFlowControl(true);
        pe.setMemoryLimit(10 * 1024);
        // needs to be > 100% such that any pending send that is less that 100% and pushed usage over 100% can
        // still get cached by the cursor and retain the message in memory
        pe.setCursorMemoryHighWaterMark(140);
        pe.setExpireMessagesPeriod(0);
        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector(TRANSPORT_URL);

        broker.start();
        return broker;
    }


    @Test
    public void testTransactedSendWithTimeout() throws Exception {


        BrokerService broker = createBroker();
        broker.waitUntilStarted();

        CountDownLatch gotTimeoutException = new CountDownLatch(1);

        try {
            int sendTimeout = 5000;

            //send 3 messages that will trigger producer flow and the 3rd send
            // times out after 10 seconds and rollback transaction
            sendMessages(broker, gotTimeoutException, sendTimeout, 3);

            assertTrue(gotTimeoutException.await(sendTimeout * 2, TimeUnit.MILLISECONDS));

        } finally {

            broker.stop();
            broker.waitUntilStopped();
        }

    }

    @Test
    public void testTransactedSendWithTimeoutRollbackUsage() throws Exception {


        BrokerService broker = createBroker();
        broker.waitUntilStarted();

        CountDownLatch gotTimeoutException = new CountDownLatch(1);

        try {

            int sendTimeout = 5000;

            //send 3 messages that will trigger producer flow and the 3rd send
            // times out after 10 seconds and rollback transaction
            int numberOfMessageSent = sendMessages(broker, gotTimeoutException, sendTimeout, 3);

            assertTrue(gotTimeoutException.await(sendTimeout * 2, TimeUnit.MILLISECONDS));

            //empty queue by consuming contents
            consumeMessages(broker, numberOfMessageSent);

            QueueView queueView = getQueueView(broker, DESTINATION);

            long queueSize = queueView.getQueueSize();
            long memoryUsage = queueView.getCursorMemoryUsage();


            LOG.info("queueSize after test = " + queueSize);
            LOG.info("memoryUsage after test = " + memoryUsage);

            assertEquals("queue size after test ", 0, queueSize);
            assertEquals("memory size after test ", 0, memoryUsage);

        } finally {

            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private int sendMessages(final BrokerService broker, final CountDownLatch gotTimeoutException, int sendTimeeOut, int messageCount) throws Exception {

        int numberOfMessageSent = 0;

        ActiveMQConnectionFactory connectionFactory = newConnectionFactory(broker);
        connectionFactory.setSendTimeout(sendTimeeOut);
        Connection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);

        try {


            MessageProducer jmsProducer = producerSession.createProducer(producerSession.createQueue(DESTINATION));

            Message sendMessage = producerSession.createTextMessage(createTextMessage(5000));

            for (int i = 0; i < messageCount; i++) {

                jmsProducer.send(sendMessage);
                producerSession.commit();
                numberOfMessageSent++;

            }

            LOG.info(" Finished after producing : " + numberOfMessageSent);
            return numberOfMessageSent;

        } catch (Exception ex) {

            LOG.info("Exception received producing ", ex);
            LOG.info("finishing after exception :" + numberOfMessageSent);
            LOG.info("rolling back current transaction ");

            gotTimeoutException.countDown();
            producerSession.rollback();

            return numberOfMessageSent;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

    private String createTextMessage(int size) {
        StringBuffer buffer = new StringBuffer();

        for (int i = 0; i < size; i++) {
            buffer.append("9");
        }

        return buffer.toString();
    }


    private ActiveMQConnectionFactory newConnectionFactory(BrokerService broker) throws Exception {
        ActiveMQConnectionFactory result = new ActiveMQConnectionFactory("admin", "admin", broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        result.setWatchTopicAdvisories(false);
        return result;
    }

    private int consumeMessages(BrokerService broker, int messageCount) throws Exception {

        int numberOfMessageConsumed = 0;

        ActiveMQConnectionFactory connectionFactory = newConnectionFactory(broker);
        Connection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {


            MessageConsumer jmsConsumer = consumerSession.createConsumer(consumerSession.createQueue(DESTINATION));


            for (int i = 0; i < messageCount; i++) {
                jmsConsumer.receive(1000);
                numberOfMessageConsumed++;
            }

            LOG.info(" Finished after consuming  : " + numberOfMessageConsumed);
            return numberOfMessageConsumed;

        } catch (Exception ex) {

            LOG.info("Exception received producing ", ex);
            LOG.info("finishing after exception :" + numberOfMessageConsumed);


            return numberOfMessageConsumed;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }


    private QueueView getQueueView(BrokerService broker, String queueName) throws Exception {
        Map<ObjectName, DestinationView> queueViews = broker.getAdminView().getBroker().getQueueViews();

        for (ObjectName key : queueViews.keySet()) {
            DestinationView destinationView = queueViews.get(key);

            if (destinationView instanceof QueueView) {
                QueueView queueView = (QueueView) destinationView;

                if (queueView.getName().equals(queueName)) {
                    return queueView;
                }

            }
        }
        return null;
    }

}

