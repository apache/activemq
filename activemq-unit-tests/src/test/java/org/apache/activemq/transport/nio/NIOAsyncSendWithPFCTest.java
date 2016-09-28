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
package org.apache.activemq.transport.nio;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 demonstrates that with nio it does not make sense to block on the broker but thread pool
 shold grow past initial corepoolsize of 10
 */
public class NIOAsyncSendWithPFCTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NIOAsyncSendWithPFCTest.class);

    private static String TRANSPORT_URL = "nio://0.0.0.0:0";
    private static final String DESTINATION_ONE = "testQ1";
    private static final String DESTINATION_TWO = "testQ2";
    private static final int MESSAGES_TO_SEND = 100;
    private static int NUMBER_OF_PRODUCERS = 10;

    protected BrokerService createBroker() throws Exception {

        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();


        pe.setMemoryLimit(256000);
        pe.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());


        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);


        broker.addConnector(TRANSPORT_URL);
        broker.setDestinations(new ActiveMQDestination[]{new ActiveMQQueue(DESTINATION_ONE)});

        broker.start();
        TRANSPORT_URL = broker.getTransportConnectorByScheme("nio").getPublishableConnectString();
        return broker;
    }

    /**
     * Test creates 10 producer who send to a single destination using Async mode.
     * Producer flow control kicks in for that destination. When producer flow control is blocking sends
     * Test tries to create another JMS connection to the nio.
     */
    public void testAsyncSendPFCNewConnection() throws Exception {


        BrokerService broker = createBroker();
        broker.waitUntilStarted();


        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PRODUCERS);
        QueueView queueView = getQueueView(broker, DESTINATION_ONE);

        try {

            for (int i = 0; i < NUMBER_OF_PRODUCERS; i++) {

                executorService.submit(new ProducerTask());

            }

            //wait till producer follow control kicks in
            waitForProducerFlowControl(broker, queueView);


            try {
                sendMessagesAsync(1, DESTINATION_TWO);
            } catch (Exception ex) {
                LOG.error("Ex on send  new connection", ex);
                fail("*** received the following exception when creating addition producer new connection:" + ex);
            }


        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }


    }


    public void testAsyncSendPFCExistingConnection() throws Exception {


        BrokerService broker = createBroker();
        broker.waitUntilStarted();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", TRANSPORT_URL + "?wireFormat.maxInactivityDuration=5000");
        ActiveMQConnection exisitngConnection = (ActiveMQConnection) connectionFactory.createConnection();


        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PRODUCERS);
        QueueView queueView = getQueueView(broker, DESTINATION_ONE);

        try {

            for (int i = 0; i < NUMBER_OF_PRODUCERS; i++) {

                executorService.submit(new ProducerTask());

            }


            //wait till producer follow control kicks in
            waitForProducerFlowControl(broker, queueView);


            TestSupport.dumpAllThreads("Blocked");

            try {
                Session producerSession = exisitngConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (Exception ex) {
                LOG.error("Ex on create session", ex);
                fail("*** received the following exception when creating producer session:" + ex);
            }


        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }



    }

    private void waitForProducerFlowControl(BrokerService broker, QueueView queueView) throws Exception {


        boolean blockingAllSends;
        do {
            blockingAllSends = queueView.getBlockedSends() > 10;
            Thread.sleep(1000);

        } while (!blockingAllSends);
    }

    class ProducerTask implements Runnable {


        @Override
        public void run() {
            try {
                //send X messages
                sendMessagesAsync(MESSAGES_TO_SEND, DESTINATION_ONE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private Long sendMessagesAsync(int messageCount, String destination) throws Exception {

        long numberOfMessageSent = 0;


        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", TRANSPORT_URL);


        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setUseAsyncSend(true);
        connection.start();

        try {

            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer jmsProducer = producerSession.createProducer(producerSession.createQueue(destination));

            Message sendMessage = createTextMessage(producerSession);

            for (int i = 0; i < messageCount; i++) {

                jmsProducer.send(sendMessage);
                numberOfMessageSent++;

            }

            LOG.info(" Finished after producing : " + numberOfMessageSent);
            return numberOfMessageSent;

        } catch (Exception ex) {
            LOG.info("Exception received producing ", ex);
            LOG.info("finishing after exception :" + numberOfMessageSent);
            return numberOfMessageSent;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

    private TextMessage createTextMessage(Session session) throws JMSException {
        StringBuffer buffer = new StringBuffer();

        for (int i = 0; i < 1000; i++) {

            buffer.append("1234567890");
        }

        return session.createTextMessage(buffer.toString());

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

