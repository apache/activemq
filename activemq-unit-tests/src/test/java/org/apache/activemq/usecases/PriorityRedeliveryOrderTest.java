/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Sends X messages with a sequence number held in a JMS property "appId"
 * Uses all priority 4 message (normal priority)
 * closed the consumer connection multiple times so the already prefetched messages will be available
 * for dispatch again.
 */

public class PriorityRedeliveryOrderTest {

    private static final Logger LOG = LoggerFactory.getLogger(PriorityRedeliveryOrderTest.class);

    private static final String DESTINATION = "testQ1";
    private static final int MESSAGES_TO_SEND = 1000;
    private static final int MESSAGES_PER_CONSUMER = 200;
    private int consumedAppId = -1;
    private int totalConsumed;
    BrokerService broker;

    @Before
    public void createBroker() throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        pe.setPrioritizedMessages(true);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);


        broker.addConnector("tcp://0.0.0.0:0");

        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testMessageDeliveryOrderAfterPrefetch() throws Exception {

        //send X messages with with a sequence number number in the message property.
        sendMessages(MESSAGES_TO_SEND);

        for (int i = 0; i < (MESSAGES_TO_SEND / MESSAGES_PER_CONSUMER); i++) {
            totalConsumed += consumeMessages(MESSAGES_PER_CONSUMER);
        }
        assertEquals("number of messages consumed should be equal to number of messages sent", MESSAGES_TO_SEND, totalConsumed);
    }

    private Long sendMessages(int messageCount) throws Exception {

        long numberOfMessageSent = 0;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());

        Connection connection = connectionFactory.createConnection();
        connection.start();

        try {

            Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer jmsProducer = producerSession.createProducer(producerSession.createQueue(DESTINATION));

            Message sendMessage = producerSession.createTextMessage("test_message");

            for (int i = 0; i < messageCount; i++) {

                sendMessage.setIntProperty("appID", i);
                jmsProducer.send(sendMessage);
                producerSession.commit();
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

    /*
     Ensure messages are consumed in the expected sequence
     */

    private int consumeMessages(int numberOfMessage) throws Exception {

        LOG.info("Creating new consumer for:" + numberOfMessage);


        int numberConsumedMessage = 0;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();

        try {

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer jmsConsumer = session.createConsumer(session.createQueue(DESTINATION));
            boolean consume = true;


            while (consume) {

                Message message = jmsConsumer.receive(4000);

                if (message == null) {
                    LOG.info("Break on:" + numberConsumedMessage);
                    break;
                }


                int newAppId = message.getIntProperty("appID");

                numberConsumedMessage++;

                LOG.debug("Message newAppID" + newAppId);

                //check it is next appID in sequence

                if (newAppId != (consumedAppId + 1)) {
                    fail(" newAppId is " + newAppId + " expected " + (consumedAppId + 1));
                }

                //increase next AppID
                consumedAppId = newAppId;

                session.commit();

                if (numberConsumedMessage == numberOfMessage) {
                    LOG.info("closing consumer after 200 message, consumedAppID is " + consumedAppId);
                    return numberConsumedMessage;
                }

            }
        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) {

                }
            }
        }
        return numberConsumedMessage;
    }

}

