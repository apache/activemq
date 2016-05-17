/*
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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueueOrderSingleTransactedConsumerTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueOrderSingleTransactedConsumerTest.class);

    BrokerService broker = null;
    ActiveMQQueue dest = new ActiveMQQueue("Queue");

    @Test
    public void testSingleConsumerTxRepeat() throws Exception {

        // effect the broker sequence id that is region wide
        ActiveMQQueue dummyDest = new ActiveMQQueue("AnotherQueue");
        publishMessagesWithOrderProperty(10, 0, dest);
        publishMessagesWithOrderProperty(1, 0, dummyDest);

        publishMessagesWithOrderProperty(10, 10, dest);
        publishMessagesWithOrderProperty(1, 0, dummyDest);

        publishMessagesWithOrderProperty(10, 20, dest);
        publishMessagesWithOrderProperty(1, 0, dummyDest);

        publishMessagesWithOrderProperty(5, 30, dest);

        consumeVerifyOrderRollback(20);
        consumeVerifyOrderRollback(10);
        consumeVerifyOrderRollback(5);
    }

    @Test
    public void testSingleSessionXConsumerTxRepeat() throws Exception {

        publishMessagesWithOrderProperty(50);

        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = consumeVerifyOrder(session, 20);
        messageConsumer.close();
        session.rollback();
        messageConsumer = consumeVerifyOrder(session, 10);
        messageConsumer.close();
        session.rollback();
        messageConsumer = consumeVerifyOrder(session, 5);
        messageConsumer.close();
        session.commit();
        connection.close();
    }

    @Test
    public void tesXConsumerTxRepeat() throws Exception {

        publishMessagesWithOrderProperty(10);

        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = consumeVerifyOrder(session, 6);
        messageConsumer.close();
        messageConsumer = consumeVerifyOrder(session, 4, 6);

        // rollback before close, so there are two consumers in the mix
        session.rollback();

        messageConsumer.close();

        messageConsumer = consumeVerifyOrder(session, 10);
        session.commit();
        messageConsumer.close();
        connection.close();
    }

    @Test
    public void testSingleTxXConsumerTxRepeat() throws Exception {

        publishMessagesWithOrderProperty(10);

        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = consumeVerifyOrder(session, 6);
        messageConsumer.close();
        messageConsumer = consumeVerifyOrder(session, 4, 6);
        messageConsumer.close();

        session.rollback();
        messageConsumer = consumeVerifyOrder(session, 10);
        session.commit();
        messageConsumer.close();
        connection.close();
    }

    private void consumeVerifyOrderRollback(final int num) throws Exception {
        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = consumeVerifyOrder(session, num);
        messageConsumer.close();
        session.rollback();
        connection.close();
    }

    private MessageConsumer consumeVerifyOrder(Session session, final int num) throws Exception {
        return consumeVerifyOrder(session, num, 0);
    }

    private MessageConsumer consumeVerifyOrder(Session session, final int num, final int base) throws Exception {
        MessageConsumer messageConsumer = session.createConsumer(dest);
        for (int i=0; i<num; ) {
            Message message = messageConsumer.receive(4000);
            if (message != null) {
                assertEquals(i + base, message.getIntProperty("Order"));
                i++;
                LOG.debug("Received:" + message.getJMSMessageID() + ", Order: " + message.getIntProperty("Order"));
            }
        }
        return messageConsumer;
    }

    private void publishMessagesWithOrderProperty(int num) throws Exception {
        publishMessagesWithOrderProperty(num, 0, dest);
    }

    private void publishMessagesWithOrderProperty(int num, int seqStart, ActiveMQQueue destination) throws Exception {
        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer messageProducer = session.createProducer(destination);
        TextMessage textMessage = session.createTextMessage("A");
        for (int i=0; i<num; i++) {
            textMessage.setIntProperty("Order", i + seqStart);
            messageProducer.send(textMessage);
        }
    }

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        // add the policy entries
        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();
        pe.setExpireMessagesPeriod(0);

        pe.setQueuePrefetch(0); // make incremental dispatch to the consumers explicit
        pe.setStrictOrderDispatch(true);  // force redeliveries back to the head of the queue

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }


    private ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
    }

}
