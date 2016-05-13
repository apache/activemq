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

    BrokerService broker = null;
    ActiveMQQueue dest = new ActiveMQQueue("Queue");

    @Test
    public void testSingleConsumerTxRepeat() throws Exception {

        publishMessages(100);

        consumeVerifyOrderRollback(20);
        consumeVerifyOrderRollback(10);
        consumeVerifyOrderRollback(5);
    }

    @Test
    public void testSingleSessionXConsumerTxRepeat() throws Exception {

        publishMessages(100);

        Connection connection = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString()).createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumeVerifyOrder(session, 20);
        session.rollback();
        consumeVerifyOrder(session, 10);
        session.rollback();
        consumeVerifyOrder(session, 5);
        session.commit();
    }

    private void consumeVerifyOrderRollback(final int num) throws Exception {
        Connection connection = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString()).createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumeVerifyOrder(session, num);
        session.rollback();
        connection.close();
    }

    private void consumeVerifyOrder(Session session, final int num) throws Exception {
        MessageConsumer messageConsumer = session.createConsumer(dest);
        for (int i=0; i<num; ) {
            Message message = messageConsumer.receive(4000);
            if (message != null) {
                assertEquals(i, message.getIntProperty("Order"));
                i++;
            }
        }
        messageConsumer.close();
    }

    private void publishMessages(int num) throws Exception {
        Connection connection = new ActiveMQConnectionFactory(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString()).createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer messageProducer = session.createProducer(dest);
        TextMessage textMessage = session.createTextMessage("A");
        for (int i=0; i<num; i++) {
            textMessage.setIntProperty("Order", i);
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
}
