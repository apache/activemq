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

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.store.jdbc.DataSourceServiceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ6122Test {

    private BrokerService brokerService;
    private EmbeddedDataSource embeddedDataSource;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.deleteAllMessages();

        // turn off the cache
        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        pe.setUseCache(false);
        pe.setExpireMessagesPeriod(0);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        brokerService.setDestinationPolicy(policyMap);

        embeddedDataSource = (EmbeddedDataSource) DataSourceServiceSupport.createDataSource(IOHelper.getDefaultDataDirectory());
        embeddedDataSource.setCreateDatabase("create");
        embeddedDataSource.getConnection().close();

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(embeddedDataSource);
        brokerService.setPersistenceAdapter(jdbc);

        jdbc.deleteAllMessages();

        brokerService.addConnector("tcp://localhost:0");
        brokerService.setAdvisorySupport(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }

        if (embeddedDataSource != null) {
            DataSourceServiceSupport.shutdownDefaultDataSource(embeddedDataSource);
        }
    }

    @Test
    public void deadlockOnDuplicateInDLQ() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString()
            + "?wireFormat.maxInactivityDuration=0");
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setWatchTopicAdvisories(false);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
        activeMQConnection.start();
        ActiveMQSession activeMQSession = (ActiveMQSession) activeMQConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        ActiveMQQueue dest = new ActiveMQQueue("ActiveMQ.DLQ");

        ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) activeMQSession.createProducer(dest);
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setDestination(dest);
        activeMQMessageProducer.send(message, null);

        // send a duplicate
        activeMQConnection.syncSendPacket(message);

        // send another "real" message. block the send on the broker (use
        // asyncSend to allow client to continue)
        message.setCorrelationId("blockme");
        SendTask sendTask = new SendTask(activeMQMessageProducer, message);
        new Thread(sendTask).start();

        // create consumer to trigger fill batch (no cache)
        // release the previous send.
        ActiveMQConnection connectionForConsumer = (ActiveMQConnection) connectionFactory.createConnection();
        connectionForConsumer.start();
        ActiveMQSession sessionForConsumer = (ActiveMQSession) connectionForConsumer.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer messageConsumer = sessionForConsumer.createConsumer(dest);

        Message received = messageConsumer.receive();
        assertNotNull("Got message", received);
        messageConsumer.close();

        activeMQConnection.close();
    }

    class SendTask implements Runnable {

        private final Message message;
        private final ActiveMQMessageProducer producer;

        SendTask(ActiveMQMessageProducer producer, Message message) {
            this.producer = producer;
            this.message = message;
        }

        @Override
        public void run() {
            try {
                producer.send(message, null);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
