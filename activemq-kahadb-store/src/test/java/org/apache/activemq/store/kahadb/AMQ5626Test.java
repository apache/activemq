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

package org.apache.activemq.store.kahadb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AMQ5626Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5626Test.class);

    private final String QUEUE_NAME = "TesQ";
    private final String KAHADB_DIRECTORY = "target/activemq-data/";

    private BrokerService brokerService;
    private URI brokerUri;

    @Before
    public void setup() throws Exception {
        createBroker(true);
    }

    @After
    public void teardown() throws Exception {
        try {
            brokerService.stop();
        } catch (Exception ex) {
            LOG.error("FAILED TO STOP/START BROKER EXCEPTION", ex);
        }
    }

    private void createBroker(boolean deleteMessagesOnStart) throws Exception {

        brokerService = new BrokerService();

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();
        pe.setPrioritizedMessages(true);
        pe.setExpireMessagesPeriod(0);

        pe.setQueue(QUEUE_NAME);
        entries.add(pe);

        policyMap.setPolicyEntries(entries);

        brokerService.setDestinationPolicy(policyMap);

        TransportConnector transportConnector = new TransportConnector();
        transportConnector.setName("openwire");
        transportConnector.setUri(new URI("tcp://0.0.0.0:0"));
        brokerService.addConnector(transportConnector);
        brokerService.setDataDirectory(KAHADB_DIRECTORY);
        brokerService.setDeleteAllMessagesOnStartup(deleteMessagesOnStart);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        brokerUri = transportConnector.getPublishableConnectURI();
    }

    @Test(timeout = 30000)
    public void testPriorityMessages() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUri);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
        Message message = session.createMessage();

        // 0,1
        producer.setPriority(9);
        producer.send(message);
        producer.send(message);

        // 2,3
        producer.setPriority(4);
        producer.send(message);
        producer.send(message);

        connection.close();

        stopRestartBroker();

        connectionFactory = new ActiveMQConnectionFactory(brokerUri);
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue(QUEUE_NAME));

        // 4
        producer.setPriority(4);
        producer.send(message);

        displayQueueViews(brokerService);

        // consume 5
        MessageConsumer jmsConsumer = session.createConsumer(session.createQueue(QUEUE_NAME));
        for (int i = 0; i < 5; i++) {
            message = jmsConsumer.receive(4000);
            assertNotNull("Got message i=" + i, message);
            LOG.info("received: " + message.getJMSMessageID() + ", priority:" + message.getJMSPriority());
        }

        connection.close();
    }

    private void displayQueueViews(BrokerService broker) throws Exception {
        Map<ObjectName, DestinationView> queueViews = broker.getAdminView().getBroker().getQueueViews();

        for (ObjectName key : queueViews.keySet()) {
            DestinationView destinationView = queueViews.get(key);

            if (destinationView instanceof QueueView) {
                QueueView queueView = (QueueView) destinationView;
                LOG.info("ObjectName " + key);
                LOG.info("QueueView name : " + queueView.getName());
                LOG.info("QueueView cursorSize : " + queueView.cursorSize());
                LOG.info("QueueView queueSize : " + queueView.getQueueSize());
                LOG.info("QueueView enqueue count : " + queueView.getEnqueueCount());
                LOG.info("QueueView dequeue count : " + queueView.getDequeueCount());
                LOG.info("QueueView inflight count : " + queueView.getInFlightCount());
            }
        }
    }

    private synchronized void stopRestartBroker() {
        try {
            LOG.info(">>>SHUTTING BROKER DOWN");
            brokerService.stop();
            brokerService.waitUntilStopped();

            //restart it
            createBroker(false);
            brokerService.start();
            brokerService.waitUntilStarted();

            LOG.info(">>>BROKER RESTARTED..");
        } catch (Exception e) {
            LOG.error("FAILED TO STOP/START BROKER EXCEPTION", e);
            fail("FAILED TO STOP/START BROKER" + e);
        }
    }
}