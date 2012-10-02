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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AMQ2832Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2832Test.class);

    BrokerService broker = null;
    private final Destination destination = new ActiveMQQueue("AMQ2832Test");

    protected void startBroker(boolean delete) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:0");

        configurePersistence(broker, delete);

        broker.start();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService, boolean deleteAllOnStart) throws Exception {
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(500);
        adapter.setCleanupInterval(500);

        if (!deleteAllOnStart) {
            adapter.setForceRecoverIndex(true);
        }

    }

    @Test
    public void testAckRemovedMessageReplayedAfterRecovery() throws Exception {

        startBroker(true);

        StagedConsumer consumer = new StagedConsumer();
        int numMessagesAvailable = produceMessagesToConsumeMultipleDataFiles(20);
        // this will block the reclaiming of one data file
        Message firstUnacked = consumer.receive(10);
        LOG.info("first unacked: " + firstUnacked.getJMSMessageID());
        Message secondUnacked = consumer.receive(1);
        LOG.info("second unacked: " + secondUnacked.getJMSMessageID());
        numMessagesAvailable -= 11;

        numMessagesAvailable += produceMessagesToConsumeMultipleDataFiles(10);
        // ensure ack is another data file
        LOG.info("Acking firstUnacked: " + firstUnacked.getJMSMessageID());
        firstUnacked.acknowledge();

        numMessagesAvailable += produceMessagesToConsumeMultipleDataFiles(10);

        consumer.receive(numMessagesAvailable).acknowledge();

        // second unacked should keep first data file available but journal with the first ack
        // may get whacked
        consumer.close();

        broker.stop();
        broker.waitUntilStopped();

        startBroker(false);

        consumer = new StagedConsumer();     
        // need to force recovery?

        Message msg = consumer.receive(1, 5);
        assertNotNull("One messages left after recovery", msg);
        msg.acknowledge();

        // should be no more messages
        msg = consumer.receive(1, 5);
        assertEquals("Only one messages left after recovery: " + msg, null, msg);
        consumer.close();

    }

    private int produceMessagesToConsumeMultipleDataFiles(int numToSend) throws Exception {
        int sent = 0;
        Connection connection = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri()).createConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < numToSend; i++) {
                producer.send(createMessage(session, i));
                sent++;
            }
        } finally {
            connection.close();
        }
        
        return sent;
    }

    final String payload = new String(new byte[1024]);

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }

    private class StagedConsumer {
        Connection connection;
        MessageConsumer consumer;

        StagedConsumer() throws Exception {
            connection = new ActiveMQConnectionFactory("failover://" +
                    broker.getTransportConnectors().get(0).getConnectUri().toString()).createConnection();
            connection.start();
            consumer = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE).createConsumer(destination);
        }

        public Message receive(int numToReceive) throws Exception {
            return receive(numToReceive, 2);
        }

        public Message receive(int numToReceive, int timeoutInSeconds) throws Exception {
            Message msg = null;
            for (; numToReceive > 0; numToReceive--) {

                do  {
                    msg = consumer.receive(1*1000);
                } while (msg == null && --timeoutInSeconds > 0);

                if (numToReceive > 1) {
                    msg.acknowledge();
                }

                if (msg != null) {
                    LOG.debug("received: " + msg.getJMSMessageID());
                }
            }
            // last message, unacked
            return msg;
        }

        void close() throws JMSException {
            consumer.close();
            connection.close();
        }
    }
}
