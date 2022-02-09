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
package org.apache.activemq.broker.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This unit test is to test that setting the property "sendDuplicateFromStoreToDLQ" on
 * PolicyEntry works correctly.
 *
 */
public class SendDuplicateFromStoreToDLQTest {
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();

        File testDataDir = new File("target/activemq-data/AMQ-8397");
        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024l * 1024 * 64);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File(testDataDir, "kahadb"));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors()
                .get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        connection.stop();
        connection.close();
        broker.deleteAllMessages();
        broker.stop();
    }

    @Test
    public void testSendDuplicateFromStoreToDLQTrue() throws Exception {
        applySendDuplicateFromStoreToDLQPolicy(true);
        doProcessSendDuplicateFromStoreToDLQ(true);
    }

    @Test
    public void testSendDuplicateFromStoreToDLQFalse() throws Exception {
        applySendDuplicateFromStoreToDLQPolicy(false);
        doProcessSendDuplicateFromStoreToDLQ(false);
    }

    protected void doProcessSendDuplicateFromStoreToDLQ(boolean enabled) throws Exception {
        createQueue("AMQ.8397");
        org.apache.activemq.broker.region.Queue queue = (org.apache.activemq.broker.region.Queue)broker.getDestination(new ActiveMQQueue("AMQ.8397"));
        assertEquals(Long.valueOf(0l), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
        assertEquals(Long.valueOf(0l), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));

        MessageConsumer messageConsumer = session.createConsumer(session.createQueue("AMQ.8397"));
        producer.send(session.createTextMessage("Hello world!"));
        
        int loopCount=0;
        int maxLoops=50;
        boolean found=false;
        Message recvMessage = null;
        do {
            recvMessage = messageConsumer.receive(200l);
            if(recvMessage != null) {
                found = true;
            }
            loopCount++;
        } while(!found && loopCount < maxLoops);

        assertNotNull(recvMessage);
        List<Subscription> queueSubscriptions = queue.getConsumers();
        assertNotNull(queueSubscriptions);
        assertEquals(Integer.valueOf(1), Integer.valueOf(queueSubscriptions.size()));
        queue.duplicateFromStore((org.apache.activemq.command.Message) recvMessage, queueSubscriptions.get(0));

        org.apache.activemq.broker.region.Queue dlq = (org.apache.activemq.broker.region.Queue)broker.getDestination(new ActiveMQQueue("ActiveMQ.DLQ.Queue.AMQ.8397"));
        
        if(enabled) {
            assertEquals(Long.valueOf(0l), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(1l), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));
            assertEquals(Long.valueOf(1l), Long.valueOf(dlq.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(0l), Long.valueOf(dlq.getDestinationStatistics().getDuplicateFromStore().getCount()));
        } else {
            assertEquals(Long.valueOf(0l), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(1l), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));
            assertEquals(Long.valueOf(0l), Long.valueOf(dlq.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(0l), Long.valueOf(dlq.getDestinationStatistics().getDuplicateFromStore().getCount()));
        }
    }

    private PolicyMap applySendDuplicateFromStoreToDLQPolicy(boolean enabled) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setSendDuplicateFromStoreToDLQ(enabled);
        defaultEntry.setDeadLetterStrategy(new IndividualDeadLetterStrategy());
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        return policyMap;
    }

    private void createQueue(String queueName) throws Exception {
        Queue queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
    }
}
