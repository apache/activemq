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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This unit test is to test that setting the property "sendDuplicateFromStoreToDLQ" on
 * PolicyEntry works correctly with and without cache enabled.
 *
 */
@RunWith(value = Parameterized.class)
public class SendDuplicateFromStoreToDLQTest {

    // [AMQ-9811] Async ack can lead to test instability
    private static final Boolean FORCE_CONCURRENTSTOREANDDISPATCHQUEUES = Boolean.FALSE;

    @Parameterized.Parameters(name="sendDupToDLQ={1},cacheEnable={2},auditEnabled={3},optimizedDispatch={4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"dup+cache+aud+opt", true, true, true, true},
                {"dup+cache+aud+!opt", true, true, true, false},
                {"dup+cache+!aud+opt", true, true, false, true},
                {"dup+cache+!aud+!opt", true, true, false, false},
                {"dup+!cache+aud+opt", true, false, true, true},
                {"dup+!cache+aud+!opt", true, false, true, false},
                {"dup+!cache+!aud+opt", true, false, false, true},
                {"dup+!cache+!aud+!opt", true, false, false, false},
                {"!dup+cache+aud+opt", false, true, true, true},
                {"!dup+cache+aud+!opt", false, true, true, false},
                {"!dup+cache+!aud+opt", false, true, false, true},
                {"!dup+cache+!aud+!opt", false, true, false, false},
                {"!dup+!cache+aud+opt", false, false, true, true},
                {"!dup+!cache+aud+!opt", false, false, true, false},
                {"!dup+!cache+!aud+opt", false, false, false, true},
                {"!dup+!cache+!aud+!opt", false, false, false, false},
                {"default", null, null, null, null},
        });
    }

    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;

    private final String testName;
    private final Boolean sendDuplicateFromStoreToDLQEnabled;
    private final Boolean useCacheEnabled;
    private final Boolean auditEnabled;
    private final Boolean optimizedDispatch;

    public SendDuplicateFromStoreToDLQTest(String testName, Boolean sendDuplicateFromStoreToDLQEnabled, Boolean useCacheEnabled, Boolean auditEnabled, Boolean optimizedDispatch) {
        this.testName = testName;
        this.sendDuplicateFromStoreToDLQEnabled = sendDuplicateFromStoreToDLQEnabled;
        this.useCacheEnabled = useCacheEnabled;
        this.auditEnabled = auditEnabled;
        this.optimizedDispatch = optimizedDispatch;
    }

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
        if(FORCE_CONCURRENTSTOREANDDISPATCHQUEUES != null) {
            persistenceAdapter.setConcurrentStoreAndDispatchQueues(FORCE_CONCURRENTSTOREANDDISPATCHQUEUES);
        }
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

    @Test(timeout = 15000)
    public void testSendDuplicateFromStoreToDLQScenario() throws Exception {
        applyPolicy(sendDuplicateFromStoreToDLQEnabled, useCacheEnabled, auditEnabled, optimizedDispatch);
        boolean sendExpected = (sendDuplicateFromStoreToDLQEnabled != null ? sendDuplicateFromStoreToDLQEnabled : new PolicyEntry().isSendDuplicateFromStoreToDLQ());
        doProcessSendDuplicateFromStoreToDLQ(sendExpected);
    }

    protected void doProcessSendDuplicateFromStoreToDLQ(boolean sendExpected) throws Exception {
        createQueue("AMQ.8397");
        org.apache.activemq.broker.region.Queue queue = (org.apache.activemq.broker.region.Queue)broker.getDestination(new ActiveMQQueue("AMQ.8397"));
        assertEquals(Long.valueOf(0L), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
        assertEquals(Long.valueOf(0L), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));

        MessageConsumer messageConsumer = session.createConsumer(session.createQueue("AMQ.8397"));
        producer.send(session.createTextMessage("Hello world!"));

        int loopCount=0;
        int maxLoops=50;
        boolean found=false;
        Message recvMessage = null;
        do {
            recvMessage = messageConsumer.receive(200L);
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

        if(sendExpected) {
            assertEquals(Long.valueOf(0L), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(1L), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));
            assertEquals(Long.valueOf(1L), Long.valueOf(dlq.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(0L), Long.valueOf(dlq.getDestinationStatistics().getDuplicateFromStore().getCount()));
        } else {
            // [AMQ-9811] Retry and delay a bit to allow value to be reflected
            int maxRetries = 200;
            int retryCount = 0;

            while(queue.getDestinationStatistics().getMessages().getCount() != 0L && retryCount < maxRetries) {
                retryCount++;
                Thread.sleep(50L);
            }
            assertEquals(Long.valueOf(0L), Long.valueOf(queue.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(1L), Long.valueOf(queue.getDestinationStatistics().getDuplicateFromStore().getCount()));
            assertEquals(Long.valueOf(0L), Long.valueOf(dlq.getDestinationStatistics().getMessages().getCount()));
            assertEquals(Long.valueOf(0L), Long.valueOf(dlq.getDestinationStatistics().getDuplicateFromStore().getCount()));
        }
    }

    private void applyPolicy(Boolean sendDuplicateFromStoreToDLQEnabled, Boolean useCacheEnabled, Boolean auditEnabled, Boolean optimizedDispatch) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();

        if(sendDuplicateFromStoreToDLQEnabled != null) {
            defaultEntry.setSendDuplicateFromStoreToDLQ(sendDuplicateFromStoreToDLQEnabled);
        }
        if(useCacheEnabled != null) {
            defaultEntry.setUseCache(useCacheEnabled);
        }
        if(auditEnabled != null) {
            defaultEntry.setEnableAudit(auditEnabled);
        }
        if(optimizedDispatch != null) {
            defaultEntry.setOptimizedDispatch(optimizedDispatch);
        }

        defaultEntry.setDeadLetterStrategy(new IndividualDeadLetterStrategy());
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
    }

    private void createQueue(String queueName) throws Exception {
        Queue queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
    }
}
