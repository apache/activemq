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
package org.apache.activemq.broker.virtual;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * This test will determine that the producer flow control does not kick in.
 * The original MirroredQueue implementation was causing the queue to update
 * the topic memory usage instead of the queue memory usage.
 * The reason is that the message memory usage instance will not be updated
 * unless it is null.  This was the case when the message was initially sent
 * to the topic but then it was non-null when it was being sent to the queue.
 * When the region destination was set, the associated memory usage was not
 * updated to the passed queue destination and thus the memory usage of the
 * topic was being updated instead.
 *
 * @author Claudio Corsi
 */
public class MirroredQueueCorrectMemoryUsageTest extends EmbeddedBrokerTestSupport {

    private static final Logger logger = LoggerFactory.getLogger(MirroredQueueCorrectMemoryUsageTest.class);

    private static final long ONE_MB = 0x0100000;
    private static final long TEN_MB = ONE_MB * 10;
    private static final long TWENTY_MB = TEN_MB * 2;

    private static final String CREATED_STATIC_FOR_PERSISTENT = "created.static.for.persistent";

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        // Create the broker service instance....
        BrokerService broker = super.createBroker();
        // Create and add the mirrored queue destination interceptor ....
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[1];
        MirroredQueue mq = new MirroredQueue();
        mq.setCopyMessage(true);
        mq.setPrefix("");
        mq.setPostfix(".qmirror");
        destinationInterceptors[0] = mq;
        broker.setDestinationInterceptors(destinationInterceptors);
        // Create the destination policy for the topics and queues
        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new LinkedList<PolicyEntry>();
        // Create Topic policy entry
        PolicyEntry policyEntry = new PolicyEntry();
        super.useTopic = true;
        ActiveMQDestination destination = super.createDestination(">");
        Assert.isTrue(destination.isTopic(), "Created destination was not a topic");
        policyEntry.setDestination(destination);
        policyEntry.setProducerFlowControl(true);
        policyEntry.setMemoryLimit(ONE_MB); // x10
        entries.add(policyEntry);
        // Create Queue policy entry
        policyEntry = new PolicyEntry();
        super.useTopic = false;
        destination = super.createDestination(CREATED_STATIC_FOR_PERSISTENT);
        Assert.isTrue(destination.isQueue(), "Created destination was not a queue");
        policyEntry.setDestination(destination);
        policyEntry.setProducerFlowControl(true);
        policyEntry.setMemoryLimit(TEN_MB);
        entries.add(policyEntry);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);
        // Set destinations
        broker.setDestinations(new ActiveMQDestination[] { destination });
        // Set system usage
        SystemUsage memoryManager = new SystemUsage();
        MemoryUsage memoryUsage = new MemoryUsage();
        memoryUsage.setLimit(TEN_MB);
        memoryManager.setMemoryUsage(memoryUsage);
        StoreUsage storeUsage = new StoreUsage();
        storeUsage.setLimit(TWENTY_MB);
        memoryManager.setStoreUsage(storeUsage);
        TempUsage tempDiskUsage = new TempUsage();
        tempDiskUsage.setLimit(TEN_MB);
        memoryManager.setTempUsage(tempDiskUsage);
        broker.setSystemUsage(memoryManager);
        // Set the persistent adapter
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setJournalMaxFileLength((int)TEN_MB);
        // Delete all current messages...
        IOHelper.deleteFile(persistenceAdapter.getDirectory());
        broker.setPersistenceAdapter(persistenceAdapter);
        return broker;
    }

    @Before
    protected void setUp() throws Exception {
        super.setUp();
    }

    @After
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Test(timeout=40000)
    public void testNoMemoryUsageIncreaseForTopic() throws Exception {
        Connection connection = super.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(CREATED_STATIC_FOR_PERSISTENT);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            char[] m = new char[1024];
            Arrays.fill(m, 'x');
            // create some messages that have 1k each
            for (int i = 1; i < 12000; i++) {
                 producer.send(session.createTextMessage(new String(m)));
                 logger.debug("Sent message: " + i);
            }
            producer.close();
            session.close();
            connection.stop();
            connection.close();
    }
}
