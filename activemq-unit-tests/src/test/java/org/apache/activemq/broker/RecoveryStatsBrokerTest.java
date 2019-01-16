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

package org.apache.activemq.broker;

import com.google.common.collect.ImmutableList;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.store.MessageStoreStatistics;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@RunWith(value = Parameterized.class)
public class RecoveryStatsBrokerTest extends BrokerRestartTestSupport {

    private RestartType restartType;

    enum RestartType {
        NORMAL,
        FULL_RECOVERY,
        UNCLEAN_SHUTDOWN
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setJournalMaxFileLength(1024*20);
        //persistenceAdapter.setConcurrentStoreAndDispatchQueues(false);
        persistenceAdapter.setDirectory(broker.getBrokerDataDirectory());
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setDestinationPolicy(policyMap);
    }

    protected void restartBroker(RestartType restartType) throws Exception {
        if (restartType == RestartType.FULL_RECOVERY) {
            stopBroker();
            KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
            File dir = kahaDBPersistenceAdapter.getDirectory();
            if (dir != null) {
                IOHelper.deleteFile(new File(dir, "db.data"));
            }
            broker.start();
        } else if (restartType == RestartType.UNCLEAN_SHUTDOWN){
            //Simulate an unclean  shutdown

            File dir = broker.getBrokerDataDirectory();
            File backUpDir = new File(dir, "bk");
            IOHelper.mkdirs(new File(dir, "bk"));

            for (File f: dir.listFiles()) {
                if (!f.isDirectory()) {
                    IOHelper.copyFile(f, new File(backUpDir, f.getName()));
                }
            }

            stopBroker();

            for (File f: backUpDir.listFiles()) {
                if (!f.isDirectory()) {
                    IOHelper.copyFile(f, new File(dir, f.getName()));
                }
            }

            broker.start();
        } else {
            restartBroker();
        }
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][] {
                {RestartType.NORMAL},
                {RestartType.FULL_RECOVERY},
                {RestartType.UNCLEAN_SHUTDOWN},
        });
    }

    public RecoveryStatsBrokerTest(RestartType restartType) {
        this.restartType = restartType;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test(timeout = 60 * 1000)
    public void testStaticsRecovery() throws Exception {
        List<ActiveMQDestination> destinations = ImmutableList.of(new ActiveMQQueue("TEST.A"), new ActiveMQQueue("TEST.B"));
        Random random = new Random();
        Map<ActiveMQDestination, Integer> consumedMessages = new HashMap<>();

        destinations.forEach(destination -> consumedMessages.put(destination, 0));

        int numberOfMessages = 400;

        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        for (int i = 0; i < numberOfMessages; i++) {
            for (ActiveMQDestination destination : destinations) {
                Message message = createMessage(producerInfo, destination);
                message.setPersistent(true);
                message.setProducerId(message.getMessageId().getProducerId());
                connection.request(message);
            }
        }

        Map<ActiveMQDestination, MessageStoreStatistics> originalStatistics = getCurrentStatistics(destinations);

        checkStatistics(destinations, originalStatistics);

        restartBroker(restartType);

        checkStatistics(destinations, originalStatistics);

        for (ActiveMQDestination destination : destinations) {
            consume(destination, 100, false);
        }

        checkStatistics(destinations, originalStatistics);

        restartBroker(restartType);

        checkStatistics(destinations, originalStatistics);

        for (ActiveMQDestination destination : destinations) {
            int messagesToConsume = random.nextInt(numberOfMessages);
            consume(destination, messagesToConsume, true);
            consumedMessages.compute(destination, (key, value) -> value = value + messagesToConsume);
        }

        originalStatistics = getCurrentStatistics(destinations);

        for (ActiveMQDestination destination : destinations) {
            int consumedCount = consumedMessages.get(destination);
            assertEquals("",  numberOfMessages - consumedCount, originalStatistics.get(destination).getMessageCount().getCount());
        }

        checkStatistics(destinations, originalStatistics);

        restartBroker(restartType);

        checkStatistics(destinations, originalStatistics);
    }

    private Map<ActiveMQDestination, MessageStoreStatistics> getCurrentStatistics(final List<ActiveMQDestination> destinations) {
        return destinations.stream()
                .map(destination -> getDestination(broker, destination))
                .collect(Collectors.toMap(destination -> new ActiveMQQueue(destination.getName()), destination2 -> destination2.getMessageStore().getMessageStoreStatistics()));
    }

    private void consume(ActiveMQDestination destination, int numberOfMessages, boolean shouldAck) throws Exception {
        // Setup the consumer and receive the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // The we should get the messages.
        for (int i = 0; i < numberOfMessages; i++) {
            Message m2 = receiveMessage(connection);
            assertNotNull(m2);
            if (shouldAck) {
                MessageAck ack = createAck(consumerInfo, m2, 1, MessageAck.STANDARD_ACK_TYPE);
                connection.request(ack);
            }
        }

        connection.request(closeConnectionInfo(connectionInfo));
    }

    private void checkStatistics(final List<ActiveMQDestination> destinations, final Map<ActiveMQDestination, MessageStoreStatistics> originalStatistics) {
        for (ActiveMQDestination destination : destinations) {
            MessageStoreStatistics original = originalStatistics.get(destination);
            MessageStoreStatistics actual = getDestination(broker, destination).getMessageStore().getMessageStoreStatistics();
            assertEquals("Have Same Count", original.getMessageCount().getCount(), actual.getMessageCount().getCount());
            assertEquals("Have Same TotalSize", original.getMessageSize().getTotalSize(), getDestination(broker, destination).getMessageStore().getMessageStoreStatistics().getMessageSize().getTotalSize());
        }
    }

    protected Destination getDestination(BrokerService target, ActiveMQDestination destination) {
        RegionBroker regionBroker = (RegionBroker) target.getRegionBroker();
        if (destination.isTemporary()) {
            return destination.isQueue() ? regionBroker.getTempQueueRegion().getDestinationMap().get(destination) :
                    regionBroker.getTempTopicRegion().getDestinationMap().get(destination);
        }
        return destination.isQueue() ?
                regionBroker.getQueueRegion().getDestinationMap().get(destination) :
                regionBroker.getTopicRegion().getDestinationMap().get(destination);
    }
}
