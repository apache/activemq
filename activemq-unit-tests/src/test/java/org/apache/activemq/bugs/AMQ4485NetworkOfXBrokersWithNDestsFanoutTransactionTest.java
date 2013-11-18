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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.TimeUtils;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4485NetworkOfXBrokersWithNDestsFanoutTransactionTest extends JmsMultipleBrokersTestSupport {
    static final String payload = new String(new byte[10 * 1024]);
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4485NetworkOfXBrokersWithNDestsFanoutTransactionTest.class);
    final int portBase = 61600;
    final int numBrokers = 4;
    final int numProducers = 10;
    final int numMessages = 800;
    final int consumerSleepTime = 20;
    StringBuilder brokersUrl = new StringBuilder();
    HashMap<ActiveMQQueue, AtomicInteger> accumulators = new HashMap<ActiveMQQueue, AtomicInteger>();
    private ArrayList<Throwable> exceptions = new ArrayList<Throwable>();

    protected void buildUrlList() throws Exception {
        for (int i = 0; i < numBrokers; i++) {
            brokersUrl.append("tcp://localhost:" + (portBase + i));
            if (i != numBrokers - 1) {
                brokersUrl.append(',');
            }
        }
    }

    protected BrokerService createBroker(int brokerid) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getManagementContext().setCreateConnector(false);


        broker.setUseJmx(true);
        broker.setBrokerName("B" + brokerid);
        broker.addConnector(new URI("tcp://localhost:" + (portBase + brokerid)));

        addNetworkConnector(broker);
        broker.setSchedulePeriodForDestinationPurge(0);
        broker.getSystemUsage().setSendFailIfNoSpace(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(512 * 1024 * 1024);


        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        policyEntry.setQueuePrefetch(1000);
        policyEntry.setMemoryLimit(1024 * 1024l);
        policyEntry.setOptimizedDispatch(false);
        policyEntry.setProducerFlowControl(false);
        policyEntry.setEnableAudit(true);
        policyEntry.setUseCache(true);
        policyMap.put(new ActiveMQQueue("GW.>"), policyEntry);
        broker.setDestinationPolicy(policyMap);

        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setConcurrentStoreAndDispatchQueues(false);

        brokers.put(broker.getBrokerName(), new BrokerItem(broker));
        return broker;
    }

    private void addNetworkConnector(BrokerService broker) throws Exception {
        StringBuilder networkConnectorUrl = new StringBuilder("static:(").append(brokersUrl.toString());
        networkConnectorUrl.append(')');

        for (int i = 0; i < 2; i++) {
            NetworkConnector nc = new DiscoveryNetworkConnector(new URI(networkConnectorUrl.toString()));
            nc.setName("Bridge-" + i);
            nc.setNetworkTTL(1);
            nc.setDecreaseNetworkConsumerPriority(true);
            nc.setDynamicOnly(true);
            nc.setPrefetchSize(100);
            nc.setDynamicallyIncludedDestinations(
                    Arrays.asList(new ActiveMQDestination[]{new ActiveMQQueue("GW.*")}));
            broker.addNetworkConnector(nc);
        }
    }

    public void testBrokers() throws Exception {

        buildUrlList();

        for (int i = 0; i < numBrokers; i++) {
            createBroker(i);
        }

        startAllBrokers();
        waitForBridgeFormation(numBrokers - 1);

        verifyPeerBrokerInfos(numBrokers - 1);


        final List<ConsumerState> consumerStates = startAllGWConsumers(numBrokers);

        startAllGWFanoutConsumers(numBrokers);

        LOG.info("Waiting for percolation of consumers..");
        TimeUnit.SECONDS.sleep(5);

        LOG.info("Produce mesages..");
        long startTime = System.currentTimeMillis();

        // produce
        produce(numMessages);

        assertTrue("Got all sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                for (ConsumerState tally : consumerStates) {
                    final int expected = numMessages * (tally.destination.isComposite() ? tally.destination.getCompositeDestinations().length : 1);
                    LOG.info("Tally for: " + tally.brokerName + ", dest: " + tally.destination + " - " + tally.accumulator.get());
                    if (tally.accumulator.get() != expected) {
                        LOG.info("Tally for: " + tally.brokerName + ", dest: " + tally.destination + " - " + tally.accumulator.get() + " != " + expected + ", " + tally.expected);
                        return false;
                    }
                    LOG.info("got tally on " + tally.brokerName);
                }
                return true;
            }
        }, 1000 * 60 * 1000l));

        assertTrue("No exceptions:" + exceptions, exceptions.isEmpty());

        LOG.info("done");
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Duration:" + TimeUtils.printDuration(duration));
    }

    private void startAllGWFanoutConsumers(int nBrokers) throws Exception {

        StringBuffer compositeDest = new StringBuffer();
        for (int k = 0; k < nBrokers; k++) {
            compositeDest.append("GW." + k);
            if (k + 1 != nBrokers) {
                compositeDest.append(',');
            }
        }
        ActiveMQQueue compositeQ = new ActiveMQQueue(compositeDest.toString());

        for (int id = 0; id < nBrokers; id++) {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:" + (portBase + id) + ")");
            connectionFactory.setWatchTopicAdvisories(false);

            QueueConnection queueConnection = connectionFactory.createQueueConnection();
            queueConnection.start();

            final QueueSession queueSession = queueConnection.createQueueSession(true, Session.SESSION_TRANSACTED);

            final MessageProducer producer = queueSession.createProducer(compositeQ);
            queueSession.createReceiver(new ActiveMQQueue("IN")).setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        producer.send(message);
                        queueSession.commit();
                    } catch (Exception e) {
                        LOG.error("Failed to fanout to GW: " + message, e);
                    }

                }
            });
        }
    }

    private List<ConsumerState> startAllGWConsumers(int nBrokers) throws Exception {
        List<ConsumerState> consumerStates = new LinkedList<ConsumerState>();
        for (int id = 0; id < nBrokers; id++) {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:" + (portBase + id) + ")");
            connectionFactory.setWatchTopicAdvisories(false);

            QueueConnection queueConnection = connectionFactory.createQueueConnection();
            queueConnection.start();

            final QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            ActiveMQQueue destination = new ActiveMQQueue("GW." + id);
            QueueReceiver queueReceiver = queueSession.createReceiver(destination);

            final ConsumerState consumerState = new ConsumerState();
            consumerState.brokerName = ((ActiveMQConnection) queueConnection).getBrokerName();
            consumerState.receiver = queueReceiver;
            consumerState.destination = destination;
            for (int j = 0; j < numMessages * (consumerState.destination.isComposite() ? consumerState.destination.getCompositeDestinations().length : 1); j++) {
                consumerState.expected.add(j);
            }

            if (!accumulators.containsKey(destination)) {
                accumulators.put(destination, new AtomicInteger(0));
            }
            consumerState.accumulator = accumulators.get(destination);

            queueReceiver.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        if (consumerSleepTime > 0) {
                            TimeUnit.MILLISECONDS.sleep(consumerSleepTime);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        consumerState.accumulator.incrementAndGet();
                        try {
                            consumerState.expected.remove(((ActiveMQMessage) message).getProperty("NUM"));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to commit slow receipt of " + message, e);
                    }
                }
            });

            consumerStates.add(consumerState);

        }
        return consumerStates;
    }

    private void produce(int numMessages) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(numProducers);
        final AtomicInteger toSend = new AtomicInteger(numMessages);
        for (int i = 1; i <= numProducers; i++) {
            final int id = i % numBrokers;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:" + (portBase + id) + ")");
                        connectionFactory.setWatchTopicAdvisories(false);
                        QueueConnection queueConnection = connectionFactory.createQueueConnection();
                        queueConnection.start();
                        QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer = queueSession.createProducer(null);
                        int val = 0;
                        while ((val = toSend.decrementAndGet()) >= 0) {

                            ActiveMQQueue compositeQ = new ActiveMQQueue("IN");
                            LOG.info("Send to: " + ((ActiveMQConnection) queueConnection).getBrokerName() + ", " + val + ", dest:" + compositeQ);
                            Message textMessage = queueSession.createTextMessage(((ActiveMQConnection) queueConnection).getBrokerName() + "->" + val + " payload:" + payload);
                            textMessage.setIntProperty("NUM", val);
                            producer.send(compositeQ, textMessage);
                        }
                        queueConnection.close();

                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                        exceptions.add(throwable);
                    }
                }
            });
        }
    }

    private void verifyPeerBrokerInfo(BrokerItem brokerItem, final int max) throws Exception {
        final BrokerService broker = brokerItem.broker;
        final RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
                return max == regionBroker.getPeerBrokerInfos().length;
            }
        });
        LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
        List<String> missing = new ArrayList<String>();
        for (int i = 0; i < max; i++) {
            missing.add("B" + i);
        }
        if (max != regionBroker.getPeerBrokerInfos().length) {
            for (BrokerInfo info : regionBroker.getPeerBrokerInfos()) {
                LOG.info(info.getBrokerName());
                missing.remove(info.getBrokerName());
            }
            LOG.info("Broker infos off.." + missing);
        }
        assertEquals(broker.getBrokerName(), max, regionBroker.getPeerBrokerInfos().length);
    }

    private void verifyPeerBrokerInfos(final int max) throws Exception {
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext(); ) {
            verifyPeerBrokerInfo(i.next(), max);
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    class ConsumerState {
        AtomicInteger accumulator;
        String brokerName;
        QueueReceiver receiver;
        ActiveMQDestination destination;
        Vector<Integer> expected = new Vector<Integer>();
    }
}
