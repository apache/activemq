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


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.ByteSequence;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class VirtualDestPerfTest {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualDestPerfTest.class);
    public int messageSize = 5*1024;
    public int messageCount = 10000;
    ActiveMQTopic target = new ActiveMQTopic("target");
    BrokerService brokerService;
    ActiveMQConnectionFactory connectionFactory;

    @Test
    @Ignore("comparison test - 'new' no wait on future with async send broker side is always on")
    public void testAsyncSendBurstToFillCache() throws Exception {
        startBroker(4, true, true);
        connectionFactory.setUseAsyncSend(true);

        // a burst of messages to fill the cache
        messageCount = 22000;
        messageSize = 10*1024;

        LinkedHashMap<Integer, Long> results = new LinkedHashMap<Integer, Long>();

        final ActiveMQQueue queue = new ActiveMQQueue("targetQ");
        for (Integer numThreads : new Integer[]{1, 2}) {
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            final AtomicLong numMessagesToSend = new AtomicLong(messageCount);
            purge();
            long startTime = System.currentTimeMillis();
            for (int i=0;i<numThreads;i++) {
                executor.execute(new Runnable(){
                    @Override
                    public void run() {
                        try {
                            produceMessages(numMessagesToSend, queue);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            long endTime = System.currentTimeMillis();
            long seconds = (endTime - startTime) / 1000;
            LOG.info("For numThreads {} duration {}", numThreads.intValue(), seconds);
            results.put(numThreads, seconds);
            LOG.info("Broker got {} messages", brokerService.getAdminView().getTotalEnqueueCount());
        }

        brokerService.stop();
        brokerService.waitUntilStopped();
        LOG.info("Results: {}", results);
    }

    private void purge() throws Exception {
        ObjectName[] queues = brokerService.getAdminView().getQueues();
        if (queues.length == 1) {
            QueueViewMBean queueViewMBean = (QueueViewMBean)
                brokerService.getManagementContext().newProxyInstance(queues[0], QueueViewMBean.class, false);
            queueViewMBean.purge();
        }
    }

    @Test
    @Ignore("comparison test - takes too long and really needs a peek at the graph")
    public void testPerf() throws Exception {
        LinkedHashMap<Integer, Long> resultsT = new LinkedHashMap<Integer, Long>();
        LinkedHashMap<Integer, Long> resultsF = new LinkedHashMap<Integer, Long>();

        for (int i=2;i<11;i++) {
            for (Boolean concurrent : new Boolean[]{true, false}) {
                startBroker(i, concurrent, false);

                long startTime = System.currentTimeMillis();
                produceMessages(new AtomicLong(messageCount), target);
                long endTime = System.currentTimeMillis();
                long seconds = (endTime - startTime) / 1000;
                LOG.info("For routes {} duration {}", i, seconds);
                if (concurrent) {
                    resultsT.put(i, seconds);
                } else {
                    resultsF.put(i, seconds);
                }
                brokerService.stop();
                brokerService.waitUntilStopped();
            }
        }
        LOG.info("results T{} F{}", resultsT, resultsF);
        LOG.info("http://www.chartgo.com/samples.do?chart=line&border=1&show3d=0&width=600&height=500&roundedge=1&transparency=1&legend=1&title=Send:10k::Concurrent-v-Serial&xtitle=routes&ytitle=Duration(seconds)&chrtbkgndcolor=white&threshold=0.0&lang=en"
                + "&xaxis1=" + toStr(resultsT.keySet())
                + "&yaxis1=" + toStr(resultsT.values())
                + "&group1=concurrent"
                + "&xaxis2=" + toStr(resultsF.keySet())
                + "&yaxis2=" + toStr(resultsF.values())
                + "&group2=serial"
                + "&from=linejsp");
    }

    private String toStr(Collection set) {
        return set.toString().replace(",","%0D%0A").replace("[","").replace("]","").replace(" ", "");
    }

    protected void produceMessages(AtomicLong messageCount, ActiveMQDestination destination) throws Exception {
        final ByteSequence payLoad = new ByteSequence(new byte[messageSize]);
        Connection connection = connectionFactory.createConnection();
        MessageProducer messageProducer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createProducer(destination);
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(payLoad);
        while (messageCount.decrementAndGet() >= 0) {
            messageProducer.send(message);
        }
        connection.close();
    }

    private void startBroker(int fanoutCount, boolean concurrentSend, boolean concurrentStoreAndDispatchQueues) throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseVirtualTopics(true);
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.setAdvisorySupport(false);
        PolicyMap destPolicyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        defaultEntry.setOptimizedDispatch(true);
        defaultEntry.setCursorMemoryHighWaterMark(110);
        destPolicyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(destPolicyMap);

        CompositeTopic route = new CompositeTopic();
        route.setName("target");
        route.setForwardOnly(true);
        route.setConcurrentSend(concurrentSend);
        Collection<ActiveMQQueue> routes = new ArrayList<ActiveMQQueue>();
        for (int i=0; i<fanoutCount; i++) {
            routes.add(new ActiveMQQueue("route." + i));
        }
        route.setForwardTo(routes);
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{route});
        brokerService.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        brokerService.start();

        connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setWatchTopicAdvisories(false);
        if (brokerService.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {

            //with parallel sends and no consumers, concurrentStoreAnd dispatch, which uses a single thread by default
            // will stop/impeed write batching. The num threads will need tweaking when consumers are in the mix but may introduce
            // order issues
            ((KahaDBPersistenceAdapter)brokerService.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(concurrentStoreAndDispatchQueues);
        }
    }
}
