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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.Arrays;

import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvisoryViaNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AdvisoryViaNetworkTest.class);


    @Override
    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
        brokers.put(brokerName, new BrokerItem(broker));

        return broker;
    }

    public void testAdvisoryForwarding() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Producer.Topic.FOO");

        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);


        MessageConsumer consumerA = createConsumer("A", advisoryTopic);
        MessageConsumer consumerB = createConsumer("B", advisoryTopic);

        this.sendMessages("A", new ActiveMQTopic("FOO"), 1);

        MessageIdList messagesA = getConsumerMessages("A", consumerA);
        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(2);
        messagesB.assertMessagesReceived(2);
    }

    /**
     * Test that explicitly setting advisoryPrefetchSize works for advisory topics
     * on a network connector
     *
     * @throws Exception
     */
    public void testAdvisoryPrefetchSize() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");
        ActiveMQTopic topic1 = new ActiveMQTopic("A.FOO");

        createBroker("A");
        BrokerService brokerB = createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.addStaticallyIncludedDestination(topic1);
        networkBridge.setDuplex(true);
        networkBridge.setAdvisoryPrefetchSize(10);
        networkBridge.setPrefetchSize(1);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        createConsumer("A", topic1);
        createConsumer("A", new ActiveMQTopic("A.FOO2"));

        //verify that brokerB's advisory prefetch is 10 but normal topic prefetch is 1
        assertEquals(10, brokerB.getDestination(advisoryTopic).getConsumers().get(0).getPrefetchSize());
        assertEquals(1, brokerB.getDestination(topic1).getConsumers().get(0).getPrefetchSize());

        //both advisory messages are not acked yet because of optimized acks
        assertDeqInflight(0, 2);
    }

    /**
     * Test that explicitly setting advisoryPrefetchSize to 1 works for advisory topics
     * on a network connector
     *
     * @throws Exception
     */
    public void testAdvisoryPrefetchSize1() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");
        ActiveMQTopic topic1 = new ActiveMQTopic("A.FOO");

        createBroker("A");
        BrokerService brokerB = createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.addStaticallyIncludedDestination(topic1);
        networkBridge.setDuplex(true);
        networkBridge.setAdvisoryPrefetchSize(1);
        networkBridge.setPrefetchSize(10);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        createConsumer("A", topic1);
        createConsumer("A", new ActiveMQTopic("A.FOO2"));

        //verify that brokerB's advisory prefetch is 1 but normal topic prefetch is 10
        assertEquals(1, brokerB.getDestination(advisoryTopic).getConsumers().get(0).getPrefetchSize());
        assertEquals(10, brokerB.getDestination(topic1).getConsumers().get(0).getPrefetchSize());

        assertDeqInflight(2, 0);
    }

    /**
     * Test that if advisoryPrefetchSize isn't set then prefetchSize is used instead
     * for backwards compatibility
     *
     * @throws Exception
     */
    public void testAdvisoryPrefetchSizeNotSet() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");
        ActiveMQTopic topic1 = new ActiveMQTopic("A.FOO");

        createBroker("A");
        BrokerService brokerB = createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.addStaticallyIncludedDestination(topic1);
        networkBridge.setDuplex(true);
        networkBridge.setPrefetchSize(10);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        createConsumer("A", topic1);
        createConsumer("A", new ActiveMQTopic("A.FOO2"));

        //verify that both consumers have a prefetch of 10
        assertEquals(10, brokerB.getDestination(advisoryTopic).getConsumers().get(0).getPrefetchSize());
        assertEquals(10, brokerB.getDestination(topic1).getConsumers().get(0).getPrefetchSize());

        assertDeqInflight(0, 2);
    }

    /**
     * Test that if advisoryPrefetchSize isn't set then prefetchSize is used instead
     * for backwards compatibility (test when set to 1)
     *
     * @throws Exception
     */
    public void testPrefetchSize1() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");
        ActiveMQTopic topic1 = new ActiveMQTopic("A.FOO");

        createBroker("A");
        BrokerService brokerB = createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.setDuplex(true);
        networkBridge.setPrefetchSize(1);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        createConsumer("A", topic1);
        createConsumer("A", new ActiveMQTopic("A.FOO2"));

        //verify that both consumers have a prefetch of 1
        assertEquals(1, brokerB.getDestination(advisoryTopic).getConsumers().get(0).getPrefetchSize());
        assertEquals(1, brokerB.getDestination(topic1).getConsumers().get(0).getPrefetchSize());

        assertDeqInflight(2, 0);
    }

    /**
     * Test configuring the advisoryAckPercentage works with advisoryPrefetchSize
     * @throws Exception
     */
    public void testAdvisoryPrefetchSizePercent() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");

        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.setDuplex(true);
        networkBridge.setAdvisoryPrefetchSize(10);
        networkBridge.setAdvisoryAckPercentage(65);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        for (int i = 0; i < 10; i++) {
            createConsumer("A", new ActiveMQTopic("A.FOO"));
        }

        assertDeqInflight(7, 3);
    }

    /**
     * Test configuring the advisoryAckPercentage works when only prefetchSize exists
     * and is applied against that instead for advisory consumers
     *
     * @throws Exception
     */
    public void testPrefetchSizePercent() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.A.>");

        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.setDuplex(true);
        networkBridge.setPrefetchSize(10);
        networkBridge.setAdvisoryAckPercentage(65);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);

        for (int i = 0; i < 10; i++) {
            createConsumer("A", new ActiveMQTopic("A.FOO"));
        }

        assertDeqInflight(7, 3);
    }

    private void assertDeqInflight(final int dequeue, final int inflight) throws Exception {
        assertTrue("deq and inflight as expected", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                RegionBroker regionBroker = (RegionBroker) brokers.get("A").broker.getRegionBroker();
                LOG.info("A Deq:" + regionBroker.getDestinationStatistics().getDequeues().getCount());
                LOG.info("A Inflight:" + regionBroker.getDestinationStatistics().getInflight().getCount());
                return regionBroker.getDestinationStatistics().getDequeues().getCount() == dequeue
                        && regionBroker.getDestinationStatistics().getInflight().getCount() == inflight;
            }
        }));
    }


    public void testAdvisoryForwardingDuplexNC() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Producer.Topic.FOO");

        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        networkBridge.setDuplex(true);
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);


        MessageConsumer consumerA = createConsumer("A", advisoryTopic);
        MessageConsumer consumerB = createConsumer("B", advisoryTopic);

        this.sendMessages("A", new ActiveMQTopic("FOO"), 1);

        MessageIdList messagesA = getConsumerMessages("A", consumerA);
        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(2);
        messagesB.assertMessagesReceived(2);
    }

    public void testBridgeRelevantAdvisoryNotAvailable() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.FOO");
        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);


        MessageConsumer consumerA = createConsumer("A", advisoryTopic);
        MessageConsumer consumerB = createConsumer("B", advisoryTopic);

        createConsumer("A", new ActiveMQTopic("FOO"));

        MessageIdList messagesA = getConsumerMessages("A", consumerA);
        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(1);
        messagesB.assertMessagesReceived(0);
    }

    public void testAdvisoryViaVirtualDest() throws Exception {
        ActiveMQQueue advisoryQueue = new ActiveMQQueue("advQ");
        createBroker("A");

        // convert advisories into advQ that cross the network bridge
        CompositeTopic compositeTopic = new CompositeTopic();
        compositeTopic.setName("ActiveMQ.Advisory.Connection");
        compositeTopic.setForwardOnly(false);
        compositeTopic.setForwardTo(Arrays.asList(advisoryQueue));
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(new VirtualDestination[]{compositeTopic});
        brokers.get("A").broker.setDestinationInterceptors(new DestinationInterceptor[]{virtualDestinationInterceptor});

        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.setDuplex(true);
        networkBridge.setPrefetchSize(1); // so advisories are acked immediately b/c we check inflight count below

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);
        verifyPeerBrokerInfo(brokers.get("B"), 1);

        MessageConsumer consumerB = createConsumer("B", advisoryQueue);

        // to make a connection on A
        createConsumer("A", new ActiveMQTopic("FOO"));

        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        messagesB.waitForMessagesToArrive(2);

        assertTrue("deq and inflight as expected", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                RegionBroker regionBroker = (RegionBroker) brokers.get("A").broker.getRegionBroker();
                LOG.info("A Deq:" + regionBroker.getDestinationStatistics().getDequeues().getCount());
                LOG.info("A Inflight:" + regionBroker.getDestinationStatistics().getInflight().getCount());
                return regionBroker.getDestinationStatistics().getDequeues().getCount() > 2
                        && regionBroker.getDestinationStatistics().getInflight().getCount() == 0;
            }
        }));

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
         }, 120 * 1000);
        LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
        for (BrokerInfo info : regionBroker.getPeerBrokerInfos()) {
            LOG.info(info.getBrokerName());
        }
        assertEquals(broker.getBrokerName(), max, regionBroker.getPeerBrokerInfos().length);
    }


    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

}
