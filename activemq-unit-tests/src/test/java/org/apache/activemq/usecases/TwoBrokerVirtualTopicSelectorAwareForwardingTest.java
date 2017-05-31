/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.VirtualDestinationSelectorCacheViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.plugin.SubQueueSelectorCacheBrokerPlugin;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.activemq.TestSupport.getDestination;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class TwoBrokerVirtualTopicSelectorAwareForwardingTest extends
        JmsMultipleBrokersTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TwoBrokerVirtualTopicSelectorAwareForwardingTest.class);

    private static final String PERSIST_SELECTOR_CACHE_FILE_BASEPATH = "./target/selectorCache-";

    public void testJMX() throws Exception {
        clearSelectorCacheFiles();
        // borkerA is local and brokerB is remote
        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        startAllBrokers();
        waitForBridgeFormation();

        createConsumer("BrokerB", createDestination("Consumer.B.VirtualTopic.tempTopic", false),
                "foo = 'bar'");


        final BrokerService brokerA = brokers.get("BrokerA").broker;

        String testQueue = "queue://Consumer.B.VirtualTopic.tempTopic";
        VirtualDestinationSelectorCacheViewMBean cache = getVirtualDestinationSelectorCacheMBean(brokerA);
        Set<String> selectors = cache.selectorsForDestination(testQueue);

        assertEquals(1, selectors.size());
        assertTrue(selectors.contains("foo = 'bar'"));

        boolean removed = cache.deleteSelectorForDestination(testQueue, "foo = 'bar'");
        assertTrue(removed);

        selectors = cache.selectorsForDestination(testQueue);
        assertEquals(0, selectors.size());

        createConsumer("BrokerB", createDestination("Consumer.B.VirtualTopic.tempTopic", false),
                "ceposta = 'redhat'");


        Wait.waitFor(new Wait.Condition() {

            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 2;
            }
        }, 500);

        selectors = cache.selectorsForDestination(testQueue);
        assertEquals(1, selectors.size());
        cache.deleteAllSelectorsForDestination(testQueue);
        selectors = cache.selectorsForDestination(testQueue);
        assertEquals(0, selectors.size());

    }

    public void testMessageLeaks() throws Exception {
        clearSelectorCacheFiles();
        startAllBrokers();

        final BrokerService brokerA = brokers.get("BrokerA").broker;

        // Create the remote virtual topic consumer with selector
        ActiveMQDestination consumerQueue = createDestination("Consumer.B.VirtualTopic.tempTopic", false);
        // create it so that the queue is there and messages don't get lost
        MessageConsumer consumer1 = createConsumer("BrokerA", consumerQueue, "SYMBOL = 'AAPL'");
        MessageConsumer consumer2 = createConsumer("BrokerA", consumerQueue, "SYMBOL = 'AAPL'");

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        ProducerThreadTester producerTester = createProducerTester("BrokerA", virtualTopic);
        producerTester.setRunIndefinitely(true);
        producerTester.setSleep(5);
        producerTester.addMessageProperty("AAPL");
        producerTester.addMessageProperty("VIX");
        producerTester.start();

        int currentCount = producerTester.getSentCount();
        LOG.info(">>>> currently sent: total=" + currentCount + ", AAPL=" + producerTester.getCountForProperty("AAPL") + ", VIX=" + producerTester.getCountForProperty("VIX"));

        // let some messages get sent
        Thread.sleep(2000);

        MessageIdList consumer1Messages = getConsumerMessages("BrokerA", consumer1);
        consumer1Messages.waitForMessagesToArrive(50, 1000);

        // switch one of the consumers to SYMBOL = 'VIX'
        consumer1.close();
        consumer1 = createConsumer("BrokerA", consumerQueue, "SYMBOL = 'VIX'");

        // wait till new consumer is on board
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                        .getConsumers().size() == 2;
            }
        });

        currentCount = producerTester.getSentCount();
        LOG.info(">>>> currently sent: total=" + currentCount + ", AAPL=" + producerTester.getCountForProperty("AAPL") + ", VIX=" + producerTester.getCountForProperty("VIX"));

        // let some messages get sent
        Thread.sleep(2000);

        // switch the other consumer to SYMBOL = 'VIX'
        consumer2.close();
        consumer2 = createConsumer("BrokerA", consumerQueue, "SYMBOL = 'VIX'");

        // wait till new consumer is on board
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                        .getConsumers().size() == 2;
            }
        });

        currentCount = producerTester.getSentCount();
        LOG.info(">>>> currently sent: total=" + currentCount + ", AAPL=" + producerTester.getCountForProperty("AAPL") + ", VIX=" + producerTester.getCountForProperty("VIX"));

        // let some messages get sent
        Thread.sleep(2000);

        currentCount = producerTester.getSentCount();
        LOG.info(">>>> currently sent: total=" + currentCount + ", AAPL=" + producerTester.getCountForProperty("AAPL") + ", VIX=" + producerTester.getCountForProperty("VIX"));


        // make sure if there are messages that are orphaned in the queue that this number doesn't
        // grow...
        final long currentDepth = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount();

        LOG.info(">>>>> Orphaned messages? " + currentDepth);

        // wait 5s to see if we can get a growth in the depth of the queue
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                        .getDestinationStatistics().getMessages().getCount() > currentDepth;
            }
        }, 5000);

        // stop producers
        producerTester.setRunning(false);
        producerTester.join();

        // pause to let consumers catch up
        Thread.sleep(1000);

        assertTrue(brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount() <= currentDepth);


    }

    private ProducerThreadTester createProducerTester(String brokerName, javax.jms.Destination destination) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);

        Connection conn = brokerItem.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ProducerThreadTester rc = new ProducerThreadTester(sess, destination);
        rc.setPersistent(persistentDelivery);
        return rc;
    }

    public void testSelectorConsumptionWithNoMatchAtHeadOfQueue() throws Exception {
        clearSelectorCacheFiles();
        startAllBrokers();

        BrokerService brokerA = brokers.get("BrokerA").broker;

        // Create the remote virtual topic consumer with selector
        ActiveMQDestination consumerQueue = createDestination("Consumer.B.VirtualTopic.tempTopic", false);

        // create it so that the queue is there and messages don't get lost
        MessageConsumer selectingConsumer = establishConsumer("BrokerA", consumerQueue);

        // send messages with NO selection criteria first, and then with a property to be selected
        // this should put messages at the head of the queue that don't match selection
        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        sendMessages("BrokerA", virtualTopic, 1);

        // close the consumer w/out consuming any messages; they'll be marked redelivered
        selectingConsumer.close();

        selectingConsumer = createConsumer("BrokerA", consumerQueue, "foo = 'bar'");

        sendMessages("BrokerA", virtualTopic, 1, asMap("foo", "bar"));


        MessageIdList selectingConsumerMessages = getConsumerMessages("BrokerA", selectingConsumer);
        selectingConsumerMessages.waitForMessagesToArrive(1, 1000L);

        assertEquals(1, selectingConsumerMessages.getMessageCount());
        selectingConsumerMessages.waitForMessagesToArrive(10, 1000L);
        assertEquals(1, selectingConsumerMessages.getMessageCount());

        // assert broker A stats
        waitForMessagesToBeConsumed(brokerA, "Consumer.B.VirtualTopic.tempTopic", false, 2, 1, 5000);
        assertEquals(1, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getConsumers().size());
        assertEquals(2, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(1, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(1, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

    }

    private MessageConsumer establishConsumer(String broker, ActiveMQDestination consumerQueue) throws Exception {
        BrokerItem item = brokers.get(broker);
        Connection c = item.createConnection();
        c.start();
        Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return s.createConsumer(consumerQueue);
    }

    public void testSelectorsAndNonSelectors() throws Exception {
        clearSelectorCacheFiles();
        // borkerA is local and brokerB is remote
        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        startAllBrokers();
        waitForBridgeFormation();


        final BrokerService brokerA = brokers.get("BrokerA").broker;
        final BrokerService brokerB = brokers.get("BrokerB").broker;

        // Create the remote virtual topic consumer with selector
        ActiveMQDestination consumerBQueue = createDestination("Consumer.B.VirtualTopic.tempTopic", false);

        MessageConsumer selectingConsumer = createConsumer("BrokerB", consumerBQueue, "foo = 'bar'");
        MessageConsumer nonSelectingConsumer = createConsumer("BrokerB", consumerBQueue);

        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 2;
            }
        }, 500);


        Destination destination = getDestination(brokerB, consumerBQueue);
        assertEquals(2, destination.getConsumers().size());

        // publisher publishes to this
        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        sendMessages("BrokerA", virtualTopic, 10, asMap("foo", "bar"));
        sendMessages("BrokerA", virtualTopic, 10);


        MessageIdList selectingConsumerMessages = getConsumerMessages("BrokerB", selectingConsumer);


        MessageIdList nonSelectingConsumerMessages = getConsumerMessages("BrokerB", nonSelectingConsumer);

        // we only expect half of the messages that get sent with the selector, because they get load balanced
        selectingConsumerMessages.waitForMessagesToArrive(5, 1000L);
        assertEquals(5, selectingConsumerMessages.getMessageCount());

        nonSelectingConsumerMessages.waitForMessagesToArrive(15, 1000L);
        assertEquals(15, nonSelectingConsumerMessages.getMessageCount());

        // assert broker A stats
        waitForMessagesToBeConsumed(brokerA, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(20, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        // assert broker B stats
        waitForMessagesToBeConsumed(brokerB, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());


        //now let's close the consumer without the selector
        nonSelectingConsumer.close();


        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 1;
            }
        }, 500);

        // and let's send messages with a selector that doesnt' match
        selectingConsumerMessages.flushMessages();

        sendMessages("BrokerA", virtualTopic, 10, asMap("ceposta", "redhat"));

        selectingConsumerMessages = getConsumerMessages("BrokerB", selectingConsumer);
        selectingConsumerMessages.waitForMessagesToArrive(1, 1000L);
        assertEquals(0, selectingConsumerMessages.getMessageCount());

        // assert broker A stats
        waitForMessagesToBeConsumed(brokerA, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(20, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        // assert broker B stats
        waitForMessagesToBeConsumed(brokerB, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        // now lets disconect the selecting consumer for a sec and send messages with a selector that DOES match
        selectingConsumer.close();

        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 0;
            }
        }, 500);

        selectingConsumerMessages.flushMessages();

        sendMessages("BrokerA", virtualTopic, 10, asMap("foo", "bar"));


        // assert broker A stats
        waitForMessagesToBeConsumed(brokerA, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(30, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(10, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        // assert broker B stats
        waitForMessagesToBeConsumed(brokerB, "Consumer.B.VirtualTopic.tempTopic", false, 20, 20, 5000);
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(20, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        selectingConsumer = createConsumer("BrokerB", consumerBQueue, "foo = 'bar'");
        selectingConsumerMessages = getConsumerMessages("BrokerB", selectingConsumer);
        selectingConsumerMessages.waitForMessagesToArrive(10);
        assertEquals(10, selectingConsumerMessages.getMessageCount());

        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 1;
            }
        }, 500);

        // assert broker A stats
        waitForMessagesToBeConsumed(brokerA, "Consumer.B.VirtualTopic.tempTopic", false, 30, 30, 5000);
        assertEquals(30, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(30, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());

        // assert broker B stats
        waitForMessagesToBeConsumed(brokerB, "Consumer.B.VirtualTopic.tempTopic", false, 30, 30, 5000);
        assertEquals(30, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());
        assertEquals(30, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getDequeues().getCount());
        assertEquals(0, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());


    }

    public VirtualDestinationSelectorCacheViewMBean getVirtualDestinationSelectorCacheMBean(BrokerService broker)
            throws MalformedObjectNameException {
        ObjectName objectName = BrokerMBeanSupport
                .createVirtualDestinationSelectorCacheName(broker.getBrokerObjectName(), "plugin", "virtualDestinationCache");
        return (VirtualDestinationSelectorCacheViewMBean) broker.getManagementContext()
                .newProxyInstance(objectName, VirtualDestinationSelectorCacheViewMBean.class, true);
    }

    public void testSelectorAwareForwarding() throws Exception {
        clearSelectorCacheFiles();
        // borkerA is local and brokerB is remote
        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        startAllBrokers();
        waitForBridgeFormation();

        final BrokerService brokerB = brokers.get("BrokerB").broker;
        final BrokerService brokerA = brokers.get("BrokerA").broker;

        // Create the remote virtual topic consumer with selector
        MessageConsumer remoteConsumer = createConsumer("BrokerB",
                createDestination("Consumer.B.VirtualTopic.tempTopic", false),
                "foo = 'bar'");


        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 1;
            }
        }, 500);

        ActiveMQQueue queueB = new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic");
        Destination destination = getDestination(brokers.get("BrokerB").broker, queueB);
        assertEquals(1, destination.getConsumers().size());

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        assertNull(getDestination(brokers.get("BrokerA").broker, virtualTopic));
        assertNull(getDestination(brokers.get("BrokerB").broker, virtualTopic));

        // send two types of messages, one unwanted and the other wanted
        sendMessages("BrokerA", virtualTopic, 1, asMap("foo", "bar"));
        sendMessages("BrokerA", virtualTopic, 1, asMap("ceposta", "redhat"));

        MessageIdList msgsB = getConsumerMessages("BrokerB", remoteConsumer);
        // wait for the wanted one to arrive at the remote consumer
        msgsB.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        msgsB.waitForMessagesToArrive(1, 1000);

        // remote consumer should only get one of the messages
        assertEquals(1, msgsB.getMessageCount());

        // and the enqueue count for the remote queue should only be 1
        assertEquals(1, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());


        // now let's remove the consumer on broker B and recreate it with new selector
        remoteConsumer.close();


        // now let's shut down broker A and clear its persistent selector cache
        brokerA.stop();
        brokerA.waitUntilStopped();
        deleteSelectorCacheFile("BrokerA");

        assertEquals(0, destination.getConsumers().size());

        remoteConsumer = createConsumer("BrokerB",
                createDestination("Consumer.B.VirtualTopic.tempTopic", false),
                "ceposta = 'redhat'");


        assertEquals(1, destination.getConsumers().size());


        // now let's start broker A back up
        brokerA.start(true);
        brokerA.waitUntilStarted();

        System.out.println(brokerA.getNetworkConnectors());

        // give a sec to let advisories propogate
        // let advisories propogate
        Wait.waitFor(new Wait.Condition() {
            Destination dest = brokerA.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"));

            @Override
            public boolean isSatisified() throws Exception {
                return dest.getConsumers().size() == 1;
            }
        }, 500);


        // send two types of messages, one unwanted and the other wanted
        sendMessages("BrokerA", virtualTopic, 1, asMap("foo", "bar"));
        sendMessages("BrokerB", virtualTopic, 1, asMap("foo", "bar"));
        sendMessages("BrokerA", virtualTopic, 1, asMap("ceposta", "redhat"));
        sendMessages("BrokerB", virtualTopic, 1, asMap("ceposta", "redhat"));

        // lets get messages on consumer B
        msgsB = getConsumerMessages("BrokerB", remoteConsumer);
        msgsB.waitForMessagesToArrive(2);

        // ensure we don't get any more messages
        msgsB.waitForMessagesToArrive(1, 1000);


        // remote consumer should only get 10 of the messages
        assertEquals(2, msgsB.getMessageCount());


        // queue should be drained
        assertEquals(0, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getMessages().getCount());
        // and the enqueue count for the remote queue should only be 1
        assertEquals(3, brokerB.getDestination(new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic"))
                .getDestinationStatistics().getEnqueues().getCount());


    }

    public void testSelectorNoMatchInCache() throws Exception {
        clearSelectorCacheFiles();

        // have the cache ignoreWildcardSelectors
        final BrokerService brokerA = brokers.get("BrokerA").broker;
        ((SubQueueSelectorCacheBrokerPlugin)brokerA.getPlugins()[0]).setIgnoreWildcardSelectors(true);

        startAllBrokers();

        ActiveMQDestination consumerBQueue = createDestination("Consumer.B.VirtualTopic.tempTopic", false);

        MessageConsumer nonMatchingConsumer = createConsumer("BrokerA", consumerBQueue, "foo = 'bar%'");

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        sendMessages("BrokerA", virtualTopic, 1, asMap("foo", "notBar"));
    }

    private HashMap<String, Object> asMap(String key, Object value) {
        HashMap<String, Object> rc = new HashMap<String, Object>(1);
        rc.put(key, value);
        return rc;
    }


    private void bridgeAndConfigureBrokers(String local, String remote)
            throws Exception {
        NetworkConnector bridge = bridgeBrokers(local, remote, false, 1, false);
        bridge.setDecreaseNetworkConsumerPriority(true);
        bridge.setDuplex(true);
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String(
                "?useJmx=false&deleteAllMessagesOnStartup=true");
        createAndConfigureBroker(new URI(
                "broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI(
                "broker:(tcp://localhost:61617)/BrokerB" + options));
    }

    private void clearSelectorCacheFiles() {
        String[] brokerNames = new String[]{"BrokerA", "BrokerB"};
        for (String brokerName : brokerNames) {
            deleteSelectorCacheFile(brokerName);
        }
    }

    private void deleteSelectorCacheFile(String brokerName) {
        File brokerPersisteFile = new File(PERSIST_SELECTOR_CACHE_FILE_BASEPATH + brokerName);

        if (brokerPersisteFile.exists()) {
            brokerPersisteFile.delete();
        }
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        broker.setUseJmx(true);
        // Make topics "selectorAware"
        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setSelectorAware(true);
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        configurePersistenceAdapter(broker);

        SubQueueSelectorCacheBrokerPlugin selectorCacheBrokerPlugin = new SubQueueSelectorCacheBrokerPlugin();
        selectorCacheBrokerPlugin.setSingleSelectorPerDestination(true);
        File persisteFile = new File(PERSIST_SELECTOR_CACHE_FILE_BASEPATH + broker.getBrokerName());
        selectorCacheBrokerPlugin.setPersistFile(persisteFile);
        broker.setPlugins(new BrokerPlugin[]{selectorCacheBrokerPlugin});
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker)
            throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/"
                + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }

    /**
     * Typically used before asserts to give producers and consumers some time to finish their tasks
     * before the final state is tested.
     *
     * @param broker BrokerService on which the destinations are looked up
     * @param destinationName
     * @param topic true if the destination is a Topic, false if it is a Queue
     * @param numEnqueueMsgs expected number of enqueued messages in the destination
     * @param numDequeueMsgs expected number of dequeued messages in the destination
     * @param waitTime number of milliseconds to wait for completion
     * @throws Exception
     */
    private void waitForMessagesToBeConsumed(final BrokerService broker, final String destinationName,
                                             final boolean topic, final int numEnqueueMsgs, final int numDequeueMsgs, int waitTime) throws Exception {
        final ActiveMQDestination destination;
        if (topic) {
            destination = new ActiveMQTopic(destinationName);
        } else {
            destination = new ActiveMQQueue(destinationName);
        }

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {

                return broker.getDestination(destination)
                        .getDestinationStatistics().getEnqueues().getCount() == numEnqueueMsgs;
            }
        }, waitTime);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {

                return broker.getDestination(destination)
                        .getDestinationStatistics().getDequeues().getCount() == numDequeueMsgs;
            }
        }, waitTime);
    }


    class ProducerThreadTester extends ProducerThread {

        private Set<String> selectors = new LinkedHashSet<String>();
        private Map<String, AtomicInteger> selectorCounts = new HashMap<String, AtomicInteger>();
        private Random rand = new Random(System.currentTimeMillis());


        public ProducerThreadTester(Session session, javax.jms.Destination destination) {
            super(session, destination);
        }

        @Override
        protected Message createMessage(int i) throws Exception {
            TextMessage msg = createTextMessage(this.session, "Message-" + i);
            if (selectors.size() > 0) {
                String value = getRandomKey();
                msg.setStringProperty("SYMBOL", value);
                AtomicInteger currentCount = selectorCounts.get(value);
                currentCount.incrementAndGet();
            }

            return msg;
        }

        @Override
        public void resetCounters() {
            super.resetCounters();
            for (String key : selectorCounts.keySet()) {
                selectorCounts.put(key, new AtomicInteger(0));
            }
        }

        private String getRandomKey() {
            ArrayList<String> keys = new ArrayList(selectors);
            return keys.get(rand.nextInt(keys.size()));
        }

        public void addMessageProperty(String value) {
            if (!this.selectors.contains(value)) {
                selectors.add(value);
                selectorCounts.put(value, new AtomicInteger(0));
            }
        }

        public int getCountForProperty(String key) {
            return selectorCounts.get(key).get();
        }

    }
}
