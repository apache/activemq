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

import java.io.IOException;
import java.net.URI;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.MessageConsumer;
import junit.framework.Test;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

/**
 * This test demonstrates and verifies the behaviour of a network bridge when it
 * forwards a message to a queue that is full and producer flow control is
 * enabled.
 * <p/>
 * The expected behaviour is that the bridge will stop forwarding messages to
 * the full queue once the associated demand consumer's prefetch is full, but
 * will continue to forward messages to the other queues that are not full.
 * <p/>
 * In actuality, a message that is sent <b>asynchronously</b> to a local queue,
 * but blocked by producer flow control on the remote queue, will stop the
 * bridge from forwarding all subsequent messages, even those destined for
 * remote queues that are not full. In the same scenario, but with a message
 * that is sent <b>synchronously</b> to the local queue, the bridge continues
 * forwarding messages to remote queues that are not full.
 * <p/>
 * This test demonstrates the differing behaviour via the following scenario:
 * <ul>
 * <li>broker0, designated as the local broker, produces messages to two shared
 * queues
 * <li>broker1, designated as the remote broker, has two consumers: the first
 * consumes from one of the shared queues as fast as possible, the second
 * consumes from the other shared queue with an artificial processing delay for
 * each message
 * <li>broker0 forwards messages to broker1 over a TCP-based network bridge
 * with a demand consumer prefetch of 1
 * <li>broker1's consumers have a prefetch of 1
 * <li>broker1's "slow consumer" queue has a memory limit that triggers
 * producer flow control once the queue contains a small number of messages
 * </ul>
 * In this scenario, since broker1's consumers have a prefetch of 1, the "slow
 * consumer" queue will quickly become full and trigger producer flow control.
 * The "fast consumer" queue is unlikely to become full. Since producer flow
 * control on the "slow consumer" queue should not affect the "fast consumer"
 * queue, the expectation is that the fast consumer in broker1 will finish
 * processing all its messages well ahead of the slow consumer.
 * <p/>
 * The difference between expected and actual behaviour is demonstrated by
 * changing the messages produced by broker0 from persistent to non-persistent.
 * With persistent messages, broker0 dispatches synchronously and the expected
 * behaviour is observed (i.e., the fast consumer on broker1 is much faster than
 * the slow consumer). With non-persistent messages, broker0 dispatches
 * asynchronously and the expected behaviour is <b>not</b> observed (i.e., the
 * fast consumer is only marginally faster than the slow consumer).
 * <p/>
 * Since the expected behaviour may be desirable for both persistent and
 * non-persistent messages, this test also demonstrates an enhancement to the
 * network bridge configuration: <tt>isAlwaysSendSync</tt>. When false the
 * bridge operates as originally observed. When <tt>true</tt>, the bridge
 * operates with the same behaviour as was originally observed with persistent
 * messages, for both persistent and non-persistent messages.
 * <p/>
 * https://issues.apache.org/jira/browse/AMQ-3331
 *
 * @author schow
 */
public class NetworkBridgeProducerFlowControlTest extends
        JmsMultipleBrokersTestSupport {

    // Protect against hanging test.
    private static final long MAX_TEST_TIME = 120000;

    private static final Log LOG = LogFactory
            .getLog(NetworkBridgeProducerFlowControlTest.class);

    // Combo flag set to true/false by the test framework.
    public boolean persistentTestMessages;
    public boolean networkIsAlwaysSendSync;

    private Vector<Throwable> exceptions = new Vector<Throwable>();

    public static Test suite() {
        return suite(NetworkBridgeProducerFlowControlTest.class);
    }

    public void initCombosForTestFastAndSlowRemoteConsumers() {
        addCombinationValues("persistentTestMessages", new Object[]{
                Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("networkIsAlwaysSendSync", new Object[]{
                Boolean.TRUE, Boolean.FALSE});
    }

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        setMaxTestTime(MAX_TEST_TIME);
        super.setUp();
    }

    /**
     * This test is parameterized by {@link #persistentTestMessages}, which
     * determines whether the producer on broker0 sends persistent or
     * non-persistent messages, and {@link #networkIsAlwaysSendSync}, which
     * determines how the bridge will forward both persistent and non-persistent
     * messages to broker1.
     *
     * @see #initCombosForTestFastAndSlowRemoteConsumers()
     */
    public void testFastAndSlowRemoteConsumers() throws Exception {
        final int NUM_MESSAGES = 100;
        final long TEST_MESSAGE_SIZE = 1024;
        final long SLOW_CONSUMER_DELAY_MILLIS = 100;

        // Consumer prefetch is disabled for broker1's consumers.
        final ActiveMQQueue SLOW_SHARED_QUEUE = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".slow.shared?consumer.prefetchSize=1");

        final ActiveMQQueue FAST_SHARED_QUEUE = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".fast.shared?consumer.prefetchSize=1");

        // Start a local and a remote broker.
        createBroker(new URI("broker:(tcp://localhost:0"
                + ")?brokerName=broker0&persistent=false&useJmx=true"));
        BrokerService remoteBroker = createBroker(new URI(
                "broker:(tcp://localhost:0"
                        + ")?brokerName=broker1&persistent=false&useJmx=true"));

        // Set a policy on the remote broker that limits the maximum size of the
        // slow shared queue.
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(5 * TEST_MESSAGE_SIZE);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(SLOW_SHARED_QUEUE, policyEntry);
        remoteBroker.setDestinationPolicy(policyMap);

        // Create an outbound bridge from the local broker to the remote broker.
        // The bridge is configured with the remoteDispatchType enhancement.
        NetworkConnector nc = bridgeBrokers("broker0", "broker1");
        nc.setAlwaysSyncSend(networkIsAlwaysSendSync);
        nc.setPrefetchSize(1);

        startAllBrokers();
        waitForBridgeFormation();

        // Send the test messages to the local broker's shared queues. The
        // messages are either persistent or non-persistent to demonstrate the
        // difference between synchronous and asynchronous dispatch.
        persistentDelivery = persistentTestMessages;
        sendMessages("broker0", FAST_SHARED_QUEUE, NUM_MESSAGES);
        sendMessages("broker0", SLOW_SHARED_QUEUE, NUM_MESSAGES);

        // Start two asynchronous consumers on the remote broker, one for each
        // of the two shared queues, and keep track of how long it takes for
        // each of the consumers to receive all the messages.
        final CountDownLatch fastConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);
        final CountDownLatch slowConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);

        final long startTimeMillis = System.currentTimeMillis();
        final AtomicLong fastConsumerTime = new AtomicLong();
        final AtomicLong slowConsumerTime = new AtomicLong();

        Thread fastWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    fastConsumerLatch.await();
                    fastConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        Thread slowWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    slowConsumerLatch.await();
                    slowConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        fastWaitThread.start();
        slowWaitThread.start();

        createConsumer("broker1", FAST_SHARED_QUEUE, fastConsumerLatch);
        MessageConsumer slowConsumer = createConsumer("broker1",
                SLOW_SHARED_QUEUE, slowConsumerLatch);
        MessageIdList messageIdList = brokers.get("broker1").consumers
                .get(slowConsumer);
        messageIdList.setProcessingDelay(SLOW_CONSUMER_DELAY_MILLIS);

        fastWaitThread.join();
        slowWaitThread.join();

        assertTrue("no exceptions on the wait threads:" + exceptions,
                exceptions.isEmpty());

        LOG.info("Fast consumer duration (ms): " + fastConsumerTime.get());
        LOG.info("Slow consumer duration (ms): " + slowConsumerTime.get());

        // Verify the behaviour as described in the description of this class.
        if (networkIsAlwaysSendSync) {
            Assert
                    .assertTrue(fastConsumerTime.get() < slowConsumerTime.get() / 20);

        } else {
            Assert.assertEquals(persistentTestMessages,
                    fastConsumerTime.get() < slowConsumerTime.get() / 10);
        }
    }

    public void testSendFailIfNoSpaceDoesNotBlockQueueNetwork() throws Exception {
        // Consumer prefetch is disabled for broker1's consumers.
        final ActiveMQQueue SLOW_SHARED_QUEUE = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".slow.shared?consumer.prefetchSize=1");

        final ActiveMQQueue FAST_SHARED_QUEUE = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".fast.shared?consumer.prefetchSize=1");

        doTestSendFailIfNoSpaceDoesNotBlockNetwork(
                SLOW_SHARED_QUEUE,
                FAST_SHARED_QUEUE);
    }

    public void testSendFailIfNoSpaceDoesNotBlockTopicNetwork() throws Exception {
        // Consumer prefetch is disabled for broker1's consumers.
        final ActiveMQTopic SLOW_SHARED_TOPIC = new ActiveMQTopic(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".slow.shared?consumer.prefetchSize=1");

        final ActiveMQTopic FAST_SHARED_TOPIC = new ActiveMQTopic(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".fast.shared?consumer.prefetchSize=1");

        doTestSendFailIfNoSpaceDoesNotBlockNetwork(
                SLOW_SHARED_TOPIC,
                FAST_SHARED_TOPIC);
    }

    public void doTestSendFailIfNoSpaceDoesNotBlockNetwork(
            ActiveMQDestination slowDestination, ActiveMQDestination fastDestination) throws Exception {

        final int NUM_MESSAGES = 100;
        final long TEST_MESSAGE_SIZE = 1024;
        final long SLOW_CONSUMER_DELAY_MILLIS = 100;

        // Start a local and a remote broker.
        createBroker(new URI("broker:(tcp://localhost:0"
                + ")?brokerName=broker0&persistent=false&useJmx=true"));
        BrokerService remoteBroker = createBroker(new URI(
                "broker:(tcp://localhost:0"
                        + ")?brokerName=broker1&persistent=false&useJmx=true"));
        remoteBroker.getSystemUsage().setSendFailIfNoSpace(true);

        // Set a policy on the remote broker that limits the maximum size of the
        // slow shared queue.
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(5 * TEST_MESSAGE_SIZE);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(slowDestination, policyEntry);
        remoteBroker.setDestinationPolicy(policyMap);

        // Create an outbound bridge from the local broker to the remote broker.
        // The bridge is configured with the remoteDispatchType enhancement.
        NetworkConnector nc = bridgeBrokers("broker0", "broker1");
        nc.setAlwaysSyncSend(true);
        nc.setPrefetchSize(1);

        startAllBrokers();
        waitForBridgeFormation();

        // Start two asynchronous consumers on the remote broker, one for each
        // of the two shared queues, and keep track of how long it takes for
        // each of the consumers to receive all the messages.
        final CountDownLatch fastConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);
        final CountDownLatch slowConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);

        final long startTimeMillis = System.currentTimeMillis();
        final AtomicLong fastConsumerTime = new AtomicLong();
        final AtomicLong slowConsumerTime = new AtomicLong();

        Thread fastWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    fastConsumerLatch.await();
                    fastConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        Thread slowWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    slowConsumerLatch.await();
                    slowConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        fastWaitThread.start();
        slowWaitThread.start();

        createConsumer("broker1", fastDestination, fastConsumerLatch);
        MessageConsumer slowConsumer = createConsumer("broker1",
                slowDestination, slowConsumerLatch);
        MessageIdList messageIdList = brokers.get("broker1").consumers
                .get(slowConsumer);
        messageIdList.setProcessingDelay(SLOW_CONSUMER_DELAY_MILLIS);

        // Send the test messages to the local broker's shared queues. The
        // messages are either persistent or non-persistent to demonstrate the
        // difference between synchronous and asynchronous dispatch.
        persistentDelivery = false;
        sendMessages("broker0", fastDestination, NUM_MESSAGES);
        sendMessages("broker0", slowDestination, NUM_MESSAGES);

        fastWaitThread.join(TimeUnit.SECONDS.toMillis(60));
        slowWaitThread.join(TimeUnit.SECONDS.toMillis(60));

        assertTrue("no exceptions on the wait threads:" + exceptions,
                exceptions.isEmpty());

        LOG.info("Fast consumer duration (ms): " + fastConsumerTime.get());
        LOG.info("Slow consumer duration (ms): " + slowConsumerTime.get());

        assertTrue("fast time set", fastConsumerTime.get() > 0);
        assertTrue("slow time set", slowConsumerTime.get() > 0);

        // Verify the behaviour as described in the description of this class.
        Assert.assertTrue(fastConsumerTime.get() < slowConsumerTime.get() / 10);
    }

    public void testSendFailIfNoSpaceReverseDoesNotBlockQueueNetwork() throws Exception {
        final int NUM_MESSAGES = 100;
        final long TEST_MESSAGE_SIZE = 1024;
        final long SLOW_CONSUMER_DELAY_MILLIS = 100;

        final ActiveMQQueue slowDestination = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".slow.shared?consumer.prefetchSize=1");

        final ActiveMQQueue fastDestination = new ActiveMQQueue(
            NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                    + ".fast.shared?consumer.prefetchSize=1");


        // Start a local and a remote broker.
        BrokerService localBroker = createBroker(new URI("broker:(tcp://localhost:0"
                + ")?brokerName=broker0&persistent=false&useJmx=true"));
        createBroker(new URI(
                "broker:(tcp://localhost:0"
                        + ")?brokerName=broker1&persistent=false&useJmx=true"));
        localBroker.getSystemUsage().setSendFailIfNoSpace(true);

        // Set a policy on the local broker that limits the maximum size of the
        // slow shared queue.
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(5 * TEST_MESSAGE_SIZE);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(slowDestination, policyEntry);
        localBroker.setDestinationPolicy(policyMap);

        // Create an outbound bridge from the local broker to the remote broker.
        // The bridge is configured with the remoteDispatchType enhancement.
        NetworkConnector nc = bridgeBrokers("broker0", "broker1");
        nc.setAlwaysSyncSend(true);
        nc.setPrefetchSize(1);
        nc.setDuplex(true);

        startAllBrokers();
        waitForBridgeFormation();

        // Start two asynchronous consumers on the local broker, one for each
        // of the two shared queues, and keep track of how long it takes for
        // each of the consumers to receive all the messages.
        final CountDownLatch fastConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);
        final CountDownLatch slowConsumerLatch = new CountDownLatch(
                NUM_MESSAGES);

        final long startTimeMillis = System.currentTimeMillis();
        final AtomicLong fastConsumerTime = new AtomicLong();
        final AtomicLong slowConsumerTime = new AtomicLong();

        Thread fastWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    fastConsumerLatch.await();
                    fastConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        Thread slowWaitThread = new Thread() {
            @Override
            public void run() {
                try {
                    slowConsumerLatch.await();
                    slowConsumerTime.set(System.currentTimeMillis()
                            - startTimeMillis);
                } catch (InterruptedException ex) {
                    exceptions.add(ex);
                    Assert.fail(ex.getMessage());
                }
            }
        };

        fastWaitThread.start();
        slowWaitThread.start();

        createConsumer("broker0", fastDestination, fastConsumerLatch);
        MessageConsumer slowConsumer = createConsumer("broker0",
                slowDestination, slowConsumerLatch);
        MessageIdList messageIdList = brokers.get("broker0").consumers
                .get(slowConsumer);
        messageIdList.setProcessingDelay(SLOW_CONSUMER_DELAY_MILLIS);

        // Send the test messages to the local broker's shared queues. The
        // messages are either persistent or non-persistent to demonstrate the
        // difference between synchronous and asynchronous dispatch.
        persistentDelivery = false;
        sendMessages("broker1", fastDestination, NUM_MESSAGES);
        sendMessages("broker1", slowDestination, NUM_MESSAGES);

        fastWaitThread.join(TimeUnit.SECONDS.toMillis(60));
        slowWaitThread.join(TimeUnit.SECONDS.toMillis(60));

        assertTrue("no exceptions on the wait threads:" + exceptions,
                exceptions.isEmpty());

        LOG.info("Fast consumer duration (ms): " + fastConsumerTime.get());
        LOG.info("Slow consumer duration (ms): " + slowConsumerTime.get());

        assertTrue("fast time set", fastConsumerTime.get() > 0);
        assertTrue("slow time set", slowConsumerTime.get() > 0);

        // Verify the behaviour as described in the description of this class.
        Assert.assertTrue(fastConsumerTime.get() < slowConsumerTime.get() / 10);
    }


    /**
     * create a duplex network bridge from broker0 to broker1
     * add a topic consumer on broker0
     * set the setSendFailIfNoSpace() on the local broker.
     * create a SimpleDiscoveryAgent impl that tracks a network reconnect
     *
     * producer connects to broker1 and messages should be sent across the network to broker0
     *
     * Ensure broker0 will not send the  javax.jms.ResourceAllocationException (when broker0 runs out of space).
     * If the javax.jms.ResourceAllocationException is sent across the wire it will force the network connector
     * to shutdown
     *
     *
     * @throws Exception
     */

    public void testDuplexSendFailIfNoSpaceDoesNotBlockNetwork() throws Exception {

        // Consumer prefetch is disabled for broker1's consumers.
        final ActiveMQTopic destination = new ActiveMQTopic(
                NetworkBridgeProducerFlowControlTest.class.getSimpleName()
                        + ".duplexTest?consumer.prefetchSize=1");

        final int NUM_MESSAGES = 100;
        final long TEST_MESSAGE_SIZE = 1024;
        final long SLOW_CONSUMER_DELAY_MILLIS = 100;

        // Start a local and a remote broker.
        BrokerService localBroker = createBroker(new URI("broker:(tcp://localhost:0"
                + ")?brokerName=broker0&persistent=false&useJmx=true"));

        BrokerService remoteBroker = createBroker(new URI(
                "broker:(tcp://localhost:0"
                        + ")?brokerName=broker1&persistent=false&useJmx=true"));

        localBroker.getSystemUsage().setSendFailIfNoSpace(true);

        // Set a policy on the remote broker that limits the maximum size of the
        // slow shared queue.
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(5 * TEST_MESSAGE_SIZE);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(destination, policyEntry);
        localBroker.setDestinationPolicy(policyMap);

        // Create a duplex network bridge from the local broker to the remote broker
        // create a SimpleDiscoveryAgent impl that tracks a reconnect
        DiscoveryNetworkConnector discoveryNetworkConnector =  (DiscoveryNetworkConnector)bridgeBrokers("broker0", "broker1");
        URI originURI = discoveryNetworkConnector.getUri();
        discoveryNetworkConnector.setAlwaysSyncSend(true);
        discoveryNetworkConnector.setPrefetchSize(1);
        discoveryNetworkConnector.setDuplex(true);

        DummySimpleDiscoveryAgent dummySimpleDiscoveryAgent = new DummySimpleDiscoveryAgent();
        dummySimpleDiscoveryAgent.setServices(originURI.toString().substring(8,originURI.toString().lastIndexOf(')')));

        discoveryNetworkConnector.setDiscoveryAgent(dummySimpleDiscoveryAgent);

        startAllBrokers();
        waitForBridgeFormation();


        final CountDownLatch consumerLatch = new CountDownLatch(
                NUM_MESSAGES);


        //createConsumer("broker0", fastDestination, fastConsumerLatch);

        MessageConsumer consumer = createConsumer("broker0",
                destination, consumerLatch);

        MessageIdList messageIdList = brokers.get("broker0").consumers
                .get(consumer);

        messageIdList.setProcessingDelay(SLOW_CONSUMER_DELAY_MILLIS);

        // Send the test messages to the local broker's shared queues. The
        // messages are either persistent or non-persistent to demonstrate the
        // difference between synchronous and asynchronous dispatch.
        persistentDelivery = false;
        sendMessages("broker1", destination, NUM_MESSAGES);

        //wait for 5 seconds for the consumer to complete
        consumerLatch.await(5, TimeUnit.SECONDS);

        assertFalse("dummySimpleDiscoveryAgent.serviceFail has been invoked - should not have been",
                dummySimpleDiscoveryAgent.isServiceFailed);

    }

    /**
     * When the network connector fails it records the failure and delegates to real SimpleDiscoveryAgent
     */
    class DummySimpleDiscoveryAgent extends SimpleDiscoveryAgent {

        boolean isServiceFailed = false;

        public void serviceFailed(DiscoveryEvent devent) throws IOException {

            //should never get in here
            LOG.info("!!!!! DummySimpleDiscoveryAgent.serviceFailed() invoked with event:"+devent+"!!!!!!");
            isServiceFailed = true;
            super.serviceFailed(devent);

        }

    }
}
