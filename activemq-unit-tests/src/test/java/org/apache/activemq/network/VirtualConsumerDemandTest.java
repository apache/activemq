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
package org.apache.activemq.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationBroker;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationPlugin;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This test is to show that dynamicallyIncludedDestinations will work properly
 * when a network of brokers is configured to treat Virtual Destinations (Virtual topic and composite destination)
 * as demand.
 */
@RunWith(Parameterized.class)
public class VirtualConsumerDemandTest extends DynamicNetworkTestSupport {

    protected static final int MESSAGE_COUNT = 10;
    private static final Logger LOG = LoggerFactory.getLogger(VirtualConsumerDemandTest.class);


    /**
     * test params
     */
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                //not duplex, useVirtualDestSubsOnCreation
                {false, true},
                //duplex
                {true, false},
                {true, true},
                {false, false}
        });
    }


    protected JavaRuntimeConfigurationBroker runtimeBroker;
    protected String consumerName = "durableSubs";
    protected String testQueueName = "include.test.foo";

    private final boolean isDuplex;
    private final boolean isUseVirtualDestSubsOnCreation;


    public VirtualConsumerDemandTest(boolean isDuplex, boolean isUseVirtualDestSubsOnCreation) {
       // Assume.assumeTrue(
        super();
        this.isDuplex = isDuplex;
        this.isUseVirtualDestSubsOnCreation = isUseVirtualDestSubsOnCreation;
    }


    /**
     * Test that the creation of a virtual topic will cause demand
     * even without a consumer for the case of useVirtualDestSubsOnCreation == true
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testVirtualTopics() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer("VirtualTopic.>");

        MessageProducer includedProducer = localSession.createProducer(new ActiveMQTopic("VirtualTopic.include.test.bar"));
        MessageProducer includedProducer2 = localSession.createProducer(new ActiveMQTopic("VirtualTopic.include.test.bar2"));
        MessageProducer includedProducer3 = localSession.createProducer(new ActiveMQTopic("VirtualTopic.include.test.bar3"));
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(new ActiveMQTopic("VirtualTopic.include.test.bar")).getDestinationStatistics();
        final DestinationStatistics destinationStatistics2 = localBroker.getDestination(new ActiveMQTopic("VirtualTopic.include.test.bar2")).getDestinationStatistics();

        //No queue destination on the remote side so should not forward
        final DestinationStatistics destinationStatistics3 = localBroker.getDestination(new ActiveMQTopic("VirtualTopic.include.test.bar3")).getDestinationStatistics();

        //this will create the destination so messages accumulate
        final DestinationStatistics remoteStats = remoteBroker.getDestination(new ActiveMQQueue("Consumer.cons1.VirtualTopic.include.test.bar")).getDestinationStatistics();
        final DestinationStatistics remoteStats2 = remoteBroker.getDestination(new ActiveMQQueue("Consumer.cons1.VirtualTopic.include.test.bar2")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);
        waitForConsumerCount(destinationStatistics2, 1);

        includedProducer.send(test);
        includedProducer2.send(localSession.createTextMessage("test2"));
        includedProducer3.send(localSession.createTextMessage("test3"));

        //assert statistics
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        waitForDispatchFromLocalBroker(destinationStatistics2, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics2, 1);
        assertEquals("remote dest messages", 1, remoteStats.getMessages().getCount());
        assertEquals("remote dest messages", 1, remoteStats2.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 2);
        assertAdvisoryBrokerCounts(1,2,2);

        //should not have forwarded for 3rd topic
        Thread.sleep(1000);
        assertEquals("local broker dest stat dispatched", 0, destinationStatistics3.getDispatched().getCount());
    }



    /**
     * Test that the creation of a virtual topic with a consumer will cause
     * demand regardless of useVirtualDestSubsOnCreation
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testVirtualTopicWithConsumer() throws Exception {
        doSetUp(true, null);

       //use just the default virtual topic setup
        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer("VirtualTopic.>");

        MessageProducer includedProducer = localSession.createProducer(new ActiveMQTopic("VirtualTopic.include.test.bar"));
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(new ActiveMQTopic("VirtualTopic.include.test.bar")).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("Consumer.cons1.VirtualTopic.include.test.bar"));
        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        assertRemoteAdvisoryCount(advisoryConsumer, 2, 1);

        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }
    }


    /**
     * Test that when a consumer goes offline for a virtual topic, that messages still flow
     * to that queue if isUseVirtualDestSubsOnCreation is true
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testVirtualTopicWithConsumerGoOffline() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);
        //use just the default virtual topic setup
        doSetUp(true, null);
        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer("VirtualTopic.>");

        MessageProducer includedProducer = localSession.createProducer(new ActiveMQTopic("VirtualTopic.include.test.bar"));
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(new ActiveMQTopic("VirtualTopic.include.test.bar")).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("Consumer.cons1.VirtualTopic.include.test.bar"));
        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        //assert a message was forwarded
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        //close the consumer and send a second message
        bridgeConsumer.close();
        Thread.sleep(2000);
        includedProducer.send(test);

        //check that the message was forwarded
        waitForDispatchFromLocalBroker(destinationStatistics, 2);
        assertLocalBrokerStatistics(destinationStatistics, 2);

        //make sure that the message can be received
        MessageConsumer bridgeConsumer2 = remoteSession.createConsumer(new ActiveMQQueue("Consumer.cons1.VirtualTopic.include.test.bar"));
        assertNotNull(bridgeConsumer2.receive(5000));

        //should be 4 advisories...1 or the virtual destination creation to a queue,
        //2 for new consumers, and 1 for a closed consumer
        assertRemoteAdvisoryCount(advisoryConsumer, 4);
        assertAdvisoryBrokerCounts(1,2,1);
    }

    /**
     * This test shows that if isUseVirtualDestSubsOnCreation is true,
     * the creation of a composite destination that forwards to a Queue will create
     * a virtual consumer and cause demand so that the queue will accumulate messages
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testDynamicFlow() throws Exception {
        testDynamicFlow(false);
    }

    @Test(timeout = 60 * 1000)
    public void testDynamicFlowForceDurable() throws Exception {
        testDynamicFlow(true);
    }

    protected void testDynamicFlow(boolean forceDurable) throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null, true, forceDurable);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);
        assertNCDurableSubsCount(localBroker, included, forceDurable ? 1 : 0);
        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(1,1,1);
    }

    /**
     * Test that dynamic flow works for virtual destinations when a second composite
     * topic is included that forwards to the same queue, but is excluded from
     * being forwarded from the remote broker
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testSecondNonIncludedCompositeTopicForwardSameQueue() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a composite topic that isn't included
        CompositeTopic compositeTopic = createCompositeTopic("include.test.bar2",
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        Thread.sleep(2000);

        //add one that is included
        CompositeTopic compositeTopic2 = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic, compositeTopic2}, true);

        Thread.sleep(2000);
        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(2,2,2);

    }

    /**
     * Test that dynamic flow works for virtual destinations when a second composite
     * topic is included, but is excluded from
     * being forwarded from the remote broker
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testSecondNonIncludedCompositeTopic() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a composite topic that isn't included
        CompositeTopic compositeTopic = createCompositeTopic("include.test.bar2",
                new ActiveMQQueue("include.test.bar.bridge2"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        Thread.sleep(2000);

        //add one that is included
        CompositeTopic compositeTopic2 = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic, compositeTopic2}, true);

        Thread.sleep(2000);
        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(2,1,1);

    }

    /**
     * Test that no messages are forwarded when isUseVirtualDestSubsOnCreation is false
     * and there are no consumers
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testNoUseVirtualDestinationSubscriptionsOnCreation() throws Exception {
        Assume.assumeTrue(!isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        includedProducer.send(test);
        Thread.sleep(2000);

        waitForDispatchFromLocalBroker(destinationStatistics, 0);
        assertLocalBrokerStatistics(destinationStatistics, 0);
        assertEquals("remote dest messages", 0, remoteDestStatistics.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 0);
        assertAdvisoryBrokerCounts(1,0,0);

    }


    /**
     * Test that messages still flow when updating a composite topic to remove 1 of the
     * forwarded destinations, but keep the other one
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testTwoTargetsRemove1() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);
        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge" and "include.test.bar.bridge2"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"),
                new ActiveMQQueue("include.test.bar.bridge2"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics2 = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge2")).getDestinationStatistics();

        Thread.sleep(2000);
        //two advisory messages sent for each target when destinations are created
        assertRemoteAdvisoryCount(advisoryConsumer, 2);
        assertAdvisoryBrokerCounts(1,2,2);

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());
        assertEquals("remote2 dest messages", 1, remoteDestStatistics2.getMessages().getCount());

        compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);
        Thread.sleep(2000);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 2);
        assertLocalBrokerStatistics(destinationStatistics, 2);

        assertEquals("remote dest messages", 2, remoteDestStatistics.getMessages().getCount());
        assertEquals("remote2 dest messages", 1, remoteDestStatistics2.getMessages().getCount());

        //We delete 2, and re-add 1 target queue
        assertRemoteAdvisoryCount(advisoryConsumer, 3);
        assertAdvisoryBrokerCounts(1,1,1);

    }

    /**
     * Test that messages still flow after removing one of the destinations that is a target
     * but the other one sticks around
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testTwoTargetsRemove1Destination() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge" and "include.test.bar.bridge2"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"),
                new ActiveMQQueue("include.test.bar.bridge2"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics2 = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge2")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());
        assertEquals("remote2 dest messages", 1, remoteDestStatistics2.getMessages().getCount());

        remoteBroker.removeDestination(new ActiveMQQueue("include.test.bar.bridge2"));
        Thread.sleep(2000);
        //2 for each target queue destination in the virtual subscription
        //1 for the removal of a queue
        assertRemoteAdvisoryCount(advisoryConsumer, 3);
        assertAdvisoryBrokerCounts(1,1,1);

        includedProducer.send(test);

        //make sure messages are still forwarded even after 1 target was deleted
        waitForDispatchFromLocalBroker(destinationStatistics, 2);
        assertLocalBrokerStatistics(destinationStatistics, 2);

        assertEquals("remote dest messages", 2, remoteDestStatistics.getMessages().getCount());

        //1 because a send causes the queue to be recreated again which sends a new demand advisory
        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(1,2,2);

    }

    /**
     * This tests that having 2 composite destinations (1 included for dynamic flow and 1 not)
     * will allow messages to flow and that deleting 1 destination dosen't clear out the virtual
     * consumer map except for what should be cleared.
     *
     */
    @Test(timeout = 60 * 1000)
    public void testTwoCompositeTopicsRemove1() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge" and "include.test.bar.bridge2"
        CompositeTopic compositeTopic1 = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));
        CompositeTopic compositeTopic2 = createCompositeTopic(testTopicName + 2,
                new ActiveMQQueue("include.test.bar.bridge2"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic1, compositeTopic2}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        //verify there are 2 virtual destinations but only 1 consumer and broker dest
        assertAdvisoryBrokerCounts(2,1,1);
        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic1}, true);
        Thread.sleep(2000);
        //verify there is is only 1 virtual dest after deletion
        assertAdvisoryBrokerCounts(1,1,1);

        includedProducer.send(test);

        //make sure messages are still forwarded even after 1 composite topic was deleted
        waitForDispatchFromLocalBroker(destinationStatistics, 2);
        assertLocalBrokerStatistics(destinationStatistics, 2);
        assertEquals("remote dest messages", 2, remoteDestStatistics.getMessages().getCount());

    }

    /**
     * Test that demand is destroyed after removing both targets from the composite Topic
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testTwoTargetsRemoveBoth() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge" and "include.test.bar.bridge2"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"),
                new ActiveMQQueue("include.test.bar.bridge2"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics2 = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge2")).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());
        assertEquals("remote2 dest messages", 1, remoteDestStatistics2.getMessages().getCount());

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {}, true);
        Thread.sleep(2000);
        includedProducer.send(test);

        Thread.sleep(2000);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());
        assertEquals("remote2 dest messages", 1, remoteDestStatistics2.getMessages().getCount());

        //2 for each target queue destination in the virtual subscription
        //2 for the removal of the virtual destination, which requires 2 advisories because there are 2 targets
        assertRemoteAdvisoryCount(advisoryConsumer, 4);
        assertAdvisoryBrokerCounts(0,0,0);
    }

    /**
     * Test that dynamic flow works when the destination is created before the
     * virtual destination has been added to the broker
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testDestinationAddedFirst() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        remoteBroker.getBroker().addDestination(remoteBroker.getAdminConnectionContext(),
                new ActiveMQQueue("include.test.bar.bridge"), false);

        Thread.sleep(2000);
        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);

        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(1,1,1);
    }

    /**
     * This test shows that a consumer listening on the target of a composite destination will create
     * a virtual consumer and cause demand so that the consumer will receive messages, regardless
     * of whether isUseVirtualDestSubsOnCreation is true or false
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testWithConsumer() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                //should only be 1 because of conduit subs even though there is 2 consumers
                //for the case where isUseVirtualDestSubsOnCreation is true,
                //1 for the composite destination creation and 1 for the actual consumer
                return 1 == destinationStatistics.getConsumers().getCount();
            }
        });

        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        waitForDispatchFromLocalBroker(destinationStatistics, 1);

        assertLocalBrokerStatistics(destinationStatistics, 1);

        //if isUseVirtualDestSubsOnCreation is true we should have
        //two advisory consumer messages, else 1
        assertRemoteAdvisoryCount(advisoryConsumer, 2, 1);
        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }

    }

    /**
     * Test that demand still exists when only 1 of 2 consumers is removed from the
     * destination
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testWith2ConsumersRemove1() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));
        MessageConsumer bridgeConsumer2 = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));

        //should only be 1 because of conduit subs even though there is 2 consumers
        //for the case where isUseVirtualDestSubsOnCreation is true,
        //1 for the composite destination creation and 1 for the actual consumer
        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertTrue(bridgeConsumer.receive(5000) != null || bridgeConsumer2.receive(5000) != null);

        assertLocalBrokerStatistics(destinationStatistics, 1);

        bridgeConsumer2.close();

        includedProducer.send(test);

        //make sure the message is still forwarded
        waitForDispatchFromLocalBroker(destinationStatistics, 2);
        assertLocalBrokerStatistics(destinationStatistics, 2);
        assertNotNull(bridgeConsumer.receive(5000));

        assertRemoteAdvisoryCount(advisoryConsumer, 4, 3);
        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }
    }

    /**
     * Test that demand is removed after both consumers are removed when
     * isUseVirtualDestSubsOnCreation is false
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testWith2ConsumersRemoveBoth() throws Exception {
        Assume.assumeTrue(!isUseVirtualDestSubsOnCreation);

        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to queue "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));
        MessageConsumer bridgeConsumer2 = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));

        //should only be 1 because of conduit subs even though there is 2 consumers
        //for the case where isUseVirtualDestSubsOnCreation is true,
        //1 for the composite destination creation and 1 for the actual consumer
        waitForConsumerCount(destinationStatistics, 1);
        assertAdvisoryBrokerCounts(1,2,0);

        includedProducer.send(test);
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertTrue(bridgeConsumer.receive(5000) != null || bridgeConsumer2.receive(5000) != null);

        assertLocalBrokerStatistics(destinationStatistics, 1);

        bridgeConsumer.close();
        bridgeConsumer2.close();

        Thread.sleep(2000);
        includedProducer.send(test);
        Thread.sleep(2000);

        assertLocalBrokerStatistics(destinationStatistics, 1);

        //in this test, virtual destinations don't cause demand, only consumers on them
        //so we should have 2 create and 2 destroy
        assertRemoteAdvisoryCount(advisoryConsumer, 4);
        assertAdvisoryBrokerCounts(1,0,0);

    }

    /**
     * Show that messages won't be send for an excluded destination
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testExcluded() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages to an excluded destination
        CompositeTopic compositeTopic = createCompositeTopic("exclude.test.bar",
                new ActiveMQQueue("exclude.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(excluded);
        Message test = localSession.createTextMessage("test");
        Thread.sleep(1000);

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("exclude.test.bar.bridge"));
        Thread.sleep(2000);
        includedProducer.send(test);
        assertNull(bridgeConsumer.receive(5000));

        final DestinationStatistics destinationStatistics = localBroker.getDestination(excluded).getDestinationStatistics();
        assertEquals("broker consumer count", 0, destinationStatistics.getConsumers().getCount());

        assertLocalBrokerStatistics(destinationStatistics, 0);

        assertRemoteAdvisoryCount(advisoryConsumer, 0);
        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }

    }

    /**
     * Test that demand will be created when using a composite queue instead of a composite topic
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testSourceQueue() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getQueueVirtualDestinationAdvisoryConsumer(testQueueName);

        //configure a virtual destination that forwards messages from queue testQueueName
        //to topic "include.test.foo.bridge"
        CompositeQueue compositeQueue = createCompositeQueue(testQueueName,
                new ActiveMQQueue("include.test.foo.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeQueue}, true);

        MessageProducer includedProducer = localSession.createProducer(new ActiveMQQueue(testQueueName));
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");


        final DestinationStatistics destinationStatistics = localBroker.getDestination(new ActiveMQQueue(testQueueName)).getDestinationStatistics();
        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.foo.bridge"));
        waitForConsumerCount(destinationStatistics, 1);

        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        final DestinationStatistics remoteStats = remoteBroker.getDestination(new ActiveMQQueue(testQueueName)).getDestinationStatistics();

        waitForDispatchFromLocalBroker(destinationStatistics, 1);

        //should only be 1 because of conduit subs
        assertEquals("broker consumer count", 1, destinationStatistics.getConsumers().getCount());

        assertLocalBrokerStatistics(destinationStatistics, 1);

        //check remote stats - confirm the message isn't on the remote queue and was forwarded only
        //since that's how the composite queue was set up
        assertEquals("message count", 0, remoteStats.getMessages().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 2, 1);
        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }
    }


    /**
     * Test that the demand will be removed if the virtual destination is deleted
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testFlowRemoved() throws Exception {
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        doSetUp(true, new VirtualDestination[] {compositeTopic});

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //sleep to allow the route to be set up
        Thread.sleep(2000);

        remoteBroker.getBroker().addDestination(remoteBroker.getAdminConnectionContext(),
                new ActiveMQQueue("include.test.bar.bridge"), false);

        Thread.sleep(2000);

        //remove the virtual destinations after startup
        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        //assert that no message was received
        //by the time we get here, there is no more virtual destinations so this won't
        //trigger demand
        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));
        Thread.sleep(2000);
        includedProducer.send(test);
        assertNull(bridgeConsumer.receive(5000));

        assertRemoteAdvisoryCount(advisoryConsumer, 2, 0);
        assertAdvisoryBrokerCounts(0,0,0);
    }

    @Test(timeout = 60 * 1000)
    public void testReplay() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        doSetUp(true, new VirtualDestination[] {compositeTopic}, false, false);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        Thread.sleep(2000);

        remoteBroker.getBroker().addDestination(remoteBroker.getAdminConnectionContext(),
                new ActiveMQQueue("include.test.bar.bridge"), false);

        Thread.sleep(2000);

        //start the local broker after establishing the virtual topic to test replay
        localBroker.addNetworkConnector(connector);
        connector.start();

        Thread.sleep(2000);

        //there should be an extra advisory because of replay
        assertRemoteAdvisoryCount(advisoryConsumer, 2);
        assertAdvisoryBrokerCounts(1,1,1);
    }

    @Test(timeout = 60 * 1000)
    public void testReplayWithConsumer() throws Exception {

        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        doSetUp(true, new VirtualDestination[] {compositeTopic}, false, false);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        Thread.sleep(2000);

        remoteBroker.getBroker().addDestination(remoteBroker.getAdminConnectionContext(),
                new ActiveMQQueue("include.test.bar.bridge"), false);

        Thread.sleep(2000);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");
        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQQueue("include.test.bar.bridge"));
        Thread.sleep(2000);

        //start the local broker after establishing the virtual topic to test replay
        localBroker.addNetworkConnector(connector);
        connector.start();

        Thread.sleep(2000);
        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        //with isUseVirtualDestSubsOnCreation is true, there should be 4 advisories (2 replay)
        //with !isUseVirtualDestSubsOnCreation, there should be 2 advisories (1 replay)
        assertRemoteAdvisoryCount(advisoryConsumer, 4, 2);
        if (isUseVirtualDestSubsOnCreation) {
            assertAdvisoryBrokerCounts(1,2,1);
        } else {
            assertAdvisoryBrokerCounts(1,1,0);
        }
    }

    /**
     * Test that the demand will be removed if the virtual destination is deleted
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testRemovedIfNoConsumer() throws Exception {
        Assume.assumeTrue(isUseVirtualDestSubsOnCreation);

        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));

        doSetUp(true, new VirtualDestination[] {compositeTopic});

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        Thread.sleep(2000);

        //destination creation will trigger the advisory since the virtual topic exists
        final DestinationStatistics destinationStatistics =
                localBroker.getDestination(new ActiveMQQueue(testQueueName)).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        Thread.sleep(2000);
        assertAdvisoryBrokerCounts(1,1,1);

        //remove the virtual destinations after startup, will trigger a remove advisory
        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");
        includedProducer.send(test);

        assertEquals("broker consumer count", 0, destinationStatistics.getConsumers().getCount());
        assertLocalBrokerStatistics(destinationStatistics, 0);
        assertEquals("remote dest messages", 0, remoteDestStatistics.getMessages().getCount());

        //one add and one remove advisory
        assertRemoteAdvisoryCount(advisoryConsumer, 2);
        assertAdvisoryBrokerCounts(0,0,0);
    }


    /**
     * Test that demand is created when the target of the compositeTopic is another topic
     * and a consumer comes online
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testToTopic() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to topic "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQTopic("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        MessageConsumer bridgeConsumer = remoteSession.createConsumer(new ActiveMQTopic("include.test.bar.bridge"));
        Thread.sleep(2000);
        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(1,1,0);
    }

    /**
     * Test that demand is NOT created when the target of the compositeTopic is another topic
     * and there are no consumers since the existience of a topic shouldn't case demand without
     * a consumer or durable on it
     *
     * @throws Exception
     */
    @Test(timeout = 60 * 1000)
    public void testToTopicNoConsumer() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to topic "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQTopic("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");
        includedProducer.send(test);

        final DestinationStatistics destinationStatistics = localBroker.getDestination(excluded).getDestinationStatistics();
        assertEquals("broker consumer count", 0, destinationStatistics.getConsumers().getCount());
        assertLocalBrokerStatistics(destinationStatistics, 0);

        assertRemoteAdvisoryCount(advisoryConsumer, 0);
        assertAdvisoryBrokerCounts(1,0,0);
    }

    /**
     * Test that demand will be created because of the existing of a durable subscription
     * created on a topic that is the target of a compositeTopic
     */
    @Test(timeout = 60 * 1000)
    public void testToTopicWithDurable() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to topic "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQTopic("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");


        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        MessageConsumer bridgeConsumer = remoteSession.createDurableSubscriber(
                new ActiveMQTopic("include.test.bar.bridge"), "sub1");
        Thread.sleep(2000);
        includedProducer.send(test);
        assertNotNull(bridgeConsumer.receive(5000));

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 1 == destinationStatistics.getDequeues().getCount();
            }
        });

        assertEquals("broker dest stat dispatched", 1, destinationStatistics.getDispatched().getCount());
        assertEquals("broker dest stat dequeues", 1, destinationStatistics.getDequeues().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 1);
        assertAdvisoryBrokerCounts(1,1,0);

    }

    /**
     * Test that messages still flow to the durable subscription on the forwarded
     * destination even if it is offline
     */
    @Test(timeout = 60 * 1000)
    public void testToTopicWithDurableOffline() throws Exception {
        doSetUp(true, null);

        MessageConsumer advisoryConsumer = getVirtualDestinationAdvisoryConsumer(testTopicName);

        //configure a virtual destination that forwards messages from topic testQueueName
        //to topic "include.test.bar.bridge"
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQTopic("include.test.bar.bridge"));

        runtimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Thread.sleep(2000);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        //create a durable subscription and go offline
        MessageConsumer bridgeConsumer = remoteSession.createDurableSubscriber(
                new ActiveMQTopic("include.test.bar.bridge"), "sub1");
        bridgeConsumer.close();
        Thread.sleep(2000);
        includedProducer.send(test);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 1 == destinationStatistics.getDequeues().getCount() &&
                        destinationStatistics.getDispatched().getCount() == 1;
            }
        });

        //offline durable should still get receive the message over the bridge and ack
        assertEquals("broker dest stat dispatched", 1, destinationStatistics.getDispatched().getCount());
        assertEquals("broker dest stat dequeues", 1, destinationStatistics.getDequeues().getCount());

        //reconnect to receive the message
        MessageConsumer bridgeConsumer2 = remoteSession.createDurableSubscriber(
                new ActiveMQTopic("include.test.bar.bridge"), "sub1");
        assertNotNull(bridgeConsumer2.receive(5000));

        Thread.sleep(2000);
        //make sure stats did not change
        assertEquals("broker dest stat dispatched", 1, destinationStatistics.getDispatched().getCount());
        assertEquals("broker dest stat dequeues", 1, destinationStatistics.getDequeues().getCount());

        assertRemoteAdvisoryCount(advisoryConsumer, 3);
        assertAdvisoryBrokerCounts(1,1,0);

    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    protected void doSetUp(boolean deleteAllMessages,
            VirtualDestination[] remoteVirtualDests) throws Exception {
        doSetUp(deleteAllMessages, remoteVirtualDests, true, false);
    }

    protected void doSetUp(boolean deleteAllMessages,
            VirtualDestination[] remoteVirtualDests, boolean startNetworkConnector, boolean forceDurable) throws Exception {
        remoteBroker = createRemoteBroker(isUseVirtualDestSubsOnCreation, remoteVirtualDests);
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        localBroker = createLocalBroker(startNetworkConnector, forceDurable);
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection();
        localConnection.setClientID("clientId");
        localConnection.start();
        URI remoteURI = remoteBroker.getVmConnectorURI();
        fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("clientId");
        remoteConnection.start();
        included = new ActiveMQTopic(testTopicName);
        excluded = new ActiveMQTopic("exclude.test.bar");
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }


    protected NetworkConnector connector;
    protected BrokerService createLocalBroker(boolean startNetworkConnector, boolean forceDurable) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setMonitorConnectionSplits(true);
        brokerService.setDataDirectoryFile(tempFolder.newFolder());
        brokerService.setBrokerName("localBroker");

        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI = transportConnectors.get(0).getConnectUri();
        String uri = "static:(" + remoteURI + ")";
        connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("networkConnector");
        connector.setDynamicOnly(false);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setDuplex(isDuplex);
        connector.setUseVirtualDestSubs(true);
        connector.setDynamicallyIncludedDestinations(Lists.newArrayList(new ActiveMQQueue(testQueueName),
                new ActiveMQTopic(testTopicName + (forceDurable ? "?forceDurable=true" : "")), new ActiveMQTopic("VirtualTopic.>")));
        connector.setExcludedDestinations(Lists.newArrayList(new ActiveMQQueue("exclude.test.foo"),
                new ActiveMQTopic("exclude.test.bar")));

        if (startNetworkConnector) {
            brokerService.addNetworkConnector(connector);
        }

        brokerService.addConnector("tcp://localhost:0");

        return brokerService;
    }

    protected AdvisoryBroker remoteAdvisoryBroker;
    protected BrokerService createRemoteBroker(boolean isUsevirtualDestinationSubscriptionsOnCreation,
            VirtualDestination[] remoteVirtualDests) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(tempFolder.newFolder());
        brokerService.setPlugins(new BrokerPlugin[] {new JavaRuntimeConfigurationPlugin()});
        brokerService.setUseVirtualDestSubs(true);
        brokerService.setUseVirtualDestSubsOnCreation(isUsevirtualDestinationSubscriptionsOnCreation);

        //apply interceptor before getting the broker, which will cause it to be built
        if (remoteVirtualDests != null) {
            VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
            interceptor.setVirtualDestinations(remoteVirtualDests);
            brokerService.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        }

        runtimeBroker = (JavaRuntimeConfigurationBroker)
                brokerService.getBroker().getAdaptor(JavaRuntimeConfigurationBroker.class);
        remoteAdvisoryBroker = (AdvisoryBroker)
                brokerService.getBroker().getAdaptor(AdvisoryBroker.class);

        brokerService.addConnector("tcp://localhost:0");

        return brokerService;
    }

    protected CompositeTopic createCompositeTopic(String name, ActiveMQDestination...forwardTo) {
        CompositeTopic compositeTopic = new CompositeTopic();
        compositeTopic.setName(name);
        compositeTopic.setForwardOnly(true);
        compositeTopic.setForwardTo( Lists.newArrayList(forwardTo));

        return compositeTopic;
    }

    protected CompositeQueue createCompositeQueue(String name, ActiveMQDestination...forwardTo) {
        CompositeQueue compositeQueue = new CompositeQueue();
        compositeQueue.setName(name);
        compositeQueue.setForwardOnly(true);
        compositeQueue.setForwardTo( Lists.newArrayList(forwardTo));

        return compositeQueue;
    }

    protected MessageConsumer getVirtualDestinationAdvisoryConsumer(String topic) throws JMSException {
        return remoteSession.createConsumer(AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(
                new ActiveMQTopic(topic)));
    }

    protected MessageConsumer getQueueVirtualDestinationAdvisoryConsumer(String queue) throws JMSException {
        return remoteSession.createConsumer(AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(
                new ActiveMQQueue(queue)));
    }

    protected void assertRemoteAdvisoryCount(final MessageConsumer advisoryConsumer, final int count) throws JMSException {
        int available = 0;
        ActiveMQMessage message = null;
        while ((message = (ActiveMQMessage) advisoryConsumer.receive(1000)) != null) {
            available++;
            LOG.info("advisory data structure: {}", message.getDataStructure());
        }
        assertEquals(count, available);
    }

    protected void assertRemoteAdvisoryCount(final MessageConsumer advisoryConsumer,
            final int isSubOnCreationCount, final int isNotSubOnCreationCount) throws JMSException {
        if (isUseVirtualDestSubsOnCreation) {
            assertRemoteAdvisoryCount(advisoryConsumer, isSubOnCreationCount);
        } else {
            assertRemoteAdvisoryCount(advisoryConsumer, isNotSubOnCreationCount);
        }
    }

    @SuppressWarnings("unchecked")
    protected void assertAdvisoryBrokerCounts(int virtualDestinationsCount,
            int virtualDestinationConsumersCount, int brokerConsumerDestsCount) throws Exception {

        Field virtualDestinationsField = AdvisoryBroker.class.getDeclaredField("virtualDestinations");
        Field virtualDestinationConsumersField = AdvisoryBroker.class.getDeclaredField("virtualDestinationConsumers");
        Field brokerConsumerDestsField = AdvisoryBroker.class.getDeclaredField("brokerConsumerDests");

        virtualDestinationsField.setAccessible(true);
        virtualDestinationConsumersField.setAccessible(true);
        brokerConsumerDestsField.setAccessible(true);

        Set<VirtualDestination> virtualDestinations = (Set<VirtualDestination>)
                virtualDestinationsField.get(remoteAdvisoryBroker);

        ConcurrentMap<ConsumerInfo, VirtualDestination> virtualDestinationConsumers =
                (ConcurrentMap<ConsumerInfo, VirtualDestination>)
                    virtualDestinationConsumersField.get(remoteAdvisoryBroker);

        ConcurrentMap<Object, ConsumerInfo> brokerConsumerDests =
                (ConcurrentMap<Object, ConsumerInfo>)
                brokerConsumerDestsField.get(remoteAdvisoryBroker);

        assertEquals(virtualDestinationsCount, virtualDestinations.size());
        assertEquals(virtualDestinationConsumersCount, virtualDestinationConsumers.size());
        assertEquals(brokerConsumerDestsCount, brokerConsumerDests.size());
    }

}
