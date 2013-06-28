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

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import junit.framework.Test;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4607Test extends JmsMultipleBrokersTestSupport implements UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4607Test.class);

    public static final int BROKER_COUNT = 3;
    public static final int CONSUMER_COUNT = 1;
    public static final int MESSAGE_COUNT = 0;
    public static final boolean CONDUIT = true;
    public static final int TIMEOUT = 20000;

    public boolean duplex = true;
    protected Map<String, MessageConsumer> consumerMap;
    Map<Thread, Throwable> unhandeledExceptions = new HashMap<Thread, Throwable>();

    private void assertNoUnhandeledExceptions() {
        for( Entry<Thread, Throwable> e: unhandeledExceptions.entrySet()) {
            LOG.error("Thread:" + e.getKey() + " Had unexpected: " + e.getValue());
        }
        assertTrue("There are no unhandelled exceptions, see: log for detail on: " + unhandeledExceptions,
                unhandeledExceptions.isEmpty());
    }

    public NetworkConnector bridge(String from, String to) throws Exception {
        NetworkConnector networkConnector = bridgeBrokers(from, to, true, -1, CONDUIT);
        networkConnector.setSuppressDuplicateQueueSubscriptions(true);
        networkConnector.setDecreaseNetworkConsumerPriority(true);
        networkConnector.setConsumerTTL(1);
        networkConnector.setDuplex(duplex);
        return networkConnector;
    }

    public static Test suite() {
        return suite(AMQ4607Test.class);
    }

    public void initCombos() {
        addCombinationValues("duplex", new Boolean[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testMigratingConsumer() throws Exception {
        bridge("Broker0", "Broker1");
        if (!duplex) bridge("Broker1", "Broker0");

        bridge("Broker1", "Broker2");
        if (!duplex) bridge("Broker2", "Broker1");

        bridge("Broker0", "Broker2");
        if (!duplex) bridge("Broker2", "Broker0");

        startAllBrokers();
        this.waitForBridgeFormation();

        Destination dest = createDestination("TEST.FOO", false);
        sendMessages("Broker0", dest, 1);

        for (int i=0; i< BROKER_COUNT; i++) {
            MessageConsumer messageConsumer = createConsumer("Broker" + i, dest, "DoNotConsume = 'true'");

            for (int J = 0; J < BROKER_COUNT; J++) {
                assertExactConsumersConnect("Broker" + J, dest, CONSUMER_COUNT, TIMEOUT);
            }

            assertNoUnhandeledExceptions();

            assertExactMessageCount("Broker" + i, dest, 1, TIMEOUT);

            messageConsumer.close();
            LOG.info("Check for no consumers..");
            for (int J = 0; J < BROKER_COUNT; J++) {
        	    assertExactConsumersConnect("Broker" + J, dest, 0, TIMEOUT);
            }
        }

        // now consume the message
        final String brokerId = "Broker2";
        MessageConsumer messageConsumer = createConsumer(brokerId, dest);
        assertTrue("Consumed ok", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokers.get(brokerId).allMessages.getMessageIds().size() == 1;
            }
        }));
        messageConsumer.close();

    }

    public void testMigratingConsumerFullCircle() throws Exception {
        bridge("Broker0", "Broker1");
        if (!duplex) bridge("Broker1", "Broker0");

        bridge("Broker1", "Broker2");
        if (!duplex) bridge("Broker2", "Broker1");

        bridge("Broker0", "Broker2");
        if (!duplex) bridge("Broker2", "Broker0");

        // allow full loop, immediate replay back to 0 from 2
        ConditionalNetworkBridgeFilterFactory conditionalNetworkBridgeFilterFactory = new ConditionalNetworkBridgeFilterFactory();
        conditionalNetworkBridgeFilterFactory.setReplayDelay(0);
        conditionalNetworkBridgeFilterFactory.setReplayWhenNoConsumers(true);
        brokers.get("Broker2").broker.getDestinationPolicy().getDefaultEntry().setNetworkBridgeFilterFactory(conditionalNetworkBridgeFilterFactory);
        startAllBrokers();
        this.waitForBridgeFormation();

        Destination dest = createDestination("TEST.FOO", false);

        sendMessages("Broker0", dest, 1);

        for (int i=0; i< BROKER_COUNT; i++) {
            MessageConsumer messageConsumer = createConsumer("Broker" + i, dest, "DoNotConsume = 'true'");

            for (int J = 0; J < BROKER_COUNT; J++) {
                assertExactConsumersConnect("Broker" + J, dest, CONSUMER_COUNT, TIMEOUT);
            }

            assertNoUnhandeledExceptions();

            // validate the message has been forwarded
            assertExactMessageCount("Broker" + i, dest, 1, TIMEOUT);

            messageConsumer.close();
            LOG.info("Check for no consumers..");
            for (int J = 0; J < BROKER_COUNT; J++) {
        	    assertExactConsumersConnect("Broker" + J, dest, 0, TIMEOUT);
            }
        }

        // now consume the message from the origin
        LOG.info("Consume from origin...");
        final String brokerId = "Broker0";
        MessageConsumer messageConsumer = createConsumer(brokerId, dest);
        assertTrue("Consumed ok", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokers.get(brokerId).allMessages.getMessageIds().size() == 1;
            }
        }));
        messageConsumer.close();

    }

    protected void assertExactMessageCount(final String brokerName, Destination destination, final int count, long timeout) throws Exception {
        ManagementContext context = brokers.get(brokerName).broker.getManagementContext();
        final QueueViewMBean queueViewMBean = (QueueViewMBean) context.newProxyInstance(brokers.get(brokerName).broker.getAdminView().getQueues()[0], QueueViewMBean.class, false);
        assertTrue("Excepected queue depth: " + count + " on: " + brokerName, Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                long currentCount = queueViewMBean.getQueueSize();
                LOG.info("On " + brokerName + " current queue size for " + queueViewMBean + ", " + currentCount);
                if (count != currentCount) {
                    LOG.info("Sub IDs: " + Arrays.asList(queueViewMBean.getSubscriptions()));
                }
                return currentCount == count;
            }
        }, timeout));
    }

    protected void assertExactConsumersConnect(final String brokerName, Destination destination, final int count, long timeout) throws Exception {
        final ManagementContext context = brokers.get(brokerName).broker.getManagementContext();
        assertTrue("Excepected consumers count: " + count + " on: " + brokerName, Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    QueueViewMBean queueViewMBean = (QueueViewMBean) context.newProxyInstance(brokers.get(brokerName).broker.getAdminView().getQueues()[0], QueueViewMBean.class, false);
                    long currentCount = queueViewMBean.getConsumerCount();
                    LOG.info("On " + brokerName + " current consumer count for " + queueViewMBean + ", " + currentCount);
                    if (count != currentCount) {
                        LOG.info("Sub IDs: " + Arrays.asList(queueViewMBean.getSubscriptions()));
                    }
                    return currentCount == count;
                } catch (Exception e) {
                    LOG.warn("Unexpected: " + e, e);
                    return false;
                }
            }
        }, timeout));
    }

    public void setUp() throws Exception {
        super.setUp();

        unhandeledExceptions.clear();
        Thread.setDefaultUncaughtExceptionHandler(this);
        
        // Setup n brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            createBroker(new URI("broker:(tcp://localhost:6161" + i + ")/Broker" + i + "?persistent=false&useJmx=true"));
        }

        consumerMap = new LinkedHashMap<String, MessageConsumer>();
    }

    @Override
    protected void configureBroker(BrokerService brokerService) {
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        brokerService.setDestinationPolicy(policyMap);
    }

    public void uncaughtException(Thread t, Throwable e) {
        synchronized(unhandeledExceptions) {
            unhandeledExceptions.put(t,e);
        }
    }
}
