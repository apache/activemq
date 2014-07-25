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

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.management.ObjectName;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerifyNetworkConsumersDisconnectTest extends JmsMultipleBrokersTestSupport implements UncaughtExceptionHandler {
    public static final int BROKER_COUNT = 3;
    public static final int CONSUMER_COUNT = 5;
    public static final int MESSAGE_COUNT = 0;
    public static final boolean DUPLEX = false;
    public static final boolean CONDUIT = true;

    public static final int NETWORK_TTL = 6;
    private static final Logger LOG = LoggerFactory.getLogger(VerifyNetworkConsumersDisconnectTest.class);
    public static final int TIMEOUT = 30000;

    protected Map<String, MessageConsumer> consumerMap;
    Map<Thread, Throwable> unhandledExceptions = new HashMap<Thread, Throwable>();

    private void assertNoUnhandledExceptions() {
        for( Entry<Thread, Throwable> e: unhandledExceptions.entrySet()) {
            LOG.error("Thread:" + e.getKey() + " Had unexpected: " + e.getValue());
        }
        assertTrue("There are no unhandled exceptions, see: log for detail on: " + unhandledExceptions,
                unhandledExceptions.isEmpty());
    }

    public NetworkConnector bridge(String from, String to) throws Exception {
        NetworkConnector networkConnector = bridgeBrokers(from, to, true, NETWORK_TTL, CONDUIT);
        networkConnector.setSuppressDuplicateQueueSubscriptions(true);
        networkConnector.setDecreaseNetworkConsumerPriority(true);
        networkConnector.setDuplex(DUPLEX);
        return networkConnector;
    }

    /*why conduit proxy proxy consumers gets us in a knot w.r.t removal
    DC-7 for CA-9, add DB-15, remove CA-9, add CB-8
    CB-8 add DC-7
    CB-8 - why not dead?
    CB-8 for BA-6, add BD-15, remove BA-6
    BD-15 for DA-11, add DC-7
    */
    public void testConsumerOnEachBroker() throws Exception {
        bridge("Broker0", "Broker1");
        if (!DUPLEX) bridge("Broker1", "Broker0");

        bridge("Broker1", "Broker2");
        if (!DUPLEX) bridge("Broker2", "Broker1");

        startAllBrokers();
        waitForBridgeFormation(brokers.get("Broker0").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker2").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker1").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker1").broker, 1, 1);

        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        for (int i = 0; i < BROKER_COUNT; i++) {
            consumerMap.put("Consumer:" + i + ":0", createConsumer("Broker" + i, dest));
        }

        assertExactConsumersConnect("Broker0", 3, 1, TIMEOUT);
        assertExactConsumersConnect("Broker2", 3, 1, TIMEOUT);
        // piggy in the middle
        assertExactConsumersConnect("Broker1", 3, 1, TIMEOUT);

        assertNoUnhandledExceptions();

        LOG.info("Complete the mesh - 0->2");

        // shorter route
        NetworkConnector nc = bridge("Broker0", "Broker2");
        nc.setBrokerName("Broker0");
        nc.start();


        if (!DUPLEX) {
            LOG.info("... complete the mesh - 2->0");
            nc = bridge("Broker2", "Broker0");
            nc.setBrokerName("Broker2");
            nc.start();
        }

        // wait for consumers to get propagated
        for (int i = 0; i < BROKER_COUNT; i++) {
        	assertExactConsumersConnect("Broker" + i, 3, 1, TIMEOUT);
        }

        // reverse order close
        consumerMap.get("Consumer:" + 2 + ":0").close();
        TimeUnit.SECONDS.sleep(1);
        consumerMap.get("Consumer:" + 1 + ":0").close();
        TimeUnit.SECONDS.sleep(1);
        consumerMap.get("Consumer:" + 0 + ":0").close();

        LOG.info("Check for no consumers..");
        for (int i = 0; i < BROKER_COUNT; i++) {
        	assertExactConsumersConnect("Broker" + i, 0, 0, TIMEOUT);
        }

    }

    public void testXConsumerOnEachBroker() throws Exception {
        bridge("Broker0", "Broker1");
        if (!DUPLEX) bridge("Broker1", "Broker0");

        bridge("Broker1", "Broker2");
        if (!DUPLEX) bridge("Broker2", "Broker1");

        startAllBrokers();

        waitForBridgeFormation(brokers.get("Broker0").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker2").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker1").broker, 1, 0);
        waitForBridgeFormation(brokers.get("Broker1").broker, 1, 1);

        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        for (int i = 0; i < BROKER_COUNT; i++) {
            for (int j=0; j< CONSUMER_COUNT; j++)
            consumerMap.put("Consumer:" + i + ":" + j, createConsumer("Broker" + i, dest));
        }

        for (int i = 0; i < BROKER_COUNT; i++) {
            assertExactConsumersConnect("Broker" + i, CONSUMER_COUNT + (BROKER_COUNT -1), 1, TIMEOUT);
        }

        assertNoUnhandledExceptions();

        LOG.info("Complete the mesh - 0->2");

        // shorter route
        NetworkConnector nc = bridge("Broker0", "Broker2");
        nc.setBrokerName("Broker0");
        nc.start();

        waitForBridgeFormation(brokers.get("Broker0").broker, 1, 1);

        if (!DUPLEX) {
            LOG.info("... complete the mesh - 2->0");
            nc = bridge("Broker2", "Broker0");
            nc.setBrokerName("Broker2");
            nc.start();
        }

        waitForBridgeFormation(brokers.get("Broker2").broker, 1, 1);

        for (int i = 0; i < BROKER_COUNT; i++) {
            assertExactConsumersConnect("Broker" + i, CONSUMER_COUNT + (BROKER_COUNT -1), 1, TIMEOUT);
        }

        // reverse order close
        for (int i=0; i<CONSUMER_COUNT; i++) {
            consumerMap.get("Consumer:" + 2 + ":" + i).close();
            TimeUnit.SECONDS.sleep(1);
            consumerMap.get("Consumer:" + 1 + ":" + i).close();
            TimeUnit.SECONDS.sleep(1);
            consumerMap.get("Consumer:" + 0 + ":" + i).close();
        }

        LOG.info("Check for no consumers..");
        for (int i = 0; i < BROKER_COUNT; i++) {
        	assertExactConsumersConnect("Broker" + i, 0, 0, TIMEOUT);
        }

    }

    protected void assertExactConsumersConnect(final String brokerName, final int count, final int numChecks, long timeout) throws Exception {
        final ManagementContext context = brokers.get(brokerName).broker.getManagementContext();
        final AtomicInteger stability = new AtomicInteger(0);
        assertTrue("Expected consumers count: " + count + " on: " + brokerName, Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    QueueViewMBean queueViewMBean = (QueueViewMBean) context.newProxyInstance(brokers.get(brokerName).broker.getAdminView().getQueues()[0], QueueViewMBean.class, false);
                    long currentCount = queueViewMBean.getConsumerCount();
                    LOG.info("On " + brokerName + " current consumer count for " + queueViewMBean + ", " + currentCount);
                    LinkedList<String> consumerIds = new LinkedList<String>();
                    for (ObjectName objectName : queueViewMBean.getSubscriptions()) {
                        consumerIds.add(objectName.getKeyProperty("consumerId"));
                    }
                    LOG.info("Sub IDs: " + consumerIds);
                    if (currentCount == count) {
                        stability.incrementAndGet();
                    } else {
                        stability.set(0);
                    }
                    return stability.get() > numChecks;
                } catch (Exception e) {
                    LOG.warn(": ", e);
                    return false;
                }
            }
        }, timeout));
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();

        unhandledExceptions.clear();
        Thread.setDefaultUncaughtExceptionHandler(this);
        
        // Setup n brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            createBroker(new URI("broker:(tcp://localhost:6161" + i + ")/Broker" + i + "?persistent=false&useJmx=true&brokerId=Broker" + i));
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
        synchronized(unhandledExceptions) {
            unhandledExceptions.put(t, e);
        }
    }
}
