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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A ConsumerControl that raises the prefetch of a wildcard or composite queue consumer
 * must dispatch the messages already waiting on the matching queues. Since AMQ-9475 the
 * broker no longer looks up the consumer's own destination, which does not exist for
 * these consumers, so it must wake the destinations the subscription is attached to.
 */
@RunWith(Parameterized.class)
@Category(ParallelTest.class)
public class ConsumerControlWakeupTest {

    @Parameters(name = "consumer={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "wakeup.>" }, { "wakeup.a,wakeup.b" } });
    }

    private final ActiveMQQueue consumerDestination;
    private BrokerService brokerService;
    private String connectionUri;

    public ConsumerControlWakeupTest(String consumerDestination) {
        this.consumerDestination = new ActiveMQQueue(consumerDestination);
    }

    @Before
    public void setUp() throws Exception {
        brokerService = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=false"));
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();
        brokerService.waitUntilStarted();
        connectionUri = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test(timeout = 60_000)
    public void testPrefetchIncreaseDispatchesWaitingMessages() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setWatchTopicAdvisories(false);
        factory.getPrefetchPolicy().setQueuePrefetch(0);
        try (var connection = (ActiveMQConnection) factory.createConnection()) {
            connection.start();
            var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(null);
            producer.send(new ActiveMQQueue("wakeup.a"), session.createTextMessage("a"));
            producer.send(new ActiveMQQueue("wakeup.b"), session.createTextMessage("b"));

            // prefetch zero and no pull, so registering the consumer dispatches nothing
            var consumer = (ActiveMQMessageConsumer) session.createConsumer(consumerDestination);
            var subscription = subscription(consumer);
            // let the dispatch pass started by adding the subscription finish first
            TimeUnit.MILLISECONDS.sleep(500);
            assertEquals("registration at prefetch zero dispatches nothing", 0, subscription.getDispatchedQueueSize());
            assertNull(brokerService.getBroker().getDestinationMap().get(consumerDestination));

            var control = new ConsumerControl();
            control.setConsumerId(consumer.getConsumerId());
            control.setDestination(consumerDestination);
            control.setPrefetch(10);
            connection.syncSendPacket(control);

            assertTrue("waiting messages dispatched after the prefetch increase",
                    Wait.waitFor(() -> subscription.getDispatchedQueueSize() == 2, 5000, 50));
            assertNull("the consumer's own destination is not created",
                    brokerService.getBroker().getDestinationMap().get(consumerDestination));
        }
    }

    private Subscription subscription(ActiveMQMessageConsumer consumer) throws Exception {
        var regionBroker = (RegionBroker) brokerService.getRegionBroker();
        return ((AbstractRegion) regionBroker.getQueueRegion()).getSubscriptions().get(consumer.getConsumerId());
    }
}
