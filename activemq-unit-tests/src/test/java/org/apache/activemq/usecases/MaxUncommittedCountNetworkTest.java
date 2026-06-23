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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.JMSContext;
import jakarta.jms.Message;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Verifies that maxUncommittedCount does not break non-transactional
 * message forwarding across network connectors.
 *
 * Prior to the fix, network-forwarded messages (which have a null
 * transactionId) caused a NullPointerException in
 * TransactionBroker.verifyUncommittedCount() when maxUncommittedCount
 * was set to a positive value.
 */
@RunWith(value = Parameterized.class)
@Category(ParallelTest.class)
public class MaxUncommittedCountNetworkTest {

    private BrokerService brokerA;
    private BrokerService brokerB;

    @Parameterized.Parameters(name="transacted={0}")
    public static Collection<Object> data() {
        return Arrays.asList(new Object[] { true, false });
    }

    private final boolean transacted;

    public MaxUncommittedCountNetworkTest(boolean transacted) {
        this.transacted = transacted;
    }

    @Before
    public void setUp() throws Exception {
        brokerA = new BrokerService();
        brokerA.setBrokerName("brokerA");
        brokerA.setUseJmx(false);
        brokerA.setPersistent(false);
        brokerA.addConnector("tcp://localhost:0");
        brokerA.setMaxUncommittedCount(10);
        brokerA.start();
        brokerA.waitUntilStarted();

        var brokerAUri = brokerA.getTransportConnectorByScheme("tcp").getPublishableConnectString();

        brokerB = new BrokerService();
        brokerB.setBrokerName("brokerB");
        brokerB.setUseJmx(false);
        brokerB.setPersistent(false);
        brokerB.addConnector("tcp://localhost:0");
        brokerB.setMaxUncommittedCount(10);

        var nc = brokerB.addNetworkConnector("static:(" + brokerAUri + ")");
        nc.setDuplex(true);

        brokerB.start();
        brokerB.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerB != null) {
            brokerB.stop();
            brokerB.waitUntilStopped();
        }
        if (brokerA != null) {
            brokerA.stop();
            brokerA.waitUntilStopped();
        }
    }

    @Test
    public void testSendAcrossNetworkConnector() throws Exception {
        var queueName = "test.max.uncommited.network.tx." + transacted;
        int messageCount = transacted ? 5 : 20;

        var brokerAUri = brokerA.getTransportConnectorByScheme("tcp").getPublishableConnectString();
        var brokerBUri = brokerB.getTransportConnectorByScheme("tcp").getPublishableConnectString();
        var consumerFactory = new ActiveMQConnectionFactory(brokerBUri);
        var producerFactory = new ActiveMQConnectionFactory(brokerAUri);

        try (final var consumerContext = consumerFactory.createContext(transacted ? JMSContext.SESSION_TRANSACTED : JMSContext.AUTO_ACKNOWLEDGE);
             final var consumer = consumerContext.createConsumer(consumerContext.createQueue(queueName));
             final var producerContext = producerFactory.createContext(transacted ? JMSContext.SESSION_TRANSACTED : JMSContext.AUTO_ACKNOWLEDGE)) {

            assertTrue("Remote consumer should come online",
                    Wait.waitFor(() -> {
                        return !brokerA.getDestination(new ActiveMQQueue(queueName)).getConsumers().isEmpty();
                    }, 2_000L, 10L));


            for (var i = 0; i < messageCount; i++) {
                var msg = producerContext
                        .createProducer()
                            .send(producerContext.createQueue(queueName),
                                  producerContext.createTextMessage("msg-" + i));
            }

            if (transacted) {
                producerContext.commit();
            }

            final var count = new AtomicInteger(0);
            assertTrue("All messages should arrive across the network connector",
                    Wait.waitFor(() -> {
                        Message msg;
                        while ((msg = consumer.receive(1_000L)) != null) {
                            if (transacted) {
                                consumerContext.commit();
                            }
                            count.incrementAndGet();
                        }
                        return count.get() >= messageCount;
                    }, 15_000L, 10L));

            assertEquals(messageCount, count.get());
        }
    }
}
