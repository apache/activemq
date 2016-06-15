/*
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

import java.net.URI;
import java.util.concurrent.Semaphore;

import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DemandForwardingBridgeSupport;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;

/**
 * This test demonstrates a bug in {@link DemandForwardingBridgeSupport} when
 * bridges are VM-to-VM. Specifically, memory usage from the local broker is
 * manipulated by the remote broker.
 */
public class AMQ4147Test extends JmsMultipleBrokersTestSupport {

    /**
     * This test demonstrates the bug: namely, when a message is bridged over
     * the VMTransport, its memory usage continues to refer to the originating
     * broker. As a result, memory usage is never accounted for on the remote
     * broker, and the local broker's memory usage is only decreased once the
     * message is consumed on the remote broker.
     */
    public void testVMTransportRemoteMemoryUsage() throws Exception {
        BrokerService broker1 = createBroker(new URI(
                "broker:(vm://broker1)/broker1?persistent=false&useJmx=false"));

        BrokerService broker2 = createBroker(new URI(
                "broker:(vm://broker2)/broker2?persistent=false&useJmx=false"));

        startAllBrokers();

        // Forward messages from broker1 to broker2 over the VM transport.
        bridgeBrokers("broker1", "broker2").start();

        // Verify that broker1 and broker2's test queues have no memory usage.
        ActiveMQDestination testQueue = createDestination(
                AMQ4147Test.class.getSimpleName() + ".queue", false);
        final Destination broker1TestQueue = broker1.getDestination(testQueue);
        final Destination broker2TestQueue = broker2.getDestination(testQueue);

        assertEquals(0, broker1TestQueue.getMemoryUsage().getUsage());
        assertEquals(0, broker2TestQueue.getMemoryUsage().getUsage());

        // Produce a message to broker1's test queue and verify that broker1's
        // memory usage has increased, but broker2 still has no memory usage.
        sendMessages("broker1", testQueue, 1);
        assertTrue(broker1TestQueue.getMemoryUsage().getUsage() > 0);
        assertEquals(0, broker2TestQueue.getMemoryUsage().getUsage());

        // Create a consumer on broker2 that is synchronized to allow detection
        // of "in flight" messages to the consumer.
        MessageIdList broker2Messages = getBrokerMessages("broker2");
        final Semaphore consumerReady = new Semaphore(0);
        final Semaphore consumerProceed = new Semaphore(0);

        broker2Messages.setParent(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                consumerReady.release();
                try {
                    consumerProceed.acquire();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        createConsumer("broker2", testQueue);

        // Verify that when broker2's consumer receives the message, the memory
        // usage has moved broker1 to broker2. The first assertion is expected
        // to fail due to the bug; the try/finally ensures the consumer is
        // released prior to failure so that the broker can shut down.
        consumerReady.acquire();

        try {
            assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker1TestQueue.getMemoryUsage().getUsage() == 0;
                }
            }));
            assertTrue(broker2TestQueue.getMemoryUsage().getUsage() > 0);
        } finally {
            // Consume the message and verify that there is no more memory usage.
            consumerProceed.release();
        }

        assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker1TestQueue.getMemoryUsage().getUsage() == 0;
            }
        }));
        assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker2TestQueue.getMemoryUsage().getUsage() == 0;
            }
        }));
    }

    /**
     * This test demonstrates that the bug is VMTransport-specific and does not
     * occur when bridges occur using other protocols.
     */
    public void testTcpTransportRemoteMemoryUsage() throws Exception {
        BrokerService broker1 = createBroker(new URI(
                "broker:(vm://broker1)/broker1?persistent=false&useJmx=false"));

        BrokerService broker2 = createBroker(new URI(
                "broker:(tcp://localhost:0)/broker2?persistent=false&useJmx=false"));

        startAllBrokers();

        // Forward messages from broker1 to broker2 over the TCP transport.
        bridgeBrokers("broker1", "broker2").start();

        // Verify that broker1 and broker2's test queues have no memory usage.
        ActiveMQDestination testQueue = createDestination(
                AMQ4147Test.class.getSimpleName() + ".queue", false);
        final Destination broker1TestQueue = broker1.getDestination(testQueue);
        final Destination broker2TestQueue = broker2.getDestination(testQueue);

        assertEquals(0, broker1TestQueue.getMemoryUsage().getUsage());
        assertEquals(0, broker2TestQueue.getMemoryUsage().getUsage());

        // Produce a message to broker1's test queue and verify that broker1's
        // memory usage has increased, but broker2 still has no memory usage.
        sendMessages("broker1", testQueue, 1);
        assertTrue(broker1TestQueue.getMemoryUsage().getUsage() > 0);
        assertEquals(0, broker2TestQueue.getMemoryUsage().getUsage());

        // Create a consumer on broker2 that is synchronized to allow detection
        // of "in flight" messages to the consumer.
        MessageIdList broker2Messages = getBrokerMessages("broker2");
        final Semaphore consumerReady = new Semaphore(0);
        final Semaphore consumerProceed = new Semaphore(0);

        broker2Messages.setParent(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                consumerReady.release();
                try {
                    consumerProceed.acquire();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        createConsumer("broker2", testQueue);

        // Verify that when broker2's consumer receives the message, the memory
        // usage has moved broker1 to broker2.
        consumerReady.acquire();

        try {
            assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker1TestQueue.getMemoryUsage().getUsage() == 0;
                }
            }));
            assertTrue(broker2TestQueue.getMemoryUsage().getUsage() > 0);
        } finally {
            // Consume the message and verify that there is no more memory usage.
            consumerProceed.release();
        }

        // Pause to allow ACK to be processed.
        assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker1TestQueue.getMemoryUsage().getUsage() == 0;
            }
        }));
        assertTrue("Memory Usage Should be Zero: ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker2TestQueue.getMemoryUsage().getUsage() == 0;
            }
        }));
    }
}
