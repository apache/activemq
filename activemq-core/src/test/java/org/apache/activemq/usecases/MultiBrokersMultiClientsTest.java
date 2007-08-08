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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class MultiBrokersMultiClientsTest extends JmsMultipleBrokersTestSupport {
    public static final int BROKER_COUNT = 2; // number of brokers to network
    public static final int CONSUMER_COUNT = 3; // consumers per broker
    public static final int PRODUCER_COUNT = 3; // producers per broker
    public static final int MESSAGE_COUNT = 10; // messages per producer

    protected Map consumerMap;

    public void testTopicAllConnected() throws Exception {
        bridgeAllBrokers();
        startAllBrokers();

        // Setup topic destination
        Destination dest = createDestination("TEST.FOO", true);

        CountDownLatch latch = new CountDownLatch(BROKER_COUNT * PRODUCER_COUNT * BROKER_COUNT * CONSUMER_COUNT * MESSAGE_COUNT);

        // Setup consumers
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < CONSUMER_COUNT; j++) {
                consumerMap.put("Consumer:" + i + ":" + j, createConsumer("Broker" + i, dest, latch));
            }
        }

        // wait for consumers to get propagated
        Thread.sleep(5000);

        // Send messages
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < PRODUCER_COUNT; j++) {
                sendMessages("Broker" + i, dest, MESSAGE_COUNT);
            }
        }

        assertTrue("Missing " + latch.getCount() + " messages", latch.await(30, TimeUnit.SECONDS));

        // Get message count
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < CONSUMER_COUNT; j++) {
                MessageIdList msgs = getConsumerMessages("Broker" + i, (MessageConsumer)consumerMap.get("Consumer:" + i + ":" + j));
                assertEquals(BROKER_COUNT * PRODUCER_COUNT * MESSAGE_COUNT, msgs.getMessageCount());
            }
        }

    }

    public void testQueueAllConnected() throws Exception {
        bridgeAllBrokers();
        startAllBrokers();

        // Setup topic destination
        Destination dest = createDestination("TEST.FOO", false);

        CountDownLatch latch = new CountDownLatch(BROKER_COUNT * PRODUCER_COUNT * MESSAGE_COUNT);

        // Setup consumers
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < CONSUMER_COUNT; j++) {
                consumerMap.put("Consumer:" + i + ":" + j, createConsumer("Broker" + i, dest, latch));
            }
        }

        // wait for consumers to get propagated
        Thread.sleep(5000);

        // Send messages
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < PRODUCER_COUNT; j++) {
                sendMessages("Broker" + i, dest, MESSAGE_COUNT);
            }
        }

        // Wait for messages to be delivered
        assertTrue("Missing " + latch.getCount() + " messages", latch.await(30, TimeUnit.SECONDS));

        // Get message count
        int totalMsg = 0;
        for (int i = 1; i <= BROKER_COUNT; i++) {
            for (int j = 0; j < CONSUMER_COUNT; j++) {
                MessageIdList msgs = getConsumerMessages("Broker" + i, (MessageConsumer)consumerMap.get("Consumer:" + i + ":" + j));
                totalMsg += msgs.getMessageCount();
            }
        }
        assertEquals(BROKER_COUNT * PRODUCER_COUNT * MESSAGE_COUNT, totalMsg);
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();

        // Setup n brokers
        for (int i = 1; i <= BROKER_COUNT; i++) {
            createBroker(new URI("broker:()/Broker" + i + "?persistent=false&useJmx=false"));
        }

        consumerMap = new HashMap();
    }
}
