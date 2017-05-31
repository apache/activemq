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
package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.util.SubscriptionKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that PendingMessageCursor size statistics work with the MemoryPersistentAdapter
 *
 * AMQ-5748
 *
 */
@RunWith(Parameterized.class)
public class MemoryPendingMessageCursorTest extends AbstractPendingMessageCursorTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(MemoryPendingMessageCursorTest.class);


    @Parameters(name = "prioritizedMessages={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // use priority messages
                { true },
                // don't use priority messages
                { false } });
    }

    public MemoryPendingMessageCursorTest(boolean prioritizedMessages) {
        super(prioritizedMessages);
    }

    @Override
    protected void initPersistence(BrokerService brokerService) throws IOException {
        broker.setPersistent(false);
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    }


    @Override
    @Test
    public void testMessageSizeOneDurable() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        org.apache.activemq.broker.region.Topic dest =
                publishTestMessagesDurable(connection, new String[] {"sub1"},
                        200, publishedMessageSize, DeliveryMode.PERSISTENT);

        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100
        verifyStoreStats(dest, 100, publishedMessageSize.get());

        //consume 100 messages
        consumeDurableTestMessages(connection, "sub1", 100, publishedMessageSize);

        //100 should be left
        verifyPendingStats(dest, subKey, 100, publishedMessageSize.get());
        verifyStoreStats(dest, 100, publishedMessageSize.get());

        connection.close();
    }

    @Override
    @Test
    public void testMessageSizeTwoDurables() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        org.apache.activemq.broker.region.Topic dest =
                publishTestMessagesDurable(connection, new String[] {"sub1", "sub2"},
                        200, publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size
        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());

        //consume messages just for sub1
        consumeDurableTestMessages(connection, "sub1", 200, publishedMessageSize);

        //There is still a durable that hasn't consumed so the messages should exist
        SubscriptionKey subKey2 = new SubscriptionKey("clientId", "sub2");
        verifyPendingStats(dest, subKey, 0, 0);
        verifyPendingStats(dest, subKey2, 200, publishedMessageSize.get());

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100
        verifyStoreStats(dest, 0, publishedMessageSize.get());

        connection.stop();
    }

    @Override
    @Test
    public void testMessageSizeOneDurablePartialConsumption() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");
        org.apache.activemq.broker.region.Topic dest = publishTestMessagesDurable(connection,
                new String[] {"sub1"}, 200, publishedMessageSize, DeliveryMode.PERSISTENT);

        //verify the count and size - durable is offline so all 200 should be pending since none are in prefetch
        verifyPendingStats(dest, subKey, 200, publishedMessageSize.get());

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100
        verifyStoreStats(dest, 100, publishedMessageSize.get());

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 50, publishedMessageSize);

        //All messages should now be gone
        verifyPendingStats(dest, subKey, 150, publishedMessageSize.get());

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100
       //verify the size is at least as big as 100 messages times the minimum of 100 size
        verifyStoreStats(dest, 100, 100 * 100);

        connection.close();
    }

}
