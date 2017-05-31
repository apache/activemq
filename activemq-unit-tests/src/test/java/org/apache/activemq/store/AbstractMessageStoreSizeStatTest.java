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
package org.apache.activemq.store;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize statistic.
 *
 * AMQ-5748
 *
 */
public abstract class AbstractMessageStoreSizeStatTest extends AbstractStoreStatTestSupport {
    protected static final Logger LOG = LoggerFactory
            .getLogger(AbstractMessageStoreSizeStatTest.class);


    protected BrokerService broker;
    protected URI brokerConnectURI;
    protected String defaultQueueName = "test.queue";
    protected String defaultTopicName = "test.topic";

    @Before
    public void startBroker() throws Exception {
        setUpBroker(true);
    }

    protected void setUpBroker(boolean clearDataDir) throws Exception {

        broker = new BrokerService();
        this.initPersistence(broker);
        //set up a transport
        TransportConnector connector = broker
                .addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();

    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Override
    protected BrokerService getBroker() {
        return this.broker;
    }

    @Override
    protected URI getBrokerConnectURI() {
        return this.brokerConnectURI;
    }

    protected abstract void initPersistence(BrokerService brokerService) throws IOException;

    @Test(timeout=60000)
    public void testMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Destination dest = publishTestQueueMessages(200, publishedMessageSize);
        verifyStats(dest, 200, publishedMessageSize.get());
    }

    @Test(timeout=60000)
    public void testMessageSizeAfterConsumption() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Destination dest = publishTestQueueMessages(200, publishedMessageSize);
        verifyStats(dest, 200, publishedMessageSize.get());

        consumeTestQueueMessages();

        verifyStats(dest, 0, 0);
    }

    @Test(timeout=60000)
    public void testMessageSizeOneDurable() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1"}, 200, 200, publishedMessageSize);

        //verify the count and size
        verifyStats(dest, 200, publishedMessageSize.get());

        //consume all messages
        consumeDurableTestMessages(connection, "sub1", 200, publishedMessageSize);

        //All messages should now be gone
        verifyStats(dest, 0, 0);

        connection.close();
    }

    @Test(timeout=60000)
    public void testMessageSizeTwoDurables() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1", "sub2"}, 200, 200, publishedMessageSize);

        //verify the count and size
        verifyStats(dest, 200, publishedMessageSize.get());

        //consume messages just for sub1
        consumeDurableTestMessages(connection, "sub1", 200, publishedMessageSize);

        //There is still a durable that hasn't consumed so the messages should exist
        verifyStats(dest, 200, publishedMessageSize.get());

        connection.stop();

    }

    @Test
    public void testMessageSizeAfterDestinationDeletion() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Destination dest = publishTestQueueMessages(200, publishedMessageSize);
        verifyStats(dest, 200, publishedMessageSize.get());

        //check that the size is 0 after deletion
        broker.removeDestination(dest.getActiveMQDestination());
        verifyStats(dest, 0, 0);
    }

    @Test
    public void testQueueBrowserMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Destination dest = publishTestQueueMessages(200, publishedMessageSize);
        browseTestQueueMessages(dest.getName());
        verifyStats(dest, 200, publishedMessageSize.get());
    }

    protected void verifyStats(Destination dest, final int count, final long minimumSize) throws Exception {
        final MessageStore messageStore = dest.getMessageStore();
        final MessageStoreStatistics storeStats = dest.getMessageStore().getMessageStoreStatistics();

        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (count == messageStore.getMessageCount()) && (messageStore.getMessageCount() ==
                        storeStats.getMessageCount().getCount()) && (messageStore.getMessageSize() ==
                messageStore.getMessageStoreStatistics().getMessageSize().getTotalSize());
            }
        }));

        if (count > 0) {
            assertTrue(storeStats.getMessageSize().getTotalSize() > minimumSize);
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return storeStats.getMessageSize().getTotalSize() > minimumSize;
                }
            }));
        } else {
            assertTrue(Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return storeStats.getMessageSize().getTotalSize() == 0;
                }
            }));
        }
    }


    protected Destination publishTestQueueMessages(int count, AtomicLong publishedMessageSize) throws Exception {
        return publishTestQueueMessages(count, defaultQueueName, DeliveryMode.PERSISTENT,
                AbstractStoreStatTestSupport.defaultMessageSize, publishedMessageSize);
    }

    protected Destination publishTestQueueMessages(int count, String queueName, AtomicLong publishedMessageSize) throws Exception {
        return publishTestQueueMessages(count, queueName, DeliveryMode.PERSISTENT,
                AbstractStoreStatTestSupport.defaultMessageSize, publishedMessageSize);
    }

    protected Destination consumeTestQueueMessages() throws Exception {
        return consumeTestQueueMessages(defaultQueueName);
    }

    protected Destination consumeDurableTestMessages(Connection connection, String sub, int size,
            AtomicLong publishedMessageSize) throws Exception {
        return consumeDurableTestMessages(connection, sub, size, defaultTopicName, publishedMessageSize);
    }

    protected Destination publishTestMessagesDurable(Connection connection, String[] subNames,
            int publishSize, int expectedSize, AtomicLong publishedMessageSize) throws Exception {
       return publishTestMessagesDurable(connection, subNames, defaultTopicName,
                publishSize, expectedSize, AbstractStoreStatTestSupport.defaultMessageSize,
                publishedMessageSize, true);
    }

}
