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
package org.apache.activemq.store.memory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.store.AbstractMessageStoreSizeStatTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize statistic.
 *
 * AMQ-5748
 *
 */
public class MemoryMessageStoreSizeStatTest extends AbstractMessageStoreSizeStatTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(MemoryMessageStoreSizeStatTest.class);

    @Override
    protected void initPersistence(BrokerService brokerService) throws IOException {
        broker.setPersistent(false);
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    }

    @Override
    @Test(timeout=30000)
    public void testMessageSizeOneDurable() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100
        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1"}, 200, 100, publishedMessageSize);

        //verify the count and size, should be 100 because of the LRUCache
        //verify size is at least the minimum of 100 messages times 100 bytes
        verifyStats(dest, 100, 100 * 100);

        consumeDurableTestMessages(connection, "sub1", 100, publishedMessageSize);

        //Since an LRU cache is used and messages are kept in memory, this should be 100 still
        verifyStats(dest, 100, publishedMessageSize.get());

        connection.stop();

    }

    @Override
    @Test(timeout=30000)
    public void testMessageSizeTwoDurables() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        //The expected value is only 100 because for durables a LRUCache is being used
        //with a max size of 100, so only 100 messages are kept
        Destination dest = publishTestMessagesDurable(connection, new String[] {"sub1", "sub2"}, 200, 100, publishedMessageSize);

        //verify the count and size
        //verify size is at least the minimum of 100 messages times 100 bytes
        verifyStats(dest, 100, 100 * 100);

        //consume for sub1
        consumeDurableTestMessages(connection, "sub1", 100, publishedMessageSize);

        //Should be 100 messages still
        verifyStats(dest, 100, publishedMessageSize.get());

        connection.stop();

    }



}
