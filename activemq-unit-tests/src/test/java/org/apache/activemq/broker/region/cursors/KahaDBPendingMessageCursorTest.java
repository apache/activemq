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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that pending message metrics work properly with KahaDB
 *
 * AMQ-5923
 *
 */
public class KahaDBPendingMessageCursorTest extends
        AbstractPendingMessageCursorTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(KahaDBPendingMessageCursorTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    @Override
    protected void setUpBroker(boolean clearDataDir) throws Exception {
        if (clearDataDir && dataFileDir.getRoot().exists())
            FileUtils.cleanDirectory(dataFileDir.getRoot());
        super.setUpBroker(clearDataDir);
    }

    @Override
    protected void initPersistence(BrokerService brokerService)
            throws IOException {
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
    }

    /**
     * Test that the the counter restores size and works after restart and more
     * messages are published
     *
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testDurableMessageSizeAfterRestartAndPublish() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Topic topic =  publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.PERSISTENT);

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");

        // verify the count and size
        verifyPendingStats(topic, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(topic, 200, publishedMessageSize.get());

        // stop, restart broker and publish more messages
        stopBroker();
        this.setUpBroker(false);

        connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();

        topic = publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.PERSISTENT);

        // verify the count and size
        verifyPendingStats(topic, subKey, 400, publishedMessageSize.get());
        verifyStoreStats(topic, 400, publishedMessageSize.get());

    }

    /**
     * Test that the the counter restores size and works after restart and more
     * messages are published
     *
     * @throws Exception
     */
    @Test(timeout=60000)
    public void testNonPersistentDurableMessageSize() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();

        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.setClientID("clientId");
        connection.start();
        Topic topic =  publishTestMessagesDurable(connection, new String[] {"sub1"}, 200,
                publishedMessageSize, DeliveryMode.NON_PERSISTENT);

        SubscriptionKey subKey = new SubscriptionKey("clientId", "sub1");

        // verify the count and size
        verifyPendingStats(topic, subKey, 200, publishedMessageSize.get());
        verifyStoreStats(topic, 0, 0);
    }
}
