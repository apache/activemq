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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.store.AbstractMessageStoreSizeStatTest;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks that KahaDB properly sets the new storeMessageSize
 * statistic.
 *
 * AMQ-5748
 *
 */
public class KahaDBMessageStoreSizeStatTest extends
        AbstractMessageStoreSizeStatTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(KahaDBMessageStoreSizeStatTest.class);

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
    public void testMessageSizeAfterRestartAndPublish() throws Exception {
        AtomicLong publishedMessageSize = new AtomicLong();
        Destination dest = publishTestQueueMessages(200, publishedMessageSize);

        // verify the count and size
        verifyStats(dest, 200, publishedMessageSize.get());

        // stop, restart broker and publish more messages
        stopBroker();
        this.setUpBroker(false);
        dest = publishTestQueueMessages(200, publishedMessageSize);

        // verify the count and size
        verifyStats(dest, 400, publishedMessageSize.get());

    }

}
