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
package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test for AMQ-6798
 */
public class QueueMemoryAndStoreUsageCleanupTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(QueueMemoryAndStoreUsageCleanupTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));
    private BrokerService broker;
    private SystemUsage systemUsage;

    @Before
    public void setUpBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        broker.setDeleteAllMessagesOnStartup(true);
        systemUsage = broker.getSystemUsage();
        startBroker();
    }

    protected void startBroker() throws Exception {
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout=30000)
    public void testQueueMemoryAndStoreUsageCleanup() throws Exception {
        Field childrenField = Usage.class.getDeclaredField("children");
        childrenField.setAccessible(true);
        List<?> memoryUsageChildren = (List<?>) childrenField.get(systemUsage.getMemoryUsage());
        List<?> storeUsageChildren = (List<?>) childrenField.get(systemUsage.getStoreUsage());

        Destination queue1 = addDestination(new ActiveMQQueue("queue1"));
        Destination queue2 = addDestination(new ActiveMQQueue("queue2"));
        Destination queue3 = addDestination(new ActiveMQQueue("queue3"));
        Destination queue4 = addDestination(new ActiveMQQueue("queue4"));

        int beforeStopMemoryChildren = memoryUsageChildren.size();
        int beforeStopStoreChildren = storeUsageChildren.size();

        queue1.stop();
        queue2.stop();
        queue3.stop();
        queue4.stop();

        //Make sure each memory usage and store usage object that was created for every queue
        //has been cleaned up
        assertEquals(beforeStopMemoryChildren - 4, memoryUsageChildren.size());
        assertEquals(beforeStopStoreChildren - 4, storeUsageChildren.size());
    }

    private Destination addDestination(ActiveMQDestination destination) throws Exception {
        Destination dest = broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                destination, false);
        dest.start();
        return dest;
    }
}
