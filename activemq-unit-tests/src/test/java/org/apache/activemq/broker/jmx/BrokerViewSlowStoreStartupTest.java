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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.*;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to verify that the BrokerView accessed while the BrokerSerivce is waiting
 * for a Slow Store startup to complete doesn't throw unexpected NullPointerExceptions.
 */
@Category(ParallelTest.class)
public class BrokerViewSlowStoreStartupTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerViewSlowStoreStartupTest.class);

    private final CountDownLatch holdStoreStart = new CountDownLatch(1);
    private final String brokerName = "brokerViewTest";

    private BrokerService broker;
    private Thread startThread;

    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(brokerName);

        KahaDBStore kaha = new KahaDBStore() {

            @Override
            public void start() throws Exception {
                LOG.info("Test KahaDB class is waiting for signal to complete its start()");
                holdStoreStart.await();
                super.start();
                LOG.info("Test KahaDB class is completed its start()");
            }
        };

        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        kaha.deleteAllMessages();

        broker.setPersistenceAdapter(kaha);
        broker.setUseJmx(true);

        return broker;
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker();

        startThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    broker.start();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        });
        startThread.start();
    }

    @After
    public void tearDown() throws Exception {

        // ensure we don't keep the broker held if an exception occurs somewhere.
        holdStoreStart.countDown();

        startThread.join();

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout=120000)
    public void testBrokerViewOnSlowStoreStart() throws Exception {

        // Ensure we have an Admin View.
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));

        final BrokerView view = broker.getAdminView();

        assertEquals(brokerName, view.getBrokerName());

        assertNull(view.getBrokerId());

        assertEquals(0, view.getTotalEnqueueCount());

        assertEquals(0, view.getTotalDequeueCount());

        assertEquals(0, view.getTotalConsumerCount());

        assertEquals(0, view.getTotalProducerCount());

        assertEquals(0, view.getTotalMessageCount());

        assertEquals(0, view.getTotalMessagesCached());

        try {
            view.resetStatistics();
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.enableStatistics();
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.disableStatistics();
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        assertFalse(view.isStatisticsEnabled());

        assertEquals(0, view.getTopics().length);

        assertEquals(0, view.getQueues().length);

        assertEquals(0, view.getTemporaryTopics().length);

        assertEquals(0, view.getTemporaryQueues().length);

        assertEquals(0, view.getTopicSubscribers().length);

        assertEquals(0, view.getDurableTopicSubscribers().length);

        assertEquals(0, view.getQueueSubscribers().length);

        assertEquals(0, view.getTemporaryTopicSubscribers().length);

        assertEquals(0, view.getTemporaryQueueSubscribers().length);

        assertEquals(0, view.getInactiveDurableTopicSubscribers().length);

        assertEquals(0, view.getTopicProducers().length);

        assertEquals(0, view.getQueueProducers().length);

        assertEquals(0, view.getTemporaryTopicProducers().length);

        assertEquals(0, view.getTemporaryQueueProducers().length);

        assertEquals(0, view.getDynamicDestinationProducers().length);

        try {
            view.removeConnector("tcp");
            fail("Should have thrown an NoSuchElementException");
        } catch(NoSuchElementException e) {
        }

        try {
            view.removeNetworkConnector("tcp");
            fail("Should have thrown an NoSuchElementException");
        } catch(NoSuchElementException e) {
        }

        try {
            view.addTopic("TEST");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.addQueue("TEST");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.removeTopic("TEST");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.removeQueue("TEST");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.createDurableSubscriber("1", "2", "3","4");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        try {
            view.destroyDurableSubscriber("1", "2");
            fail("Should have thrown an IllegalStateException");
        } catch(IllegalStateException e) {
        }

        holdStoreStart.countDown();
        startThread.join();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return view.getBroker() != null;
            }
        });
        assertNotNull(view.getBroker());

        try {
            view.getBrokerName();
        } catch(Exception e) {
            fail("caught an exception getting the Broker property: " + e.getClass().getName());
        }

        try {
            view.getBrokerId();
        } catch(IllegalStateException e) {
            fail("caught an exception getting the Broker property: " + e.getClass().getName());
        }

        try {
            view.getTotalEnqueueCount();
        } catch(IllegalStateException e) {
            fail("caught an exception getting the Broker property: " + e.getClass().getName());
        }
    }
}
