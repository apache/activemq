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
package org.apache.activemq.transport.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.Random;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Frames larger than the NIO read buffer must not leave a new direct buffer behind per frame.
 * Unpooled direct buffers are only released by GC, so steady large-message traffic fills direct
 * memory and Bits.reserveMemory falls back to System.gc(), stalling the selector.
 */
@Category(ParallelTest.class)
public class NIOOversizeFrameDirectMemoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(NIOOversizeFrameDirectMemoryTest.class);

    private static final int MESSAGES = 400;
    private static final int MAX_DIRECT_BUFFER_GROWTH = 64;

    private BrokerService broker;
    private String connectUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector("nio://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        connectUri = connector.getPublishableConnectString().replace("nio://", "tcp://");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 120_000)
    public void testVariedSizeFramesDoNotAccumulateDirectBuffers() throws Exception {
        BufferPoolMXBean direct = directPool();
        long seed = Long.getLong("NIOOversizeFrameDirectMemoryTest.seed", 2744L);
        LOG.info("message size seed={}", seed);
        Random random = new Random(seed);

        Connection connection = new ActiveMQConnectionFactory(connectUri).createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getClass().getSimpleName());
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);

            long baselineCount = direct.getCount();
            long peakCount = baselineCount;
            int oversize = 0;
            for (int i = 0; i < MESSAGES; i++) {
                int size = nextMessageSize(random);
                if (size > 8 * 1024) {
                    oversize++;
                }
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(new byte[size]);
                producer.send(message);

                BytesMessage received = (BytesMessage) consumer.receive(10_000);
                assertNotNull("message " + i + " not received", received);
                assertEquals(size, received.getBodyLength());
                peakCount = Math.max(peakCount, direct.getCount());
            }

            long growth = peakCount - baselineCount;
            LOG.info("oversize frames={}, direct buffer count growth={}", oversize, growth);
            assertTrue("expected at least " + (MAX_DIRECT_BUFFER_GROWTH * 2) + " oversize frames, got " + oversize,
                    oversize > MAX_DIRECT_BUFFER_GROWTH * 2);
            assertTrue("direct buffer count grew by " + growth + " for " + oversize + " oversize frames",
                    growth < MAX_DIRECT_BUFFER_GROWTH);
        } finally {
            connection.close();
        }
    }

    static int nextMessageSize(Random random) {
        int bucket = random.nextInt(100);
        if (bucket < 30) {
            return 64 + random.nextInt(8 * 1024 - 64);
        } else if (bucket < 70) {
            return 8 * 1024 + random.nextInt(56 * 1024);
        } else if (bucket < 92) {
            return 64 * 1024 + random.nextInt(960 * 1024);
        } else {
            return 1024 * 1024 + random.nextInt(3 * 1024 * 1024);
        }
    }

    private static BufferPoolMXBean directPool() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                return pool;
            }
        }
        throw new IllegalStateException("no direct buffer pool MXBean");
    }
}
