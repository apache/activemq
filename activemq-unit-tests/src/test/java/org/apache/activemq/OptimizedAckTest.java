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
package org.apache.activemq;

import java.util.concurrent.TimeUnit;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizedAckTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedAckTest.class);
    private ActiveMQConnection connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        connection = (ActiveMQConnection) createConnection();
        connection.setOptimizeAcknowledge(true);
        connection.setOptimizeAcknowledgeTimeOut(0);
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(10);
        connection.setPrefetchPolicy(prefetchPolicy);
    }

    @Override
    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    public void testReceivedMessageStillInflight() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("Hello" + i));
        }

        final RegionBroker regionBroker = (RegionBroker) BrokerRegistry.getInstance().findFirst().getRegionBroker();
        MessageConsumer consumer = session.createConsumer(queue);

        assertTrue("prefetch full", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("inflight count: " + regionBroker.getDestinationStatistics().getInflight().getCount());
                return 10 == regionBroker.getDestinationStatistics().getInflight().getCount();
            }
        }));

        for (int i = 0; i < 6; i++) {
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);
            assertEquals("all prefetch is still in flight: " + i, 10, regionBroker.getDestinationStatistics().getInflight().getCount());
        }

        for (int i = 6; i < 10; i++) {
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);

            assertTrue("most are acked but 3 remain", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 3 == regionBroker.getDestinationStatistics().getInflight().getCount();
                }
            }));
        }
    }

    public void testVerySlowReceivedMessageStillInflight() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("Hello" + i));
        }

        final RegionBroker regionBroker = (RegionBroker) BrokerRegistry.getInstance().findFirst().getRegionBroker();
        MessageConsumer consumer = session.createConsumer(queue);

        assertTrue("prefetch full", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("inflight count: " + regionBroker.getDestinationStatistics().getInflight().getCount());
                return 10 == regionBroker.getDestinationStatistics().getInflight().getCount();
            }
        }));

        for (int i = 0; i < 6; i++) {
            Thread.sleep(400);
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);
            assertEquals("all prefetch is still in flight: " + i, 10, regionBroker.getDestinationStatistics().getInflight().getCount());
        }

        for (int i = 6; i < 10; i++) {
            Thread.sleep(400);
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);

            assertTrue("most are acked but 3 remain", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 3 == regionBroker.getDestinationStatistics().getInflight().getCount();
                }
            }));
        }

    }

    public void testReceivedMessageNotInFlightAfterScheduledAckFires() throws Exception {
        connection.setOptimizedAckScheduledAckInterval(TimeUnit.SECONDS.toMillis(10));
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 10; i++) {
            producer.send(session.createTextMessage("Hello" + i));
        }

        final RegionBroker regionBroker = (RegionBroker) BrokerRegistry.getInstance().findFirst().getRegionBroker();
        MessageConsumer consumer = session.createConsumer(queue);

        assertTrue("prefetch full", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("inflight count: " + regionBroker.getDestinationStatistics().getInflight().getCount());
                return 10 == regionBroker.getDestinationStatistics().getInflight().getCount();
            }
        }));

        for (int i = 0; i < 6; i++) {
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);
            assertEquals("all prefetch is still in flight: " + i, 10, regionBroker.getDestinationStatistics().getInflight().getCount());
        }

        for (int i = 6; i < 10; i++) {
            javax.jms.Message msg = consumer.receive(4000);
            assertNotNull(msg);
            assertTrue("most are acked but 3 remain", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 3 == regionBroker.getDestinationStatistics().getInflight().getCount();
                }
            }));
        }

        assertTrue("After delay the scheduled ack should ack all inflight.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("inflight count: " + regionBroker.getDestinationStatistics().getInflight().getCount());
                return 0 == regionBroker.getDestinationStatistics().getInflight().getCount();
            }
        }));
    }
}
