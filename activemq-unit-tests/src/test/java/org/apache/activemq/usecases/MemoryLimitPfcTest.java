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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.ProducerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class MemoryLimitPfcTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLimitPfcTest.class);
    final String payload = new String(new byte[100 * 1024]);
    protected BrokerService broker;

    @Parameterized.Parameter
    public PersistenceAdapterChoice persistenceAdapterChoice;

    @Parameterized.Parameters(name="store={0}")
    public static Iterable<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{PersistenceAdapterChoice.KahaDB}, {PersistenceAdapterChoice.LevelDB}, {PersistenceAdapterChoice.JDBC}});
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.getSystemUsage().getMemoryUsage().setLimit(1 * 1024 * 1024); //1MB
        broker.setDeleteAllMessagesOnStartup(true);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0); // when this fires it will consume 2*pageSize mem which will throw the test
        policyMap.put(new ActiveMQQueue(">"), policyEntry);
        broker.setDestinationPolicy(policyMap);

        LOG.info("Starting broker with persistenceAdapterChoice " + persistenceAdapterChoice.toString());
        setPersistenceAdapter(broker, persistenceAdapterChoice);

        return broker;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        broker.start();
        broker.waitUntilStarted();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }


    @Test(timeout = 120000)
    public void testStopCachingDispatchNoPfc() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=10");
        factory.setOptimizeAcknowledge(true);
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = sess.createQueue("STORE");
        final ProducerThread producer = new ProducerThread(sess, queue) {
            @Override
            protected Message createMessage(int i) throws Exception {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(payload.getBytes());
                return bytesMessage;
            }
        };
        producer.setMessageCount(200);
        producer.start();
        producer.join();

        Thread.sleep(1000);

        // assert we didn't break high watermark (70%) usage
        final Destination dest = broker.getDestination((ActiveMQQueue) queue);
        LOG.info("Destination usage: " + dest.getMemoryUsage());
        int percentUsage = dest.getMemoryUsage().getPercentUsage();
        assertTrue("Should be less than 70% of limit but was: " + percentUsage, percentUsage <= 80);
        LOG.info("Broker usage: " + broker.getSystemUsage().getMemoryUsage());
        assertTrue(broker.getSystemUsage().getMemoryUsage().getPercentUsage() <= 80);

        assertFalse("cache disabled", ((org.apache.activemq.broker.region.Queue) dest).getMessages().isCacheEnabled());

        // consume one message
        MessageConsumer consumer = sess.createConsumer(queue);
        Message msg = consumer.receive(5000);
        msg.acknowledge();

        LOG.info("Destination usage after consume one: " + dest.getMemoryUsage());

        // ensure we can send more messages
        final ProducerThread secondProducer = new ProducerThread(sess, queue) {
                    @Override
                    protected Message createMessage(int i) throws Exception {
                        BytesMessage bytesMessage = session.createBytesMessage();
                        bytesMessage.writeBytes(payload.getBytes());
                        return bytesMessage;
                    }
                };
        secondProducer.setMessageCount(100);
        secondProducer.start();
        secondProducer.join();

        LOG.info("Broker usage: " + broker.getSystemUsage().getMemoryUsage());
        assertTrue(broker.getSystemUsage().getMemoryUsage().getPercentUsage() <= 100);

        // let's make sure we can consume all messages
        for (int i = 1; i < 300; i++) {
            msg = consumer.receive(5000);
            if (msg == null) {
                dumpAllThreads("NoMessage");
            }
            assertNotNull("Didn't receive message " + i, msg);
            msg.acknowledge();
        }
    }

    @Test(timeout = 120000)
    public void testConsumeFromTwoAfterPageInToOne() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=10");
        factory.setOptimizeAcknowledge(true);
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final ProducerThread producer = new ProducerThread(sess, sess.createQueue("STORE.1")) {
            @Override
            protected Message createMessage(int i) throws Exception {
                return session.createTextMessage(payload + "::" + i);
            }
        };
        producer.setMessageCount(20);

        final ProducerThread producer2 = new ProducerThread(sess, sess.createQueue("STORE.2")) {
            @Override
            protected Message createMessage(int i) throws Exception {
                return session.createTextMessage(payload + "::" + i);
            }
        };
        producer2.setMessageCount(20);

        producer.start();
        producer2.start();

        producer.join();
        producer2.join();

        LOG.info("before consumer1, broker % mem usage: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());

        MessageConsumer consumer = sess.createConsumer(sess.createQueue("STORE.1"));
        Message msg = null;
        for (int i=0; i<10; i++) {
            msg = consumer.receive(5000);
            LOG.info("% mem usage: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
            msg.acknowledge();
        }

        TimeUnit.SECONDS.sleep(2);
        LOG.info("Before consumer2, Broker % mem usage: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());

        MessageConsumer consumer2 = sess.createConsumer(sess.createQueue("STORE.2"));
        for (int i=0; i<10; i++) {
            msg = consumer2.receive(5000);
            LOG.info("% mem usage: " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
            msg.acknowledge();
        }

    }

}
