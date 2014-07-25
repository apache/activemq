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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4853Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4853Test.class);
    private static BrokerService brokerService;
    private static final String BROKER_ADDRESS = "tcp://localhost:0";
    private static final ActiveMQQueue DESTINATION = new ActiveMQQueue("TEST.QUEUE");
    private CountDownLatch cycleDoneLatch;

    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(true);
        brokerService.setDeleteAllMessagesOnStartup(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();

        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    /**
     * Test to shows the performance of the removing consumers while other stay active.
     * @throws Exception
     */
    @Ignore
    @Test
    public void test() throws Exception {

        // Create a stable set of consumers to fill in the advisory broker's consumer list.
        ArrayList<Consumer> fixedConsumers = new ArrayList<Consumer>(100);
        for (int i = 0; i < 200; ++i) {
            fixedConsumers.add(new Consumer());
        }

        // Create a set of consumers that comes online for a short time and then
        // goes offline again.  Cycles will repeat as each batch completes
        final int fixedDelayConsumers = 300;
        final int fixedDelayCycles = 25;

        final CountDownLatch fixedDelayCycleLatch = new CountDownLatch(fixedDelayCycles);

        // Update so done method can track state.
        cycleDoneLatch = fixedDelayCycleLatch;

        CyclicBarrier barrier = new CyclicBarrier(fixedDelayConsumers, new Runnable() {
            @Override
            public void run() {
                LOG.info("Fixed delay consumers cycle {} completed.", fixedDelayCycleLatch.getCount());
                fixedDelayCycleLatch.countDown();
            }
        });

        for (int i = 0; i < fixedDelayConsumers; ++i) {
            new Thread(new FixedDelyConsumer(barrier)).start();
        }

        fixedDelayCycleLatch.await(10, TimeUnit.MINUTES);

        // Clean up.

        for (Consumer consumer : fixedConsumers) {
            consumer.close();
        }
        fixedConsumers.clear();
    }

    private ConnectionInfo createConnectionInfo() {
        ConnectionId id = new ConnectionId();
        id.setValue("ID:123456789:0:1");

        ConnectionInfo info = new ConnectionInfo();
        info.setConnectionId(id);
        return info;
    }

    private SessionInfo createSessionInfo(ConnectionInfo connection) {
        SessionId id = new SessionId(connection.getConnectionId(), 1);

        SessionInfo info = new SessionInfo();
        info.setSessionId(id);

        return info;
    }

    public ConsumerInfo createConsumerInfo(SessionInfo session, int value, ActiveMQDestination destination) {
        ConsumerId id = new ConsumerId();
        id.setConnectionId(session.getSessionId().getConnectionId());
        id.setSessionId(1);
        id.setValue(value);

        ConsumerInfo info = new ConsumerInfo();
        info.setConsumerId(id);
        info.setDestination(destination);
        return info;
    }

    /**
     * Test to shows the performance impact of removing consumers in various scenarios.
     * @throws Exception
     */
    @Ignore
    @Test
    public void testPerformanceOfRemovals() throws Exception {
        // setup
        AdvisoryBroker testObj = (AdvisoryBroker) brokerService.getBroker().getAdaptor(AdvisoryBroker.class);
        ActiveMQDestination destination = new ActiveMQQueue("foo");
        ConnectionInfo connectionInfo = createConnectionInfo();
        ConnectionContext connectionContext = new ConnectionContext(connectionInfo);
        connectionContext.setBroker(brokerService.getBroker());
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);

        long start = System.currentTimeMillis();

        for (int i = 0; i < 200; ++i) {

            for (int j = 1; j <= 500; j++) {
                ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
                testObj.addConsumer(connectionContext, consumerInfo);
            }

            for (int j = 500; j > 0; j--) {
                ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
                testObj.removeConsumer(connectionContext, consumerInfo);
            }

            for (int j = 1; j <= 500; j++) {
                ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
                testObj.addConsumer(connectionContext, consumerInfo);
            }

            for (int j = 1; j <= 500; j++) {
                ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
                testObj.removeConsumer(connectionContext, consumerInfo);
            }
        }

        long finish = System.currentTimeMillis();

        long totalTime = finish - start;

        LOG.info("Total test time: {} seconds", TimeUnit.MILLISECONDS.toSeconds(totalTime));

        assertEquals(0, testObj.getAdvisoryConsumers().size());
    }

    @Test
    public void testEqualsNeeded() throws Exception {
        // setup
        AdvisoryBroker testObj = (AdvisoryBroker) brokerService.getBroker().getAdaptor(AdvisoryBroker.class);
        ActiveMQDestination destination = new ActiveMQQueue("foo");
        ConnectionInfo connectionInfo = createConnectionInfo();
        ConnectionContext connectionContext = new ConnectionContext(connectionInfo);
        connectionContext.setBroker(brokerService.getBroker());
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);

        for (int j = 1; j <= 5; j++) {
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
            testObj.addConsumer(connectionContext, consumerInfo);
        }

        for (int j = 1; j <= 5; j++) {
            ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, j, destination);
            testObj.removeConsumer(connectionContext, consumerInfo);
        }

        assertEquals(0, testObj.getAdvisoryConsumers().size());
    }

    private boolean done() {
        if (cycleDoneLatch == null) {
            return true;
        }
        return cycleDoneLatch.getCount() == 0;
    }

    class Consumer implements MessageListener {

        Connection connection;
        Session session;
        Destination destination;
        MessageConsumer consumer;

        Consumer() throws JMSException {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
            connection = factory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumer = session.createConsumer(DESTINATION);
            consumer.setMessageListener(this);
            connection.start();
        }

        @Override
        public void onMessage(Message message) {
        }

        public void close() {
            try {
                connection.close();
            } catch(Exception e) {
            }

            connection = null;
            session = null;
            consumer = null;
        }
    }

    class FixedDelyConsumer implements Runnable {

        private final CyclicBarrier barrier;
        private final int sleepInterval;

        public FixedDelyConsumer(CyclicBarrier barrier) {
            this.barrier = barrier;
            this.sleepInterval = 1000;
        }

        public FixedDelyConsumer(CyclicBarrier barrier, int sleepInterval) {
            this.barrier = barrier;
            this.sleepInterval = sleepInterval;
        }

        @Override
        public void run() {
            while (!done()) {

                try {
                    Consumer consumer = new Consumer();
                    TimeUnit.MILLISECONDS.sleep(sleepInterval);
                    consumer.close();
                    barrier.await();
                } catch (Exception ex) {
                    return;
                }
            }
        }
    }

}
