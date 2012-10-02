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


import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentDestinationCreationTest extends org.apache.activemq.TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentDestinationCreationTest.class);
    BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        broker = createBroker();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        broker.stop();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString() + "?jms.watchTopicAdvisories=false&jms.closeTimeout=35000");
    }

    BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setDeleteAllMessagesOnStartup(true);
        service.setAdvisorySupport(false);
        service.setTransportConnectorURIs(new String[]{"tcp://localhost:0"});
        service.setPersistent(false);
        service.setUseJmx(false);
        service.start();
        return service;
    }

    public void testSendRateWithActivatingConsumers() throws Exception {

        final Vector<Throwable> exceptions = new Vector<Throwable>();
        final int jobs = 50;
        final int destinationCount = 10;
        final CountDownLatch allDone = new CountDownLatch(jobs);
        ExecutorService executor = java.util.concurrent.Executors.newCachedThreadPool();
        for (int i = 0; i < jobs; i++) {
            if (i %2 == 0 &&  i<jobs/2) {
                executor.execute(new Runnable() {
                    final ConnectionFactory factory = createConnectionFactory();

                    @Override
                    public void run() {
                        try {
                            final Connection connection = factory.createConnection();
                            connection.start();
                            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                            for (int j = 0; j< jobs*10; j++) {
                                final MessageProducer producer = session.createProducer(new ActiveMQQueue("Q." + (j%destinationCount)));
                                producer.send(session.createMessage());
                            }
                            connection.close();
                            allDone.countDown();
                            LOG.info("Producers done!");
                        } catch (Exception ignored) {
                            LOG.error("unexpected ", ignored);
                            exceptions.add(ignored);
                        }
                    }
                });
            } else {

                executor.execute(new Runnable() {
                    final ConnectionFactory factory = createConnectionFactory();

                    @Override
                    public void run() {
                        try {
                            final Connection connection = factory.createConnection();
                            connection.start();
                            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            for (int j = 0; j < jobs; j++) {
                                final MessageConsumer consumer = session.createConsumer(new ActiveMQQueue("Q.>"));
                                consumer.receiveNoWait();
                            }
                            connection.close();
                            allDone.countDown();
                            LOG.info("Consumers done!");
                        } catch (Exception ignored) {
                            LOG.error("unexpected ", ignored);
                            exceptions.add(ignored);
                        }
                    }
                });
            }
        }
        LOG.info("Waiting for completion");
        executor.shutdown();
        boolean success = allDone.await(30, TimeUnit.SECONDS);
        if (!success) {
            dumpAllThreads("hung");

            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            LOG.info("Supports dead lock detection: " + bean.isSynchronizerUsageSupported());
            long[] threadIds = bean.findDeadlockedThreads();
            if (threadIds != null) {
                System.err.println("Dead locked threads....");
                ThreadInfo[] infos = bean.getThreadInfo(threadIds);

                for (ThreadInfo info : infos) {
                    StackTraceElement[] stack = info.getStackTrace();
                    System.err.println(" " + info + ", stack size::"  + stack.length);
                    for (StackTraceElement stackEntry : stack) {
                        System.err.println("   " + stackEntry);
                    }
                }
            }
        }
        assertTrue("Finished on time", success);
        assertTrue("No unexpected exceptions", exceptions.isEmpty());
    }
}
