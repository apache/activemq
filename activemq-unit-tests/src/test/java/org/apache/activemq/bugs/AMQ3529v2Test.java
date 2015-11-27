/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.activemq.bugs;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AMQ3529v2Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ3529v2Test.class);

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testRandomInterruptionAffects() throws Exception {
        doTestRandomInterruptionAffects();
    }

    @Test(timeout = 60000)
    public void testRandomInterruptionAffectsWithFailover() throws Exception {
        connectionUri = "failover:(" + connectionUri + ")";
        doTestRandomInterruptionAffects();
    }

    public void doTestRandomInterruptionAffects() throws Exception {
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);

        ThreadGroup tg = new ThreadGroup("tg");

        assertEquals(0, tg.activeCount());

        class ClientThread extends Thread {

            public Exception error;

            public ClientThread(ThreadGroup tg, String name) {
                super(tg, name);
            }

            @Override
            public void run() {
                Context ctx = null;
                Connection connection = null;
                Session session = null;
                MessageConsumer consumer = null;

                try {
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    assertNotNull(session);

                    Properties props = new Properties();
                    props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
                    props.setProperty(Context.PROVIDER_URL, connectionUri);
                    ctx = null;
                    try {
                        ctx = new InitialContext(props);
                    } catch (NoClassDefFoundError e) {
                        throw new NamingException(e.toString());
                    } catch (Exception e) {
                        throw new NamingException(e.toString());
                    }
                    Destination destination = (Destination) ctx.lookup("dynamicTopics/example.C");
                    consumer = session.createConsumer(destination);
                    consumer.receive(10000);
                } catch (Exception e) {
                    // Expect an exception here from the interrupt.
                } finally {
                    try {
                        if (consumer != null) {
                            consumer.close();
                        }
                    } catch (JMSException e) {
                        trackException("Consumer Close failed with", e);
                    }
                    try {
                        if (session != null) {
                            session.close();
                        }
                    } catch (JMSException e) {
                        trackException("Session Close failed with", e);
                    }
                    try {
                        if (connection != null) {
                            connection.close();
                        }
                    } catch (JMSException e) {
                        trackException("Connection Close failed with", e);
                    }
                    try {
                        if (ctx != null) {
                            ctx.close();
                        }
                    } catch (Exception e) {
                        trackException("Connection Close failed with", e);
                    }
                }
            }

            private void trackException(String s, Exception e) {
                LOG.error(s, e);
                this.error = e;
            }
        }

        final Random random = new Random();
        List<ClientThread> threads = new LinkedList<ClientThread>();
        for (int i=0;i<10;i++) {
            threads.add(new ClientThread(tg, "Client-"+ i));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        // interrupt the threads at some random time
        ExecutorService doTheInterrupts = Executors.newFixedThreadPool(threads.size());
        for (final Thread thread : threads) {
            doTheInterrupts.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(5000));
                    } catch (InterruptedException ignored) {
                        ignored.printStackTrace();
                    }
                    thread.interrupt();
                }
            });
        }
        doTheInterrupts.shutdown();
        assertTrue("all interrupts done", doTheInterrupts.awaitTermination(30, TimeUnit.SECONDS));

        for (Thread thread : threads) {
            thread.join();
        }

        for (ClientThread thread : threads) {
            if (thread.error != null) {
                LOG.info("Close error on thread: " + thread, thread.error);
            }
        }

        Thread[] remainThreads = new Thread[tg.activeCount()];
        tg.enumerate(remainThreads);
        for (final Thread t : remainThreads) {
            if (t != null && t.isAlive() && !t.isDaemon())
                assertTrue("Thread completes:" + t, Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        LOG.info("Remaining thread: " + t.toString());
                        return !t.isAlive();
                    }
                }));
        }

        ThreadGroup root = Thread.currentThread().getThreadGroup().getParent();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        visit(root, 0);
    }

    // This method recursively visits all thread groups under `group'.
    public static void visit(ThreadGroup group, int level) {
        // Get threads in `group'
        int numThreads = group.activeCount();
        Thread[] threads = new Thread[numThreads * 2];
        numThreads = group.enumerate(threads, false);

        // Enumerate each thread in `group'
        for (int i = 0; i < numThreads; i++) {
            // Get thread
            Thread thread = threads[i];
            LOG.debug("Thread:" + thread.getName() + " is still running");
        }

        // Get thread subgroups of `group'
        int numGroups = group.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[numGroups * 2];
        numGroups = group.enumerate(groups, false);

        // Recursively visit each subgroup
        for (int i = 0; i < numGroups; i++) {
            visit(groups[i], level + 1);
        }
    }
}
