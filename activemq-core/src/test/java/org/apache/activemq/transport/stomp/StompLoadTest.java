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

package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompLoadTest {

    private static final Logger LOG = LoggerFactory.getLogger(StompLoadTest.class);

    protected String bindAddress = "stomp://localhost:61613";
    protected String confUri = "xbean:org/apache/activemq/transport/stomp/stomp-auth-broker.xml";
    protected String jmsUri = "vm://localhost";

    private static final int TASK_COUNT = 100;
    private static final int MSG_COUNT = 250;  // AMQ-3819: Above 250 or so and the CPU goes bonkers with NOI+SSL.

    private BrokerService broker;
    protected StompConnection stompConnection = new StompConnection();
    protected Connection connection;
    protected Session session;
    protected ActiveMQQueue queue;

    private ExecutorService executor;
    private CountDownLatch started;
    private CountDownLatch ready;
    private AtomicInteger receiveCount;

    @Before
    public void setUp() throws Exception {

        broker = BrokerFactory.createBroker(new URI(confUri));
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
        broker.waitUntilStarted();

        stompConnect();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(jmsUri);
        connection = cf.createConnection("system", "manager");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = new ActiveMQQueue(getDestinationName());
        connection.start();

        executor = Executors.newFixedThreadPool(TASK_COUNT, new ThreadFactory() {

            private long i = 0;

            public Thread newThread(Runnable runnable) {
                this.i++;
                final Thread t = new Thread(runnable, "Test Worker " + this.i);
                return t;
            }
        });

        started = new CountDownLatch(TASK_COUNT);
        ready = new CountDownLatch(1);
        receiveCount = new AtomicInteger(0);
    }

    @After
    public void tearDown() throws Exception {
        try {
            executor.shutdownNow();
            connection.close();
            stompDisconnect();
        } catch(Exception e) {
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout=20*60*1000*1000)
    public void testStompUnloadLoad() throws Exception {

        for (int i = 0; i < TASK_COUNT; ++i) {
            executor.execute(new Runnable() {

                @Override
                public void run() {

                    LOG.debug("Receive Thread Connecting to Broker.");

                    int numReceived = 0;

                    StompConnection connection = new StompConnection();
                    try {
                        stompConnect(connection);
                    } catch (Exception e) {
                        LOG.error("Caught Exception while connecting: " + e.getMessage());
                    }

                    try {

                        for (int i = 0; i < 10; i++) {
                            connection.subscribe("/queue/test-" + i, "auto");
                            connection.subscribe("/topic/test-" + i, "auto");
                        }

                        HashMap<String, String> headers = new HashMap<String, String>();
                        headers.put("activemq.prefetchSize", "1");
                        connection.subscribe("/topic/" + getDestinationName(), "auto", headers);
                        ready.await();

                        // Now that the main test thread is ready we wait a bit to let the tasks
                        // all subscribe and the CPU to settle a bit.
                        TimeUnit.SECONDS.sleep(3);
                        started.countDown();

                        while (true) {
                            // Read Timeout ends this task, we override the default here since there
                            // are so many threads running and we don't know how slow the test box is.
                            StompFrame frame = connection.receive(TimeUnit.SECONDS.toMillis(60));
                            assertNotNull(frame);
                            numReceived++;
                            if (LOG.isDebugEnabled() && (numReceived % 50) == 0 || numReceived == MSG_COUNT) {
                                LOG.debug("Receiver thread got message: " + frame.getHeaders().get("message-id"));
                            }
                            receiveCount.incrementAndGet();
                        }

                    } catch (Exception e) {
                        if (numReceived != MSG_COUNT) {
                            LOG.warn("Receive task caught exception after receipt of ["+numReceived+
                                     "] messages: " + e.getMessage());
                        }
                    }
                }
            });
        }

        ready.countDown();
        assertTrue("Timed out waiting for receivers to start.", started.await(5, TimeUnit.MINUTES));
        String frame;

        // Lets still wait a bit to make sure all subscribers get a fair shake at
        // getting online before we send.  Account for slow Hudson machines
        TimeUnit.SECONDS.sleep(5);

        for( int ix = 0; ix < MSG_COUNT; ix++) {
            frame = "SEND\n destination:/topic/" + getDestinationName() +
                    "\nid:" + ix +
                    "\ncontent-length:5" + " \n\n" +
                    "\u0001\u0002\u0000\u0004\u0005" + Stomp.NULL;
            stompConnection.sendFrame(frame);
        }

        LOG.info("All " + MSG_COUNT + " message have been sent, awaiting receipt.");

        assertTrue("Should get [" + TASK_COUNT * MSG_COUNT + "] message but was: " + receiveCount.get(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return receiveCount.get() == TASK_COUNT * MSG_COUNT;
            }
        }, TimeUnit.MINUTES.toMillis(10)));

        LOG.info("Test Completed and all messages received, shutting down.");

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);
        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        LOG.info("Test Finished.");
    }

    protected void stompConnect() throws Exception {
        URI connectUri = new URI(bindAddress);
        LOG.debug("Attempting connection to: " + bindAddress);
        stompConnection.open(createSocket(connectUri));
        stompConnection.connect("system", "manager");
    }

    private void stompConnect(StompConnection connection) throws Exception {
        URI connectUri = new URI(bindAddress);
        LOG.debug("Attempting connection to: " + bindAddress);
        connection.open(createSocket(connectUri));
        connection.connect("system", "manager");
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        return new Socket("127.0.0.1", connectUri.getPort());
    }

    protected String getDestinationName() {
        return getClass().getName() + ".Tester";
    }

    protected void stompDisconnect() throws IOException {
        if (stompConnection != null) {
            stompConnection.close();
            stompConnection = null;
        }
    }
}
