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
package org.apache.activemq.transport.failover;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.mock.MockTransport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.1 $
 */
public class ReconnectTest extends TestCase {

    public static final int MESSAGES_PER_ITTERATION = 10;
    public static final int WORKER_COUNT = 10;

    private static final Log LOG = LogFactory.getLog(ReconnectTest.class);

    private BrokerService bs;
    private URI tcpUri;
    private AtomicInteger interruptedCount = new AtomicInteger();
    private Worker[] workers;

    class Worker implements Runnable, ExceptionListener {

        public AtomicInteger iterations = new AtomicInteger();
        public CountDownLatch stopped = new CountDownLatch(1);

        private ActiveMQConnection connection;
        private AtomicBoolean stop = new AtomicBoolean(false);
        private Throwable error;

        public Worker() throws URISyntaxException, JMSException {
            URI uri = new URI("failover://(mock://(" + tcpUri + "))");
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
            connection = (ActiveMQConnection)factory.createConnection();
            connection.setExceptionListener(this);
            connection.addTransportListener(new TransportListener() {
                public void onCommand(Object command) {
                }

                public void onException(IOException error) {
                    setError(error);
                }

                public void transportInterupted() {
                    interruptedCount.incrementAndGet();
                }

                public void transportResumed() {
                }
            });
            connection.start();
        }

        public void failConnection() {
            MockTransport mockTransport = (MockTransport)connection.getTransportChannel().narrow(MockTransport.class);
            mockTransport.onException(new IOException("Simulated error"));
        }

        public void start() {
            new Thread(this).start();
        }

        public void stop() {
            stop.set(true);
            try {
                if (!stopped.await(5, TimeUnit.SECONDS)) {
                    connection.close();
                    stopped.await(5, TimeUnit.SECONDS);
                } else {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            try {
                ActiveMQQueue queue = new ActiveMQQueue("FOO");
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(queue);
                MessageProducer producer = session.createProducer(queue);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                while (!stop.get()) {
                    for (int i = 0; i < MESSAGES_PER_ITTERATION; i++) {
                        producer.send(session.createTextMessage("TEST:" + i));
                    }
                    for (int i = 0; i < MESSAGES_PER_ITTERATION; i++) {
                        consumer.receive();
                    }
                    iterations.incrementAndGet();
                }
                session.close();
            } catch (JMSException e) {
                setError(e);
            } finally {
                stopped.countDown();
            }
        }

        public void onException(JMSException error) {
            setError(error);
            stop();
        }

        public synchronized Throwable getError() {
            return error;
        }

        public synchronized void setError(Throwable error) {
            this.error = error;
        }

        public synchronized void assertNoErrors() {
            if (error != null) {
                error.printStackTrace();
                fail("Got Exception: " + error);
            }
        }

    }

    public void testReconnects() throws Exception {

        for (int k = 1; k < 5; k++) {

            LOG.info("Test run: " + k);

            // Wait for at least one iteration to occur...
            for (int i = 0; i < WORKER_COUNT; i++) {
                for (int j = 0; workers[i].iterations.get() == 0 && j < 5; j++) {
                    workers[i].assertNoErrors();
                    LOG.info("Waiting for worker " + i + " to finish an iteration.");
                    Thread.sleep(1000);
                }
                assertTrue("Worker " + i + " never completed an interation.", workers[i].iterations.get() != 0);
                workers[i].assertNoErrors();
            }

            LOG.info("Simulating transport error to cause reconnect.");

            // Simulate a transport failure.
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers[i].failConnection();
            }

            // Wait for the connections to get interrupted...
            while (interruptedCount.get() < WORKER_COUNT) {
                LOG.info("Waiting for connections to get interrupted.. at: " + interruptedCount.get());
                Thread.sleep(1000);
            }

            // let things stablize..
            LOG.info("Pausing before starting next iterations...");
            Thread.sleep(1000);

            // Reset the counters..
            interruptedCount.set(0);
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers[i].iterations.set(0);
            }

        }

    }

    protected void setUp() throws Exception {
        bs = new BrokerService();
        bs.setPersistent(false);
        bs.setUseJmx(true);
        TransportConnector connector = bs.addConnector("tcp://localhost:0");
        bs.start();
        tcpUri = connector.getConnectUri();

        workers = new Worker[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers[i] = new Worker();
            workers[i].start();
        }

    }

    protected void tearDown() throws Exception {
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers[i].stop();
        }
        new ServiceStopper().stop(bs);
    }

}
