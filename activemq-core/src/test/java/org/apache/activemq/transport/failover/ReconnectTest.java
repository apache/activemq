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
    private AtomicInteger resumedCount = new AtomicInteger();
    private AtomicInteger interruptedCount = new AtomicInteger();
    private Worker[] workers;

    class Worker implements Runnable {

        public AtomicInteger iterations = new AtomicInteger();
        public CountDownLatch stopped = new CountDownLatch(1);

        private ActiveMQConnection connection;
        private AtomicBoolean stop = new AtomicBoolean(false);
        private Throwable error;
        private String name;

        public Worker(final String name) throws URISyntaxException, JMSException {
            this.name=name;
            URI uri = new URI("failover://(mock://(" + tcpUri + "))");
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
            connection = (ActiveMQConnection)factory.createConnection();
            connection.addTransportListener(new TransportListener() {
                public void onCommand(Object command) {
                }

                public void onException(IOException error) {
                    setError(error);
                }

                public void transportInterupted() {
                    LOG.info("Worker "+name+" was interrupted...");
                    interruptedCount.incrementAndGet();
                }

                public void transportResumed() {
                    LOG.info("Worker "+name+" was resummed...");
                    resumedCount.incrementAndGet();
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
                ActiveMQQueue queue = new ActiveMQQueue("FOO_"+name);
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

        public synchronized Throwable getError() {
            return error;
        }

        public synchronized void setError(Throwable error) {
            this.error = error;
        }

        public synchronized void assertNoErrors() {
            if (error != null) {
                error.printStackTrace();
                fail("Worker "+name+" got Exception: " + error);
            }
        }

    }

    public void testReconnects() throws Exception {

        for (int k = 1; k < 10; k++) {

            LOG.info("Test run: " + k);

            // Wait for at least one iteration to occur...
            for (int i = 0; i < WORKER_COUNT; i++) {
               int c=0;
                for (int j = 0; j < 30; j++) {
                       c = workers[i].iterations.getAndSet(0);
                       if( c != 0 ) {
                               break;
                       }
                    workers[i].assertNoErrors();
                    LOG.info("Test run "+k+": Waiting for worker " + i + " to finish an iteration.");
                    Thread.sleep(1000);
                }
                assertTrue("Test run "+k+": Worker " + i + " never completed an interation.", c != 0);
                workers[i].assertNoErrors();
            }

            LOG.info("Simulating transport error to cause reconnect.");

            // Simulate a transport failure.
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers[i].failConnection();
            }

            long start;
            // Wait for the connections to get interrupted...
            start = System.currentTimeMillis();
            while (interruptedCount.get() < WORKER_COUNT) {
               if( System.currentTimeMillis()-start > 1000*60 ) {
                      fail("Timed out waiting for all connections to be interrupted.");
               }
                LOG.info("Test run "+k+": Waiting for connections to get interrupted.. at: " + interruptedCount.get());
                Thread.sleep(1000);
            }

            // Wait for the connections to re-establish...
            start = System.currentTimeMillis();
            while (resumedCount.get() < WORKER_COUNT) {
               if( System.currentTimeMillis()-start > 1000*60 ) {
                       fail("Timed out waiting for all connections to be resumed.");
               }
                LOG.info("Test run "+k+": Waiting for connections to get resumed.. at: " + resumedCount.get());
                Thread.sleep(1000);
            }

            // Reset the counters..
            interruptedCount.set(0);
            resumedCount.set(0);
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers[i].iterations.set(0);
            }
            
            Thread.sleep(1000);

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
            workers[i] = new Worker(""+i);
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
