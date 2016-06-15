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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.Socket;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverTimeoutTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTimeoutTest.class);

    private static final String QUEUE_NAME = "test.failovertimeout";
    BrokerService bs;
    URI tcpUri;

    @Before
    public void setUp() throws Exception {
        bs = new BrokerService();
        bs.setUseJmx(false);
        bs.addConnector(getTransportUri());
        bs.start();
        tcpUri = bs.getTransportConnectors().get(0).getConnectUri();
    }

    @After
    public void tearDown() throws Exception {
        if (bs != null) {
            bs.stop();
        }
    }

    protected String getTransportUri() {
        return "tcp://localhost:0";
    }

    @Test
    public void testTimoutDoesNotFailConnectionAttempts() throws Exception {
        bs.stop();
        long timeout = 1000;

        long startTime = System.currentTimeMillis();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
            "failover:(" + tcpUri + ")" +
            "?timeout=" + timeout + "&useExponentialBackOff=false" +
            "&maxReconnectAttempts=5" + "&initialReconnectDelay=1000");
        Connection connection = cf.createConnection();
        try {
            connection.start();
            fail("Should have failed to connect");
        } catch (JMSException ex) {
            LOG.info("Caught exception on call to start: {}", ex.getMessage());
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        LOG.info("Time spent waiting to connect: {} ms", duration);

        assertTrue(duration > 3000);

        safeClose(connection);
    }

    private void safeClose(Connection connection) {
        try {
            connection.close();
        } catch (Exception ignored) {}
    }

    @Test
    public void testTimeout() throws Exception {

        long timeout = 1000;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?timeout=" + timeout + "&useExponentialBackOff=false");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session
                .createQueue(QUEUE_NAME));
        TextMessage message = session.createTextMessage("Test message");
        producer.send(message);

        bs.stop();

        try {
            producer.send(message);
        } catch (JMSException jmse) {
            assertEquals("Failover timeout of " + timeout + " ms reached.", jmse.getMessage());
        }

        bs = new BrokerService();
        bs.setUseJmx(false);
        bs.addConnector(tcpUri);
        bs.start();
        bs.waitUntilStarted();

        producer.send(message);
        bs.stop();
        connection.close();
    }

    @Test
    public void testInterleaveAckAndException() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?maxReconnectAttempts=0");
        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();

        doTestInterleaveAndException(connection, new MessageAck());
        safeClose(connection);
    }

    @Test
    public void testInterleaveTxAndException() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?maxReconnectAttempts=0");
        final ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();

        TransactionInfo tx = new TransactionInfo();
        tx.setConnectionId(connection.getConnectionInfo().getConnectionId());
        tx.setTransactionId(new LocalTransactionId(tx.getConnectionId(), 1));
        doTestInterleaveAndException(connection, tx);

        safeClose(connection);
    }

    public void doTestInterleaveAndException(final ActiveMQConnection connection, final Command command) throws Exception {

        connection.start();

        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                try {
                    LOG.info("Deal with exception - invoke op that may block pending outstanding oneway");
                    // try and invoke on connection as part of handling exception
                    connection.asyncSendPacket(command);
                } catch (Exception e) {
                }
            }
        });

        final ExecutorService executorService = Executors.newCachedThreadPool();

        final int NUM_TASKS = 200;
        final CountDownLatch enqueueOnExecutorDone = new CountDownLatch(NUM_TASKS);

        // let a few tasks delay a bit
        final AtomicLong sleepMillis = new AtomicLong(1000);
        for (int i=0; i < NUM_TASKS; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        TimeUnit.MILLISECONDS.sleep(Math.max(0, sleepMillis.addAndGet(-50)));
                        connection.asyncSendPacket(command);
                    } catch (Exception e) {
                    } finally {
                        enqueueOnExecutorDone.countDown();
                    }
                }
            });
        }

        while (enqueueOnExecutorDone.getCount() > (NUM_TASKS - 10)) {
            enqueueOnExecutorDone.await(20, TimeUnit.MILLISECONDS);
        }

        // force IOException
        final Socket socket = connection.getTransport().narrow(Socket.class);
        socket.close();

        executorService.shutdown();

        assertTrue("all ops finish", enqueueOnExecutorDone.await(15, TimeUnit.SECONDS));
    }


    @Test
    public void testUpdateUris() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?useExponentialBackOff=false");
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        FailoverTransport failoverTransport = connection.getTransport().narrow(FailoverTransport.class);

        URI[] bunchOfUnknownAndOneKnown = new URI[]{
                new URI("tcp://unknownHost:" + tcpUri.getPort()),
                new URI("tcp://unknownHost2:" + tcpUri.getPort()),
                new URI("tcp://localhost:2222")};
        failoverTransport.add(false, bunchOfUnknownAndOneKnown);
    }
}
