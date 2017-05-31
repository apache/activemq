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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.transaction.Synchronization;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

public class ActiveMQXAConnectionTxInterruptTest {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQXAConnectionTxInterruptTest.class);
    long txGenerator = System.currentTimeMillis();
    private BrokerService broker;
    XASession session;
    XAResource resource;
    ActiveMQXAConnection xaConnection;
    Destination dest;

    @Before
    public void startBrokerEtc() throws Exception {
        broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:0)/BRXA"));
        broker.setPersistent(false);
        broker.start();
        ActiveMQXAConnectionFactory cf1 = new ActiveMQXAConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getConnectUri() + ")");
        cf1.setStatsEnabled(true);
        xaConnection = (ActiveMQXAConnection)cf1.createConnection();
        xaConnection.start();
        session = xaConnection.createXASession();
        resource = session.getXAResource();

        dest = new ActiveMQQueue("Q");

    }

    @After
    public void tearDown() throws Exception {
        try {
            xaConnection.close();
        } catch (Throwable ignore) {
        }
        try {
            broker.stop();
        } catch (Throwable ignore) {
        }
    }


    @Test
    public void testRollbackAckInterrupted() throws Exception {

        // publish a message
        publishAMessage();
        Xid tid;

        // consume in tx and rollback with interrupt
        session = xaConnection.createXASession();
        final MessageConsumer consumer = session.createConsumer(dest);
        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        ((TransactionContext)resource).addSynchronization(new Synchronization() {
            @Override
            public void beforeEnd() throws Exception {
                LOG.info("Interrupting thread: " + Thread.currentThread(), new Throwable("Source"));
                Thread.currentThread().interrupt();
            }
        });
        TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
        assertNotNull(receivedMessage);
        assertEquals(getName(), receivedMessage.getText());
        resource.end(tid, XAResource.TMFAIL);
        resource.rollback(tid);
        session.close();
        assertTrue("Was interrupted", Thread.currentThread().isInterrupted());
    }

    @Test
    public void testCommitAckInterrupted() throws Exception {

        // publish a message
        publishAMessage();

        // consume in tx and rollback with interrupt
        session = xaConnection.createXASession();
        MessageConsumer consumer = session.createConsumer(dest);
        Xid tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        ((TransactionContext)resource).addSynchronization(new Synchronization() {
            @Override
            public void beforeEnd() throws Exception {
                LOG.info("Interrupting thread: " + Thread.currentThread(), new Throwable("Source"));
                Thread.currentThread().interrupt();
            }
        });
        TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
        assertNotNull(receivedMessage);
        assertEquals(getName(), receivedMessage.getText());
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);
        session.close();

    }

    @Test
    public void testInterruptWhilePendingResponseToAck() throws Exception {

        final LinkedList<Throwable> errors = new LinkedList<Throwable>();
        final CountDownLatch blockedServerSize = new CountDownLatch(1);
        final CountDownLatch canContinue = new CountDownLatch(1);
        MutableBrokerFilter filter = (MutableBrokerFilter)broker.getBroker().getAdaptor(MutableBrokerFilter.class);
        filter.setNext(new MutableBrokerFilter(filter.getNext()) {
            @Override
            public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                blockedServerSize.countDown();
                canContinue.await();
                super.acknowledge(consumerExchange, ack);
            }
        });

        publishAMessage();

        // consume in tx and rollback with interrupt while pending reply
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    session = xaConnection.createXASession();
                    MessageConsumer consumer = session.createConsumer(dest);
                    Xid tid = createXid();
                    resource = session.getXAResource();
                    resource.start(tid, XAResource.TMNOFLAGS);

                    TextMessage receivedMessage = (TextMessage) consumer.receive(1000);
                    assertNotNull(receivedMessage);
                    assertEquals(getName(), receivedMessage.getText());

                    try {
                        resource.end(tid, XAResource.TMSUCCESS);
                        fail("Expect end to fail");
                    } catch (Throwable expectedWithInterrupt) {
                        assertTrue(expectedWithInterrupt instanceof XAException);
                        assertCause(expectedWithInterrupt, new Class[]{InterruptedException.class});
                    }

                    try {
                        resource.rollback(tid);
                        fail("Expect rollback to fail due to connection being closed");
                    } catch (Throwable expectedWithInterrupt) {
                        assertTrue(expectedWithInterrupt instanceof XAException);
                        assertCause(expectedWithInterrupt, new Class[]{ConnectionClosedException.class, InterruptedException.class});
                    }
                    session.close();

                    assertTrue("Was interrupted", Thread.currentThread().isInterrupted());

                } catch (Throwable error) {
                    error.printStackTrace();
                    errors.add(error);
                }
            }
        });

        assertTrue("got to blocking call", blockedServerSize.await(20, TimeUnit.SECONDS));

        // will interrupt
        executorService.shutdownNow();
        canContinue.countDown();

        assertTrue("job done", executorService.awaitTermination(20, TimeUnit.SECONDS));

        assertTrue("no errors: " + errors, errors.isEmpty());
    }

    private void assertCause(Throwable expectedWithInterrupt, Class[] exceptionClazzes) {
        Throwable candidate = expectedWithInterrupt;

        while (candidate != null) {
            for (Class<?> exceptionClazz: exceptionClazzes) {
                if (exceptionClazz.isInstance(candidate)) {
                    return;
                }
            }
            candidate = candidate.getCause();
        }
        LOG.error("ex", expectedWithInterrupt);
        fail("no expected type as cause:" + expectedWithInterrupt);
    }

    public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            public int getFormatId() {
                return 87;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };

    }

    private void publishAMessage() throws IOException, XAException, JMSException {
        Xid tid = createXid();
        resource.start(tid, XAResource.TMNOFLAGS);
        MessageProducer producer = session.createProducer(dest);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        producer.send(message);
        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);
        session.close();
    }


    private String getName() {
        return this.getClass().getName();
    }
}
