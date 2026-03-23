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
package org.apache.activemq.ra;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.resource.spi.BootstrapContext;
import jakarta.resource.spi.endpoint.MessageEndpointFactory;
import jakarta.resource.spi.work.ExecutionContext;
import jakarta.resource.spi.work.Work;
import jakarta.resource.spi.work.WorkListener;
import jakarta.resource.spi.work.WorkManager;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@Ignore
public class ServerSessionImplTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSessionImplTest.class);
    private static final String BROKER_URL = "vm://localhost?broker.persistent=false";

    private ServerSessionImpl serverSession;
    private ServerSessionPoolImpl pool;
    private WorkManager workManager;
    private MessageEndpointProxy messageEndpoint;
    private ActiveMQConnection con;
    private ActiveMQSession session;
    private ActiveMQEndpointWorker endpointWorker;

    private MessageEndpointFactory messageEndpointFactory;
    private MessageResourceAdapter resourceAdapter;
    private ActiveMQEndpointActivationKey key;
    private MessageActivationSpec messageActivationSpec;
    private BootstrapContext bootstrapContext;

    @Before
    public void setUp() throws Exception {
        org.apache.activemq.ActiveMQConnectionFactory factory = new org.apache.activemq.ActiveMQConnectionFactory(BROKER_URL);
        con = (ActiveMQConnection) factory.createConnection();
        con.start();
        session = (ActiveMQSession) con.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private void setupCommonMocks() throws Exception {
        messageEndpointFactory = mock(MessageEndpointFactory.class);
        resourceAdapter = mock(MessageResourceAdapter.class);
        key = mock(ActiveMQEndpointActivationKey.class);
        messageEndpoint = mock(MessageEndpointProxy.class);
        workManager = mock(WorkManager.class);
        messageActivationSpec = mock(MessageActivationSpec.class);
        bootstrapContext = mock(BootstrapContext.class);

        lenient().when(bootstrapContext.getWorkManager()).thenReturn(workManager);
        lenient().when(resourceAdapter.getBootstrapContext()).thenReturn(bootstrapContext);
        lenient().when(messageEndpointFactory.isDeliveryTransacted(any(Method.class))).thenReturn(Boolean.FALSE);
        lenient().when(key.getMessageEndpointFactory()).thenReturn(messageEndpointFactory);
        lenient().when(key.getActivationSpec()).thenReturn(messageActivationSpec);
        lenient().when(messageActivationSpec.isUseJndi()).thenReturn(Boolean.FALSE);
        lenient().when(messageActivationSpec.getDestinationType()).thenReturn("jakarta.jms.Queue");
        lenient().when(messageActivationSpec.getDestination()).thenReturn("Queue");
        lenient().when(messageActivationSpec.getAcknowledgeModeForSession()).thenReturn(1);
        lenient().when(messageActivationSpec.getEnableBatchBooleanValue()).thenReturn(Boolean.FALSE);
        lenient().when(messageActivationSpec.isUseRAManagedTransactionEnabled()).thenReturn(Boolean.TRUE);
        lenient().when(messageEndpointFactory.createEndpoint(isNull())).thenReturn(messageEndpoint);
    }

    @After
    public void tearDown() throws Exception {
        if (con != null) {
            con.close();
        }
    }

    @Test
    public void testRunDetectsStoppedSession() throws Exception {

        pool = mock(ServerSessionPoolImpl.class);
        workManager = mock(WorkManager.class);
        messageEndpoint = mock(MessageEndpointProxy.class);

        serverSession = new ServerSessionImpl(pool, session, workManager, messageEndpoint, false, 10);

        con.close();
        serverSession.run();

        verify(pool).removeFromPool(same(serverSession));
    }

    @Test
    public void testCloseCanStopActiveSession() throws Exception {

        final int maxMessages = 4000;
        final CountDownLatch messageCount = new CountDownLatch(maxMessages);

        setupCommonMocks();
        lenient().when(messageActivationSpec.getMaxSessionsIntValue()).thenReturn(1);

        lenient().doAnswer(invocation -> null).when(workManager).scheduleWork(
            any(Work.class), anyLong(), any(ExecutionContext.class), any(WorkListener.class));

        lenient().doNothing().when(messageEndpoint).beforeDelivery(any(Method.class));
        lenient().doAnswer(invocation -> {
            messageCount.countDown();
            if (messageCount.getCount() < maxMessages - 11) {
                TimeUnit.MILLISECONDS.sleep(200);
            }
            return null;
        }).when(messageEndpoint).onMessage(any(jakarta.jms.Message.class));
        lenient().doNothing().when(messageEndpoint).afterDelivery();
        lenient().doNothing().when(messageEndpoint).release();

        endpointWorker = new ActiveMQEndpointWorker(resourceAdapter, key);
        endpointWorker.setConnection(con);
        pool = new ServerSessionPoolImpl(endpointWorker, 2);

        endpointWorker.start();
        final ServerSessionImpl serverSession1 = (ServerSessionImpl) pool.getServerSession();

        // preload the session dispatch queue to keep the session active
        ActiveMQSession session1 = (ActiveMQSession) serverSession1.getSession();
        for (int i = 0; i < maxMessages; i++) {
            MessageDispatch messageDispatch = new MessageDispatch();
            ActiveMQMessage message = new ActiveMQTextMessage();
            message.setMessageId(new MessageId("0:0:0:" + i));
            message.getMessageId().setBrokerSequenceId(i);
            messageDispatch.setMessage(message);
            messageDispatch.setConsumerId(new ConsumerId("0:0:0"));
            session1.dispatch(messageDispatch);
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch runState = new CountDownLatch(1);
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSession1.run();
                    runState.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return messageCount.getCount() < maxMessages - 10;
            }
        });
        assertTrue("some messages consumed", messageCount.getCount() < maxMessages);
        LOG.info("Closing pool on {}", messageCount.getCount());
        pool.close();

        assertTrue("run has completed", runState.await(20, TimeUnit.SECONDS));
        assertTrue("not all messages consumed", messageCount.getCount() > 0);
    }

    @Test
    public void testGetWhenClosed() throws Exception {

        final int maxMessages = 2000;
        final AtomicReference<CountDownLatch> messageCountRef = new AtomicReference<CountDownLatch>();

        ExecutorService executorService = Executors.newCachedThreadPool();

        setupCommonMocks();
        lenient().when(messageActivationSpec.getMaxSessionsIntValue()).thenReturn(10);

        lenient().doAnswer(invocation -> {
            LOG.info("Work manager invocation: " + invocation);

            if (invocation.getArgument(0) instanceof ServerSessionImpl) {
                final ServerSessionImpl serverSession1 = (ServerSessionImpl) invocation.getArgument(0);
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            serverSession1.run();
                        } catch (Exception e) {
                            LOG.error("Error on Work run: {}", serverSession1, e);
                            e.printStackTrace();
                        }
                    }
                });
            }
            return null;
        }).when(workManager).scheduleWork(any(Work.class), anyLong(), any(ExecutionContext.class), any(WorkListener.class));

        lenient().doNothing().when(messageEndpoint).beforeDelivery(any(Method.class));
        lenient().doAnswer(invocation -> {
            messageCountRef.get().countDown();
            return null;
        }).when(messageEndpoint).onMessage(any(jakarta.jms.Message.class));
        lenient().doAnswer(invocation -> {
            TransactionInfo transactionInfo = new TransactionInfo();
            transactionInfo.setType(TransactionInfo.END);
            LOG.info("AfterDelivery on: " + messageCountRef.get().getCount());
            return null;
        }).when(messageEndpoint).afterDelivery();
        lenient().doNothing().when(messageEndpoint).release();

        endpointWorker = new ActiveMQEndpointWorker(resourceAdapter, key);
        endpointWorker.setConnection(con);


        for (int i=0; i<40; i++) {
            final int iteration  = i;
            LOG.info("ITERATION: " +  iteration);
            pool = new ServerSessionPoolImpl(endpointWorker, 2);
            endpointWorker.start();

            messageCountRef.set(new CountDownLatch(maxMessages));

            final CountDownLatch senderDone = new CountDownLatch(1);
            final CountDownLatch messageSent = new CountDownLatch(maxMessages);
            final AtomicBoolean foundClosedSession = new AtomicBoolean(false);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        // preload the session dispatch queue to keep the session active

                        for (int i = 0; i < maxMessages; i++) {
                            MessageDispatch messageDispatch = new MessageDispatch();
                            ActiveMQMessage message = new ActiveMQTextMessage();
                            message.setMessageId(new MessageId("0:0:0:" + i));
                            message.getMessageId().setBrokerSequenceId(i);
                            messageDispatch.setMessage(message);
                            messageDispatch.setConsumerId(new ConsumerId("0:0:0"));
                            ServerSessionImpl serverSession1 = null;
                            try {
                                serverSession1 = (ServerSessionImpl) pool.getServerSession();
                                ActiveMQSession session1 = (ActiveMQSession) serverSession1.getSession();
                                if (session1.isClosed()) {
                                    // closed flag is not volatile - ok to give a whirl with it closed
                                    foundClosedSession.set(true);
                                }
                                session1.dispatch(messageDispatch);
                                messageSent.countDown();
                                serverSession1.start();
                            } catch (JMSException okOnClose) {
                                LOG.info("Exception on dispatch to {}", serverSession1, okOnClose);
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        senderDone.countDown();
                    }
                }
            });

            assertTrue("[" + iteration + "] Some messages dispatched", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    LOG.info("[" + iteration + "] Wait before close work MessageSent: " + messageSent.getCount() + ", messages got: "+ messageCountRef.get().getCount());
                    return messageSent.getCount() < maxMessages - 20 && messageCountRef.get().getCount() < maxMessages - 5;
                }
            }, 5000, 10));

            assertTrue("some messages consumed", messageCountRef.get().getCount() < maxMessages);

            final CountDownLatch closeDone = new CountDownLatch(1);
            final CountDownLatch closeSuccess = new CountDownLatch(1);

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.info("[" + iteration + "] Closing pool on delivered {} and dispatched {}", messageSent.getCount(), messageCountRef.get().getCount());
                    try {
                        pool.close();
                        closeSuccess.countDown();
                    } catch (InvalidMessageEndpointException error) {
                        LOG.error("Ex on pool close", error);
                        //error.printStackTrace();
                    } finally {
                        closeDone.countDown();
                    }
                }
            });

            assertTrue("[" + iteration + "] Pool close does not block", closeDone.await(10, TimeUnit.SECONDS));
            assertTrue("[" + iteration + "] Pool close ok", closeSuccess.await(10, TimeUnit.MILLISECONDS));

            assertTrue("[" + iteration + "] sender complete", senderDone.await(30, TimeUnit.SECONDS));
        }
    }



    @Test
    public void testSessionReusedByPool() throws Exception {

        setupCommonMocks();
        lenient().when(messageActivationSpec.getMaxSessionsIntValue()).thenReturn(10);
        lenient().doNothing().when(messageEndpoint).release();

        endpointWorker = new ActiveMQEndpointWorker(resourceAdapter, key);
        endpointWorker.setConnection(con);

        pool = new ServerSessionPoolImpl(endpointWorker, 2);
        endpointWorker.start();

        // the test!
        ServerSessionImpl first = (ServerSessionImpl) pool.getServerSession();
        pool.returnToPool(first);

        ServerSessionImpl reused = (ServerSessionImpl) pool.getServerSession();
        assertEquals("got reuse", first, reused);
    }
}
