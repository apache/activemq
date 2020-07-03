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
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JMock.class)
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
    private Mockery context;

    @Before
    public void setUp() throws Exception {
        context = new Mockery() {
            {
                setImposteriser(ClassImposteriser.INSTANCE);
            }
        };

        org.apache.activemq.ActiveMQConnectionFactory factory = new org.apache.activemq.ActiveMQConnectionFactory(BROKER_URL);
        con = (ActiveMQConnection) factory.createConnection();
        con.start();
        session = (ActiveMQSession) con.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if (con != null) {
            con.close();
        }
    }

    @Test
    public void testRunDetectsStoppedSession() throws Exception {

        pool = context.mock(ServerSessionPoolImpl.class);
        workManager = context.mock(WorkManager.class);
        messageEndpoint = context.mock(MessageEndpointProxy.class);

        serverSession = new ServerSessionImpl(pool, session, workManager, messageEndpoint, false, 10);

        con.close();
        context.checking(new Expectations() {
            {
                oneOf(pool).removeFromPool(with(same(serverSession)));
            }
        });
        serverSession.run();
    }

    @Test
    public void testCloseCanStopActiveSession() throws Exception {

        final int maxMessages = 4000;
        final CountDownLatch messageCount = new CountDownLatch(maxMessages);

        final MessageEndpointFactory messageEndpointFactory = context.mock(MessageEndpointFactory.class);
        final MessageResourceAdapter resourceAdapter = context.mock(MessageResourceAdapter.class);
        final ActiveMQEndpointActivationKey key = context.mock(ActiveMQEndpointActivationKey.class);
        messageEndpoint = context.mock(MessageEndpointProxy.class);
        workManager = context.mock(WorkManager.class);
        final MessageActivationSpec messageActivationSpec = context.mock(MessageActivationSpec.class);
        final BootstrapContext boostrapContext = context.mock(BootstrapContext.class);
        context.checking(new Expectations() {
            {
                allowing(boostrapContext).getWorkManager();
                will(returnValue(workManager));
                allowing(resourceAdapter).getBootstrapContext();
                will(returnValue(boostrapContext));
                allowing(messageEndpointFactory).isDeliveryTransacted(with(any(Method.class)));
                will(returnValue(Boolean.FALSE));
                allowing(key).getMessageEndpointFactory();
                will(returnValue(messageEndpointFactory));
                allowing(key).getActivationSpec();
                will(returnValue(messageActivationSpec));
                allowing(messageActivationSpec).isUseJndi();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).getDestinationType();
                will(returnValue("javax.jms.Queue"));
                allowing(messageActivationSpec).getDestination();
                will(returnValue("Queue"));
                allowing(messageActivationSpec).getAcknowledgeModeForSession();
                will(returnValue(1));
                allowing(messageActivationSpec).getMaxSessionsIntValue();
                will(returnValue(1));
                allowing(messageActivationSpec).getEnableBatchBooleanValue();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).isUseRAManagedTransactionEnabled();
                will(returnValue(Boolean.TRUE));
                allowing(messageEndpointFactory).createEndpoint(with(any(XAResource.class)));
                will(returnValue(messageEndpoint));

                allowing(workManager).scheduleWork((Work) with(Matchers.instanceOf(Work.class)), with(any(long.class)), with(any(ExecutionContext.class)),
                    with(any(WorkListener.class)));
                will(new Action() {
                    @Override
                    public Object invoke(Invocation invocation) throws Throwable {
                        return null;
                    }

                    @Override
                    public void describeTo(Description description) {
                    }
                });

                allowing(messageEndpoint).beforeDelivery((Method) with(Matchers.instanceOf(Method.class)));
                allowing(messageEndpoint).onMessage(with(any(javax.jms.Message.class)));
                will(new Action() {
                    @Override
                    public Object invoke(Invocation invocation) throws Throwable {
                        messageCount.countDown();
                        if (messageCount.getCount() < maxMessages - 11) {
                            TimeUnit.MILLISECONDS.sleep(200);
                        }
                        return null;
                    }

                    @Override
                    public void describeTo(Description description) {
                        description.appendText("Keep message count");
                    }
                });
                allowing(messageEndpoint).afterDelivery();
                allowing(messageEndpoint).release();

            }
        });

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


        final MessageEndpointFactory messageEndpointFactory = context.mock(MessageEndpointFactory.class);
        final MessageResourceAdapter resourceAdapter = context.mock(MessageResourceAdapter.class);
        final ActiveMQEndpointActivationKey key = context.mock(ActiveMQEndpointActivationKey.class);
        messageEndpoint = context.mock(MessageEndpointProxy.class);
        workManager = context.mock(WorkManager.class);
        final MessageActivationSpec messageActivationSpec = context.mock(MessageActivationSpec.class);
        final BootstrapContext boostrapContext = context.mock(BootstrapContext.class);
        context.checking(new Expectations() {
            {
                allowing(boostrapContext).getWorkManager();
                will(returnValue(workManager));
                allowing(resourceAdapter).getBootstrapContext();
                will(returnValue(boostrapContext));
                allowing(messageEndpointFactory).isDeliveryTransacted(with(any(Method.class)));
                will(returnValue(Boolean.FALSE));
                allowing(key).getMessageEndpointFactory();
                will(returnValue(messageEndpointFactory));
                allowing(key).getActivationSpec();
                will(returnValue(messageActivationSpec));
                allowing(messageActivationSpec).isUseJndi();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).getDestinationType();
                will(returnValue("javax.jms.Queue"));
                allowing(messageActivationSpec).getDestination();
                will(returnValue("Queue"));
                allowing(messageActivationSpec).getAcknowledgeModeForSession();
                will(returnValue(1));
                allowing(messageActivationSpec).getMaxSessionsIntValue();
                will(returnValue(10));
                allowing(messageActivationSpec).getEnableBatchBooleanValue();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).isUseRAManagedTransactionEnabled();
                will(returnValue(Boolean.TRUE));
                allowing(messageEndpointFactory).createEndpoint(with(any(XAResource.class)));
                will(returnValue(messageEndpoint));

                allowing(workManager).scheduleWork((Work) with(Matchers.instanceOf(Work.class)), with(any(long.class)), with(any(ExecutionContext.class)),
                        with(any(WorkListener.class)));
                will(new Action() {
                    @Override
                    public Object invoke(Invocation invocation) throws Throwable {
                        LOG.info("Wok manager invocation: " + invocation);

                        if (invocation.getParameter(0) instanceof ServerSessionImpl) {
                            final ServerSessionImpl serverSession1 = (ServerSessionImpl)invocation.getParameter(0);
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
                    }

                    @Override
                    public void describeTo(Description description) {
                    }
                });

                allowing(messageEndpoint).beforeDelivery((Method) with(Matchers.instanceOf(Method.class)));
                allowing(messageEndpoint).onMessage(with(any(javax.jms.Message.class)));
                will(new Action() {
                    @Override
                    public Object invoke(Invocation invocation) throws Throwable {
                        messageCountRef.get().countDown();
                        return null;
                    }

                    @Override
                    public void describeTo(Description description) {
                        description.appendText("Keep message count");
                    }
                });
                allowing(messageEndpoint).afterDelivery();
                will(new Action() {
                    @Override
                    public void describeTo(Description description) {
                        description.appendText("do sync work on broker");
                    }

                    @Override
                    public Object invoke(Invocation invocation) throws Throwable {
                        TransactionInfo transactionInfo = new TransactionInfo();
                        transactionInfo.setType(TransactionInfo.END);
                        LOG.info("AfterDelivery on: " + messageCountRef.get().getCount());
                        return null;
                    }
                });
                allowing(messageEndpoint).release();

            }
        });

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

        final MessageEndpointFactory messageEndpointFactory = context.mock(MessageEndpointFactory.class);
        final MessageResourceAdapter resourceAdapter = context.mock(MessageResourceAdapter.class);
        final ActiveMQEndpointActivationKey key = context.mock(ActiveMQEndpointActivationKey.class);
        messageEndpoint = context.mock(MessageEndpointProxy.class);
        workManager = context.mock(WorkManager.class);
        final MessageActivationSpec messageActivationSpec = context.mock(MessageActivationSpec.class);
        final BootstrapContext bootstrapContext = context.mock(BootstrapContext.class);
        context.checking(new Expectations() {
            {
                allowing(bootstrapContext).getWorkManager();
                will(returnValue(workManager));
                allowing(resourceAdapter).getBootstrapContext();
                will(returnValue(bootstrapContext));
                allowing(messageEndpointFactory).isDeliveryTransacted(with(any(Method.class)));
                will(returnValue(Boolean.FALSE));
                allowing(key).getMessageEndpointFactory();
                will(returnValue(messageEndpointFactory));
                allowing(key).getActivationSpec();
                will(returnValue(messageActivationSpec));
                allowing(messageActivationSpec).isUseJndi();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).getDestinationType();
                will(returnValue("javax.jms.Queue"));
                allowing(messageActivationSpec).getDestination();
                will(returnValue("Queue"));
                allowing(messageActivationSpec).getAcknowledgeModeForSession();
                will(returnValue(1));
                allowing(messageActivationSpec).getMaxSessionsIntValue();
                will(returnValue(10));
                allowing(messageActivationSpec).getEnableBatchBooleanValue();
                will(returnValue(Boolean.FALSE));
                allowing(messageActivationSpec).isUseRAManagedTransactionEnabled();
                will(returnValue(Boolean.TRUE));
                allowing(messageEndpointFactory).createEndpoint(with(any(XAResource.class)));
                will(returnValue(messageEndpoint));

                allowing(workManager).scheduleWork((Work) with(Matchers.instanceOf(Work.class)), with(any(long.class)), with(any(ExecutionContext.class)),
                        with(any(WorkListener.class)));
                allowing(messageEndpoint).release();
            }
        });

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
