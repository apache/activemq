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

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Session;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
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
}
