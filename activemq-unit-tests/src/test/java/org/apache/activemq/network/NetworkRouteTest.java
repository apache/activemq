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
package org.apache.activemq.network;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NetworkRouteTest {
    private IMocksControl control;
    private BrokerService brokerService;
    private Transport localBroker;
    private Transport remoteBroker;
    private TransportListener localListener;
    private TransportListener remoteListener;
    private MessageDispatch msgDispatch;
    private ActiveMQMessage path1Msg;
    private ActiveMQMessage path2Msg;
    private ActiveMQMessage removePath1Msg;
    private ActiveMQMessage removePath2Msg;

    // this sort of mockery is very brittle but it is fast!

    @Test
    public void verifyNoRemoveOnOneConduitRemove() throws Exception {
        localBroker.oneway(EasyMock.isA(ConsumerInfo.class));
        control.replay();

        remoteListener.onCommand(path2Msg);
        remoteListener.onCommand(path1Msg);

        remoteListener.onCommand(removePath2Msg);
        control.verify();
    }

    @Test
    public void addAndRemoveOppositeOrder() throws Exception {
        // from (1)
        localBroker.oneway(EasyMock.isA(ConsumerInfo.class));
        ArgHolder localConsumer = ArgHolder.holdArgsForLastObjectCall();
        // from (2a)
        remoteBroker.asyncRequest(EasyMock.isA(ActiveMQMessage.class), EasyMock.isA(ResponseCallback.class));
        ArgHolder firstMessageFuture = ArgHolder.holdArgsForLastFutureRequestCall();
        localBroker.oneway(EasyMock.isA(MessageAck.class));
        // from (2b)
        remoteBroker.asyncRequest(EasyMock.isA(ActiveMQMessage.class), EasyMock.isA(ResponseCallback.class));
        ArgHolder secondMessageFuture = ArgHolder.holdArgsForLastFutureRequestCall();

        localBroker.oneway(EasyMock.isA(MessageAck.class));
        // from (3)
        localBroker.oneway(EasyMock.isA(RemoveInfo.class));
        ExpectationWaiter waitForRemove = ExpectationWaiter.waiterForLastVoidCall();
        control.replay();

        // (1) send advisory of path 1
        remoteListener.onCommand(path1Msg);
        msgDispatch.setConsumerId(((ConsumerInfo) localConsumer.arguments[0]).getConsumerId());
        // send advisory of path 2, doesn't send a ConsumerInfo to localBroker
        remoteListener.onCommand(path2Msg);
        // (2a) send a message
        localListener.onCommand(msgDispatch);
        ResponseCallback callback = (ResponseCallback) firstMessageFuture.arguments[1];
        FutureResponse response = new FutureResponse(callback);
        response.set(new Response());

        // send advisory of path 2 remove, doesn't send a RemoveInfo to localBroker
        remoteListener.onCommand(removePath2Msg);
        // (2b) send a message
        localListener.onCommand(msgDispatch);
        callback = (ResponseCallback) secondMessageFuture.arguments[1];
        response = new FutureResponse(callback);
        response.set(new Response());

        // (3) send advisory of path 1 remove, sends a RemoveInfo to localBroker
        remoteListener.onCommand(removePath1Msg);
        waitForRemove.assertHappens(5, TimeUnit.SECONDS);
        // send a message, does not send message as in 2a and 2b
        localListener.onCommand(msgDispatch);

        control.verify();
    }

    @Test
    public void addAndRemoveSameOrder() throws Exception {
        // from (1)
        localBroker.oneway(EasyMock.isA(ConsumerInfo.class));
        ArgHolder localConsumer = ArgHolder.holdArgsForLastObjectCall();

        // from (2a)
        remoteBroker.asyncRequest(EasyMock.isA(ActiveMQMessage.class), EasyMock.isA(ResponseCallback.class));
        ArgHolder firstMessageFuture = ArgHolder.holdArgsForLastFutureRequestCall();

        localBroker.oneway(EasyMock.isA(MessageAck.class));

        // from (2b)
        remoteBroker.asyncRequest(EasyMock.isA(ActiveMQMessage.class), EasyMock.isA(ResponseCallback.class));
        ArgHolder secondMessageFuture = ArgHolder.holdArgsForLastFutureRequestCall();

        localBroker.oneway(EasyMock.isA(MessageAck.class));

        // from (3)
        localBroker.oneway(EasyMock.isA(RemoveInfo.class));
        ExpectationWaiter waitForRemove = ExpectationWaiter.waiterForLastVoidCall();
        control.replay();

        // (1) send advisory of path 1
        remoteListener.onCommand(path1Msg);
        msgDispatch.setConsumerId(((ConsumerInfo) localConsumer.arguments[0]).getConsumerId());
        // send advisory of path 2, doesn't send a ConsumerInfo to localBroker
        remoteListener.onCommand(path2Msg);
        // (2a) send a message
        localListener.onCommand(msgDispatch);
        ResponseCallback callback = (ResponseCallback) firstMessageFuture.arguments[1];
        FutureResponse response = new FutureResponse(callback);
        response.set(new Response());

        // send advisory of path 1 remove, shouldn't send a RemoveInfo to localBroker
        remoteListener.onCommand(removePath1Msg);
        // (2b) send a message, should send the message as in 2a
        localListener.onCommand(msgDispatch);
        callback = (ResponseCallback) secondMessageFuture.arguments[1];
        response = new FutureResponse(callback);
        response.set(new Response());

        // (3) send advisory of path 1 remove, should send a RemoveInfo to localBroker
        remoteListener.onCommand(removePath2Msg);
        waitForRemove.assertHappens(5, TimeUnit.SECONDS);
        // send a message, does not send message as in 2a
        localListener.onCommand(msgDispatch);

        control.verify();
    }

    @Before
    public void before() throws Exception {
        control = EasyMock.createControl();
        localBroker = control.createMock(Transport.class);
        remoteBroker = control.createMock(Transport.class);

        NetworkBridgeConfiguration configuration = new NetworkBridgeConfiguration();
        brokerService = new BrokerService();
        BrokerInfo remoteBrokerInfo = new BrokerInfo();

        configuration.setDuplex(true);
        configuration.setNetworkTTL(5);
        brokerService.setBrokerId("broker-1");
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.start();
        brokerService.waitUntilStarted();
        remoteBrokerInfo.setBrokerId(new BrokerId("remote-broker-id"));
        remoteBrokerInfo.setBrokerName("remote-broker-name");

        localBroker.setTransportListener(EasyMock.isA(TransportListener.class));
        ArgHolder localListenerRef = ArgHolder.holdArgsForLastVoidCall();

        remoteBroker.setTransportListener(EasyMock.isA(TransportListener.class));
        ArgHolder remoteListenerRef = ArgHolder.holdArgsForLastVoidCall();
        localBroker.start();
        remoteBroker.start();

        remoteBroker.oneway(EasyMock.isA(Object.class));
        EasyMock.expectLastCall().times(4);
        remoteBroker.oneway(EasyMock.isA(Object.class));
        ExpectationWaiter remoteInitWaiter = ExpectationWaiter.waiterForLastVoidCall();

        localBroker.oneway(remoteBrokerInfo);
        EasyMock.expect(localBroker.request(EasyMock.isA(Object.class)))
                .andReturn(null);
        EasyMock.expect(remoteBroker.narrow(TcpTransport.class)).andReturn(null);
        localBroker.oneway(EasyMock.isA(Object.class));
        ExpectationWaiter localInitWaiter = ExpectationWaiter.waiterForLastVoidCall();

        control.replay();

        DurableConduitBridge bridge = new DurableConduitBridge(configuration, localBroker, remoteBroker);
        bridge.setBrokerService(brokerService);
        bridge.start();

        localListener = (TransportListener) localListenerRef.getArguments()[0];
        Assert.assertNotNull(localListener);
        remoteListener = (TransportListener) remoteListenerRef.getArguments()[0];
        Assert.assertNotNull(remoteListener);

        remoteListener.onCommand(remoteBrokerInfo);

        remoteInitWaiter.assertHappens(5, TimeUnit.SECONDS);
        localInitWaiter.assertHappens(5, TimeUnit.SECONDS);

        control.verify();
        control.reset();

        ActiveMQMessage msg = new ActiveMQMessage();
        msg.setDestination(new ActiveMQTopic("test"));
        msgDispatch = new MessageDispatch();
        msgDispatch.setMessage(msg);
        msgDispatch.setDestination(msg.getDestination());

        ConsumerInfo path1 = new ConsumerInfo();
        path1.setDestination(msg.getDestination());
        path1.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn-id-1"), 1), 3));
        path1.setBrokerPath(new BrokerId[]{
                new BrokerId("remote-broker-id"),
                new BrokerId("server(1)-broker-id"),
        });
        path1Msg = new ActiveMQMessage();
        path1Msg.setDestination(AdvisorySupport.getConsumerAdvisoryTopic(path1.getDestination()));
        path1Msg.setDataStructure(path1);

        ConsumerInfo path2 = new ConsumerInfo();
        path2.setDestination(path1.getDestination());
        path2.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn-id-2"), 2), 4));
        path2.setBrokerPath(new BrokerId[]{
                new BrokerId("remote-broker-id"),
                new BrokerId("server(2)-broker-id"),
                new BrokerId("server(1)-broker-id"),
        });
        path2Msg = new ActiveMQMessage();
        path2Msg.setDestination(path1Msg.getDestination());
        path2Msg.setDataStructure(path2);

        RemoveInfo removePath1 = new RemoveInfo(path1.getConsumerId());
        RemoveInfo removePath2 = new RemoveInfo(path2.getConsumerId());

        removePath1Msg = new ActiveMQMessage();
        removePath1Msg.setDestination(path1Msg.getDestination());
        removePath1Msg.setDataStructure(removePath1);

        removePath2Msg = new ActiveMQMessage();
        removePath2Msg.setDestination(path1Msg.getDestination());
        removePath2Msg.setDataStructure(removePath2);
    }

    @After
    public void after() throws Exception {
        control.reset();
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    private static class ArgHolder {
        public Object[] arguments;

        public static ArgHolder holdArgsForLastVoidCall() {
            final ArgHolder holder = new ArgHolder();
            EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    Object[] args = EasyMock.getCurrentArguments();
                    holder.arguments = Arrays.copyOf(args, args.length);
                    return null;
                }
            });
            return holder;
        }

        public static ArgHolder holdArgsForLastObjectCall() {
            final ArgHolder holder = new ArgHolder();
            EasyMock.expect(new Object()).andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    Object[] args = EasyMock.getCurrentArguments();
                    holder.arguments = Arrays.copyOf(args, args.length);
                    return null;
                }
            });
            return holder;
        }

        public static ArgHolder holdArgsForLastFutureRequestCall() {
            final ArgHolder holder = new ArgHolder();
            EasyMock.expect(new FutureResponse(null)).andAnswer(new IAnswer<FutureResponse>() {
                @Override
                public FutureResponse answer() throws Throwable {
                    Object[] args = EasyMock.getCurrentArguments();
                    holder.arguments = Arrays.copyOf(args, args.length);
                    return null;
                }
            });

            return holder;
        }

        public Object[] getArguments() {
            Assert.assertNotNull(arguments);
            return arguments;
        }
    }

    private static class ExpectationWaiter {
        private CountDownLatch latch = new CountDownLatch(1);

        public static ExpectationWaiter waiterForLastVoidCall() {
            final ExpectationWaiter waiter = new ExpectationWaiter();
            EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
                @Override
                public Object answer() throws Throwable {
                    waiter.latch.countDown();
                    return null;
                }
            });
            return waiter;
        }

        public void assertHappens(long timeout, TimeUnit unit) throws InterruptedException {
            Assert.assertTrue(latch.await(timeout, unit));
        }
    }
}