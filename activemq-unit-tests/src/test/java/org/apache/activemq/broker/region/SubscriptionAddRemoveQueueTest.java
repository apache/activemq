/*
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
package org.apache.activemq.broker.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.InvalidSelectorException;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SubscriptionAddRemoveQueueTest {

    private BrokerService brokerService;
    private Queue queue;
    private ConsumerInfo info = new ConsumerInfo();
    private List<SimpleImmediateDispatchSubscription> subs = new ArrayList<SimpleImmediateDispatchSubscription>();
    private ConnectionContext context = new ConnectionContext();
    private ProducerBrokerExchange producerBrokerExchange = new ProducerBrokerExchange();
    private ProducerInfo producerInfo = new ProducerInfo();
    private ProducerState producerState = new ProducerState(producerInfo);
    private ActiveMQDestination destination = new ActiveMQQueue("TEST");
    private int numSubscriptions = 1000;
    private boolean working = true;
    private int senders = 20;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.start();

        DestinationStatistics parentStats = new DestinationStatistics();
        parentStats.setEnabled(true);

        TaskRunnerFactory taskFactory = new TaskRunnerFactory();
        MessageStore store = null;

        info.setDestination(destination);
        info.setPrefetchSize(100);

        producerBrokerExchange.setProducerState(producerState);
        producerBrokerExchange.setConnectionContext(context);

        queue = new Queue(brokerService, destination, store, parentStats, taskFactory);
        queue.initialize();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test(timeout = 120000)
    public void testNoDispatchToRemovedConsumers() throws Exception {
        final AtomicInteger producerId = new AtomicInteger();
        Runnable sender = new Runnable() {
            @Override
            public void run() {
                AtomicInteger id = new AtomicInteger();
                int producerIdAndIncrement = producerId.getAndIncrement();
                while (working) {
                    try {
                        Message msg = new ActiveMQMessage();
                        msg.setDestination(destination);
                        msg.setMessageId(new MessageId(producerIdAndIncrement + ":0:" + id.getAndIncrement()));
                        queue.send(producerBrokerExchange, msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception in sendMessage, ex:" + e);
                    }
                }
            }
        };

        Runnable subRemover = new Runnable() {
            @Override
            public void run() {
                for (Subscription sub : subs) {
                    try {
                        queue.removeSubscription(context, sub, 0);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception in removeSubscription, ex:" + e);
                    }
                }
            }
        };

        for (int i = 0; i < numSubscriptions; i++) {
            SimpleImmediateDispatchSubscription sub = new SimpleImmediateDispatchSubscription();
            subs.add(sub);
            queue.addSubscription(context, sub);
        }

        assertEquals("there are X subscriptions", numSubscriptions, queue.getDestinationStatistics().getConsumers().getCount());
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < senders; i++) {
            executor.submit(sender);
        }

        assertTrue("All subs should have some locks", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                boolean allHaveLocks = true;

                for (SimpleImmediateDispatchSubscription sub : subs) {
                    if (!hasSomeLocks(sub.dispatched)) {
                        allHaveLocks = false;
                        break;
                    }
                }

                return allHaveLocks;
            }
        }));

        Future<?> result = executor.submit(subRemover);
        result.get();
        working = false;
        assertEquals("there are no subscriptions", 0, queue.getDestinationStatistics().getConsumers().getCount());

        for (SimpleImmediateDispatchSubscription sub : subs) {
            assertTrue("There are no locked messages in any removed subscriptions", !hasSomeLocks(sub.dispatched));
        }
    }

    private boolean hasSomeLocks(List<MessageReference> dispatched) {
        boolean hasLock = false;
        for (MessageReference mr : dispatched) {
            QueueMessageReference qmr = (QueueMessageReference) mr;
            if (qmr.getLockOwner() != null) {
                hasLock = true;
                break;
            }
        }
        return hasLock;
    }

    private class SimpleImmediateDispatchSubscription implements Subscription, LockOwner {

        private SubscriptionStatistics subscriptionStatistics = new SubscriptionStatistics();
        List<MessageReference> dispatched = new CopyOnWriteArrayList<MessageReference>();

        @Override
        public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception {
        }

        @Override
        public void add(MessageReference node) throws Exception {
            // immediate dispatch
            QueueMessageReference qmr = (QueueMessageReference) node;
            qmr.lock(this);
            dispatched.add(qmr);
        }

        @Override
        public ConnectionContext getContext() {
            return null;
        }

        @Override
        public int getCursorMemoryHighWaterMark() {
            return 0;
        }

        @Override
        public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        }

        @Override
        public boolean isSlowConsumer() {
            return false;
        }

        @Override
        public void unmatched(MessageReference node) throws IOException {
        }

        @Override
        public long getTimeOfLastMessageAck() {
            return 0;
        }

        @Override
        public long getConsumedCount() {
            return 0;
        }

        @Override
        public void incrementConsumedCount() {
        }

        @Override
        public void resetConsumedCount() {
        }

        @Override
        public void add(ConnectionContext context, Destination destination) throws Exception {
        }

        @Override
        public void destroy() {
        }

        @Override
        public void gc() {
        }

        @Override
        public ConsumerInfo getConsumerInfo() {
            return info;
        }

        @Override
        public long getDequeueCounter() {
            return 0;
        }

        @Override
        public long getDispatchedCounter() {
            return 0;
        }

        @Override
        public int getDispatchedQueueSize() {
            return 0;
        }

        @Override
        public long getEnqueueCounter() {
            return 0;
        }

        @Override
        public int getInFlightSize() {
            return 0;
        }

        @Override
        public int getInFlightUsage() {
            return 0;
        }

        @Override
        public ObjectName getObjectName() {
            return null;
        }

        @Override
        public int getPendingQueueSize() {
            return 0;
        }

        @Override
        public long getPendingMessageSize() {
            return 0;
        }

        @Override
        public int getPrefetchSize() {
            return 0;
        }

        @Override
        public String getSelector() {
            return null;
        }

        @Override
        public boolean isBrowser() {
            return false;
        }

        @Override
        public boolean isFull() {
            return false;
        }

        @Override
        public boolean isHighWaterMark() {
            return false;
        }

        @Override
        public boolean isLowWaterMark() {
            return false;
        }

        @Override
        public boolean isRecoveryRequired() {
            return false;
        }

        @Override
        public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
            return true;
        }

        @Override
        public boolean matches(ActiveMQDestination destination) {
            return false;
        }

        @Override
        public void processMessageDispatchNotification(MessageDispatchNotification mdn) throws Exception {
        }

        @Override
        public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
            return null;
        }

        @Override
        public boolean isWildcard() {
            return false;
        }

        @Override
        public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
            return new ArrayList<MessageReference>(dispatched);
        }

        @Override
        public void setObjectName(ObjectName objectName) {
        }

        @Override
        public void setSelector(String selector) throws InvalidSelectorException, UnsupportedOperationException {
        }

        @Override
        public void updateConsumerPrefetch(int newPrefetch) {
        }

        @Override
        public boolean addRecoveredMessage(ConnectionContext context, MessageReference message) throws Exception {
            return false;
        }

        @Override
        public ActiveMQDestination getActiveMQDestination() {
            return null;
        }

        @Override
        public int getLockPriority() {
            return 0;
        }

        @Override
        public boolean isLockExclusive() {
            return false;
        }

        @Override
        public int countBeforeFull() {
            return 10;
        }

        @Override
        public SubscriptionStatistics getSubscriptionStatistics() {
            return subscriptionStatistics;
        }

        @Override
        public long getInFlightMessageSize() {
            return subscriptionStatistics.getInflightMessageSize().getTotalSize();
        }
    }
}
