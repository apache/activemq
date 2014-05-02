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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import junit.framework.TestCase;

public class SubscriptionAddRemoveQueueTest extends TestCase {

    Queue queue;

    ConsumerInfo info = new ConsumerInfo();
    List<SimpleImmediateDispatchSubscription> subs = new ArrayList<SimpleImmediateDispatchSubscription>();
    ConnectionContext context = new ConnectionContext();
    ProducerBrokerExchange producerBrokerExchange = new ProducerBrokerExchange();
    ProducerInfo producerInfo = new ProducerInfo();
    ProducerState producerState = new ProducerState(producerInfo);
    ActiveMQDestination destination = new ActiveMQQueue("TEST");
    int numSubscriptions = 1000;
    boolean working = true;
    int senders = 20;


    @Override
    public void setUp() throws Exception {
        BrokerService brokerService = new BrokerService();
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

    public void testNoDispatchToRemovedConsumers() throws Exception {
        final AtomicInteger producerId = new AtomicInteger();
        Runnable sender = new Runnable() {
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

        for (int i=0;i<numSubscriptions; i++) {
            SimpleImmediateDispatchSubscription sub = new SimpleImmediateDispatchSubscription();
            subs.add(sub);
            queue.addSubscription(context, sub);
        }
        assertEquals("there are X subscriptions", numSubscriptions, queue.getDestinationStatistics().getConsumers().getCount());
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i=0; i<senders ; i++) {
            executor.submit(sender);
        }

        Thread.sleep(1000);
        for (SimpleImmediateDispatchSubscription sub : subs) {
            assertTrue("There are some locked messages in the subscription", hasSomeLocks(sub.dispatched));
        }

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
        for (MessageReference mr: dispatched) {
            QueueMessageReference qmr = (QueueMessageReference) mr;
            if (qmr.getLockOwner() != null) {
                hasLock = true;
                break;
            }
        }
        return hasLock;
    }

    public class SimpleImmediateDispatchSubscription implements Subscription, LockOwner {

        List<MessageReference> dispatched =
            Collections.synchronizedList(new ArrayList<MessageReference>());

        public void acknowledge(ConnectionContext context, MessageAck ack)
                throws Exception {
        }

        public void add(MessageReference node) throws Exception {
            // immediate dispatch
            QueueMessageReference  qmr = (QueueMessageReference)node;
            qmr.lock(this);
            dispatched.add(qmr);
        }

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

        public void add(ConnectionContext context, Destination destination)
                throws Exception {
        }

        public void destroy() {
        }

        public void gc() {
        }

        public ConsumerInfo getConsumerInfo() {
            return info;
        }

        public long getDequeueCounter() {
            return 0;
        }

        public long getDispatchedCounter() {
            return 0;
        }

        public int getDispatchedQueueSize() {
            return 0;
        }

        public long getEnqueueCounter() {
            return 0;
        }

        public int getInFlightSize() {
            return 0;
        }

        public int getInFlightUsage() {
            return 0;
        }

        public ObjectName getObjectName() {
            return null;
        }

        public int getPendingQueueSize() {
            return 0;
        }

        public int getPrefetchSize() {
            return 0;
        }

        public String getSelector() {
            return null;
        }

        public boolean isBrowser() {
            return false;
        }

        public boolean isFull() {
            return false;
        }

        public boolean isHighWaterMark() {
            return false;
        }

        public boolean isLowWaterMark() {
            return false;
        }

        public boolean isRecoveryRequired() {
            return false;
        }

        public boolean isSlave() {
            return false;
        }

        public boolean matches(MessageReference node,
                MessageEvaluationContext context) throws IOException {
            return true;
        }

        public boolean matches(ActiveMQDestination destination) {
            return false;
        }

        public void processMessageDispatchNotification(
                MessageDispatchNotification mdn) throws Exception {
        }

        public Response pullMessage(ConnectionContext context, MessagePull pull)
                throws Exception {
            return null;
        }

        @Override
        public boolean isWildcard() {
            return false;
        }

        public List<MessageReference> remove(ConnectionContext context,
                Destination destination) throws Exception {
            return new ArrayList<MessageReference>(dispatched);
        }

        public void setObjectName(ObjectName objectName) {
        }

        public void setSelector(String selector)
                throws InvalidSelectorException, UnsupportedOperationException {
        }

        public void updateConsumerPrefetch(int newPrefetch) {
        }

        public boolean addRecoveredMessage(ConnectionContext context,
                MessageReference message) throws Exception {
            return false;
        }

        public ActiveMQDestination getActiveMQDestination() {
            return null;
        }

        public int getLockPriority() {
            return 0;
        }

        public boolean isLockExclusive() {
            return false;
        }

        public void addDestination(Destination destination) {
        }

        public void removeDestination(Destination destination) {
        }

        public int countBeforeFull() {
            return 10;
        }

    }
}
