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
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.InvalidSelectorException;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.SubscriptionStatistics;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gtully
 * @see https://issues.apache.org/activemq/browse/AMQ-2020
 **/
public class QueueDuplicatesFromStoreTest extends TestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(QueueDuplicatesFromStoreTest.class);

    ActiveMQQueue destination = new ActiveMQQueue("queue-"
            + QueueDuplicatesFromStoreTest.class.getSimpleName());
    BrokerService brokerService;

    final static String mesageIdRoot = "11111:22222:";
    final int messageBytesSize = 256;
    final String text = new String(new byte[messageBytesSize]);

    final int ackStartIndex = 100;
    final int ackWindow = 50;
    final int ackBatchSize = 50;
    final int fullWindow = 200;
    protected int count = 5000;

    @Override
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.setUseJmx(false);
        brokerService.deleteAllMessages();
        brokerService.start();
    }

    protected BrokerService createBroker() throws Exception {
        return new BrokerService();
    }

    @Override
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    public void testNoDuplicateAfterCacheFullAndAckedWithLargeAuditDepth() throws Exception {
        doTestNoDuplicateAfterCacheFullAndAcked(1024*10);
    }

    public void testNoDuplicateAfterCacheFullAndAckedWithSmallAuditDepth() throws Exception {
        doTestNoDuplicateAfterCacheFullAndAcked(512);
    }

    public void doTestNoDuplicateAfterCacheFullAndAcked(final int auditDepth) throws Exception {
        final PersistenceAdapter persistenceAdapter =  brokerService.getPersistenceAdapter();
        final MessageStore queueMessageStore =
                persistenceAdapter.createQueueMessageStore(destination);
        final ConnectionContext contextNotInTx = new ConnectionContext();
        final ConsumerInfo consumerInfo = new ConsumerInfo();
        final DestinationStatistics destinationStatistics = new DestinationStatistics();
        consumerInfo.setExclusive(true);
        final Queue queue = new Queue(brokerService, destination,
                queueMessageStore, destinationStatistics, brokerService.getTaskRunnerFactory());

        // a workaround for this issue
        // queue.setUseCache(false);
        queue.systemUsage.getMemoryUsage().setLimit(1024 * 1024 * 10);
        queue.setMaxAuditDepth(auditDepth);
        queue.initialize();
        queue.start();


        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        ProducerInfo producerInfo = new ProducerInfo();
        ProducerState producerState = new ProducerState(producerInfo);
        producerExchange.setProducerState(producerState);
        producerExchange.setConnectionContext(contextNotInTx);

        final CountDownLatch receivedLatch = new CountDownLatch(count);
        final AtomicLong ackedCount = new AtomicLong(0);
        final AtomicLong enqueueCounter = new AtomicLong(0);
        final Vector<String> errors = new Vector<String>();

        // populate the queue store, exceed memory limit so that cache is disabled
        for (int i = 0; i < count; i++) {
            Message message = getMessage(i);
            queue.send(producerExchange, message);
        }

        assertEquals("store count is correct", count, queueMessageStore.getMessageCount());

        // pull from store in small windows
        Subscription subscription = new Subscription() {

            private SubscriptionStatistics subscriptionStatistics = new SubscriptionStatistics();

            @Override
            public void add(MessageReference node) throws Exception {
                if (enqueueCounter.get() != node.getMessageId().getProducerSequenceId()) {
                    errors.add("Not in sequence at: " + enqueueCounter.get() + ", received: "
                            + node.getMessageId().getProducerSequenceId());
                }
                assertEquals("is in order", enqueueCounter.get(), node
                        .getMessageId().getProducerSequenceId());
                receivedLatch.countDown();
                enqueueCounter.incrementAndGet();
                node.decrementReferenceCount();
            }

            @Override
            public void add(ConnectionContext context, Destination destination)
                    throws Exception {
            }

            @Override
            public int countBeforeFull() {
                if (isFull()) {
                    return 0;
                } else {
                    return fullWindow - (int) (enqueueCounter.get() - ackedCount.get());
                }
            }

            @Override
            public void destroy() {
            };

            @Override
            public void gc() {
            }

            @Override
            public ConsumerInfo getConsumerInfo() {
                return consumerInfo;
            }

            @Override
            public ConnectionContext getContext() {
                return null;
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
                return (enqueueCounter.get() - ackedCount.get()) >= fullWindow;
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
            public boolean matches(MessageReference node,
                    MessageEvaluationContext context) throws IOException {
                return true;
            }

            @Override
            public boolean matches(ActiveMQDestination destination) {
                return true;
            }

            @Override
            public void processMessageDispatchNotification(
                    MessageDispatchNotification mdn) throws Exception {
            }

            @Override
            public Response pullMessage(ConnectionContext context,
                    MessagePull pull) throws Exception {
                return null;
            }

            @Override
            public boolean isWildcard() {
                return false;
            }

            @Override
            public List<MessageReference> remove(ConnectionContext context,
                    Destination destination) throws Exception {
                return null;
            }

            @Override
            public void setObjectName(ObjectName objectName) {
            }

            @Override
            public void setSelector(String selector)
                    throws InvalidSelectorException,
                    UnsupportedOperationException {
            }

            @Override
            public void updateConsumerPrefetch(int newPrefetch) {
            }

            @Override
            public boolean addRecoveredMessage(ConnectionContext context,
                    MessageReference message) throws Exception {
                return false;
            }

            @Override
            public ActiveMQDestination getActiveMQDestination() {
                return destination;
            }

            @Override
            public void acknowledge(ConnectionContext context, MessageAck ack)
                    throws Exception {
            }

            @Override
            public int getCursorMemoryHighWaterMark(){
                return 0;
            }

            @Override
            public void setCursorMemoryHighWaterMark(
                    int cursorMemoryHighWaterMark) {
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
            public void incrementConsumedCount(){

            }

            @Override
            public void resetConsumedCount(){

            }

            @Override
            public SubscriptionStatistics getSubscriptionStatistics() {
                return subscriptionStatistics;
            }

            @Override
            public long getInFlightMessageSize() {
                return subscriptionStatistics.getInflightMessageSize().getTotalSize();
            }
        };

        queue.addSubscription(contextNotInTx, subscription);
        int removeIndex = 0;
        do {
            // Simulate periodic acks in small but recent windows
            long receivedCount = enqueueCounter.get();
            if (receivedCount > ackStartIndex) {
                if (receivedCount >= removeIndex + ackWindow) {
                    for (int j = 0; j < ackBatchSize; j++, removeIndex++) {
                        ackedCount.incrementAndGet();
                        MessageAck ack = new MessageAck();
                        ack.setLastMessageId(new MessageId(mesageIdRoot
                                + removeIndex));
                        ack.setMessageCount(1);
                        queue.removeMessage(contextNotInTx, subscription,
                                new IndirectMessageReference(
                                        getMessage(removeIndex)), ack);
                        queue.wakeup();

                    }
                    if (removeIndex % 1000 == 0) {
                        LOG.info("acked: " + removeIndex);
                        persistenceAdapter.checkpoint(true);
                    }
                }
            }

        } while (!receivedLatch.await(0, TimeUnit.MILLISECONDS) && errors.isEmpty());

        assertTrue("There are no errors: " + errors, errors.isEmpty());
        assertEquals(count, enqueueCounter.get());
        assertEquals("store count is correct", count - removeIndex,
                queueMessageStore.getMessageCount());
    }

    private Message getMessage(int i) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setMessageId(new MessageId(mesageIdRoot + i));
        message.setDestination(destination);
        message.setPersistent(true);
        message.setResponseRequired(true);
        message.setText("Msg:" + i + " " + text);
        assertEquals(message.getMessageId().getProducerSequenceId(), i);
        return message;
    }
}
