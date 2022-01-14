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
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.SlowConsumerStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.util.SubscriptionKey;

/**
 *
 *
 */
public class DestinationFilter implements Destination {

    protected final Destination next;

    public DestinationFilter(Destination next) {
        this.next = next;
    }

    @Override
    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node) throws IOException {
        next.acknowledge(context, sub, ack, node);
    }

    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        next.addSubscription(context, sub);
    }

    @Override
    public Message[] browse() {
        return next.browse();
    }

    @Override
    public void dispose(ConnectionContext context) throws IOException {
        next.dispose(context);
    }

    @Override
    public boolean isDisposed() {
        return next.isDisposed();
    }

    @Override
    public void gc() {
        next.gc();
    }

    @Override
    public void markForGC(long timeStamp) {
        next.markForGC(timeStamp);
    }

    @Override
    public boolean canGC() {
        return next.canGC();
    }

    @Override
    public long getInactiveTimeoutBeforeGC() {
        return next.getInactiveTimeoutBeforeGC();
    }

    @Override
    public ActiveMQDestination getActiveMQDestination() {
        return next.getActiveMQDestination();
    }

    @Override
    public DeadLetterStrategy getDeadLetterStrategy() {
        return next.getDeadLetterStrategy();
    }

    @Override
    public DestinationStatistics getDestinationStatistics() {
        return next.getDestinationStatistics();
    }

    @Override
    public String getName() {
        return next.getName();
    }

    @Override
    public MemoryUsage getMemoryUsage() {
        return next.getMemoryUsage();
    }

    @Override
    public void setMemoryUsage(MemoryUsage memoryUsage) {
        next.setMemoryUsage(memoryUsage);
    }

    @Override
    public TempUsage getTempUsage() {
        return next.getTempUsage();
    }

    @Override
    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception {
        next.removeSubscription(context, sub, lastDeliveredSequenceId);
    }

    @Override
    public void send(ProducerBrokerExchange context, Message messageSend) throws Exception {
        next.send(context, messageSend);
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void stop() throws Exception {
        next.stop();
    }

    @Override
    public List<Subscription> getConsumers() {
        return next.getConsumers();
    }

    /**
     * Sends a message to the given destination which may be a wildcard
     *
     * @param context broker context
     * @param message message to send
     * @param destination possibly wildcard destination to send the message to
     * @throws Exception on error
     */
    protected void send(ProducerBrokerExchange context, Message message, ActiveMQDestination destination) throws Exception {
        Broker broker = context.getConnectionContext().getBroker();
        Set<Destination> destinations = broker.getDestinations(destination);

        for (Destination dest : destinations) {
            dest.send(context, message.copy());
        }
    }

    @Override
    public MessageStore getMessageStore() {
        return next.getMessageStore();
    }

    @Override
    public boolean isProducerFlowControl() {
        return next.isProducerFlowControl();
    }

    @Override
    public void setProducerFlowControl(boolean value) {
        next.setProducerFlowControl(value);
    }

    @Override
    public boolean isAlwaysRetroactive() {
        return next.isAlwaysRetroactive();
    }

    @Override
    public void setAlwaysRetroactive(boolean value) {
        next.setAlwaysRetroactive(value);
    }

    @Override
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        next.setBlockedProducerWarningInterval(blockedProducerWarningInterval);
    }

    @Override
    public long getBlockedProducerWarningInterval() {
        return next.getBlockedProducerWarningInterval();
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.addProducer(context, info);
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.removeProducer(context, info);
    }

    @Override
    public int getMaxAuditDepth() {
        return next.getMaxAuditDepth();
    }

    @Override
    public int getMaxProducersToAudit() {
        return next.getMaxProducersToAudit();
    }

    @Override
    public boolean isEnableAudit() {
        return next.isEnableAudit();
    }

    @Override
    public void setEnableAudit(boolean enableAudit) {
        next.setEnableAudit(enableAudit);
    }

    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        next.setMaxAuditDepth(maxAuditDepth);
    }

    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        next.setMaxProducersToAudit(maxProducersToAudit);
    }

    @Override
    public boolean isActive() {
        return next.isActive();
    }

    @Override
    public int getMaxPageSize() {
        return next.getMaxPageSize();
    }

    @Override
    public void setMaxPageSize(int maxPageSize) {
        next.setMaxPageSize(maxPageSize);
    }

    @Override
    public boolean isUseCache() {
        return next.isUseCache();
    }

    @Override
    public void setUseCache(boolean useCache) {
        next.setUseCache(useCache);
    }

    @Override
    public int getMinimumMessageSize() {
        return next.getMinimumMessageSize();
    }

    @Override
    public void setMinimumMessageSize(int minimumMessageSize) {
        next.setMinimumMessageSize(minimumMessageSize);
    }

    @Override
    public void wakeup() {
        next.wakeup();
    }

    @Override
    public boolean isLazyDispatch() {
        return next.isLazyDispatch();
    }

    @Override
    public void setLazyDispatch(boolean value) {
        next.setLazyDispatch(value);
    }

    public void messageExpired(ConnectionContext context, PrefetchSubscription prefetchSubscription, MessageReference node) {
        next.messageExpired(context, prefetchSubscription, node);
    }

    @Override
    public boolean iterate() {
        return next.iterate();
    }

    @Override
    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo) {
        next.fastProducer(context, producerInfo);
    }

    @Override
    public void isFull(ConnectionContext context, Usage<?> usage) {
        next.isFull(context, usage);
    }

    @Override
    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        next.messageConsumed(context, messageReference);
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        next.messageDelivered(context, messageReference);
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        next.messageDiscarded(context, sub, messageReference);
    }

    @Override
    public void slowConsumer(ConnectionContext context, Subscription subs) {
        next.slowConsumer(context, subs);
    }

    @Override
    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference node) {
        next.messageExpired(context, subs, node);
    }

    @Override
    public int getMaxBrowsePageSize() {
        return next.getMaxBrowsePageSize();
    }

    @Override
    public void setMaxBrowsePageSize(int maxPageSize) {
        next.setMaxBrowsePageSize(maxPageSize);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        next.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public int getCursorMemoryHighWaterMark() {
        return next.getCursorMemoryHighWaterMark();
    }

    @Override
    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        next.setCursorMemoryHighWaterMark(cursorMemoryHighWaterMark);
    }

    @Override
    public boolean isPrioritizedMessages() {
        return next.isPrioritizedMessages();
    }

    @Override
    public SlowConsumerStrategy getSlowConsumerStrategy() {
        return next.getSlowConsumerStrategy();
    }

    @Override
    public boolean isDoOptimzeMessageStorage() {
        return next.isDoOptimzeMessageStorage();
    }

    @Override
    public void setDoOptimzeMessageStorage(boolean doOptimzeMessageStorage) {
        next.setDoOptimzeMessageStorage(doOptimzeMessageStorage);
    }

    @Override
    public void clearPendingMessages(int pendingAdditionsCount) {
        next.clearPendingMessages(pendingAdditionsCount);
    }

    @Override
    public void duplicateFromStore(Message message, Subscription subscription) {
        next.duplicateFromStore(message, subscription);
    }

    @Override
    public boolean isSendDuplicateFromStoreToDLQ() {
        return next.isSendDuplicateFromStoreToDLQ();
    }

    @Override
    public void setSendDuplicateFromStoreToDLQ(boolean sendDuplicateFromStoreToDLQ) {
        next.setSendDuplicateFromStoreToDLQ(sendDuplicateFromStoreToDLQ);
    }

    public void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws Exception {
        if (next instanceof DestinationFilter) {
            DestinationFilter filter = (DestinationFilter) next;
            filter.deleteSubscription(context, key);
        } else if (next instanceof Topic) {
            Topic topic = (Topic)next;
            topic.deleteSubscription(context, key);
        }
    }

    public Destination getNext() {
        return next;
    }

    public <T> T getAdaptor(Class <? extends T> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        } else if (next != null && clazz.isInstance(next)) {
            return clazz.cast(next);
        } else if (next instanceof DestinationFilter) {
            return ((DestinationFilter)next).getAdaptor(clazz);
        }
        return null;
    }
}
