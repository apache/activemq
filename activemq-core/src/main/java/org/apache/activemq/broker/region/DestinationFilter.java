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
import org.apache.activemq.usage.Usage;

/**
 *
 *
 */
public class DestinationFilter implements Destination {

    private final Destination next;

    public DestinationFilter(Destination next) {
        this.next = next;
    }

    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node) throws IOException {
        next.acknowledge(context, sub, ack, node);
    }

    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        next.addSubscription(context, sub);
    }

    public Message[] browse() {
        return next.browse();
    }

    public void dispose(ConnectionContext context) throws IOException {
        next.dispose(context);
    }

    public boolean isDisposed() {
        return next.isDisposed();
    }

    public void gc() {
        next.gc();
    }

    public ActiveMQDestination getActiveMQDestination() {
        return next.getActiveMQDestination();
    }

    public DeadLetterStrategy getDeadLetterStrategy() {
        return next.getDeadLetterStrategy();
    }

    public DestinationStatistics getDestinationStatistics() {
        return next.getDestinationStatistics();
    }

    public String getName() {
        return next.getName();
    }

    public MemoryUsage getMemoryUsage() {
        return next.getMemoryUsage();
    }

    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception {
        next.removeSubscription(context, sub, lastDeliveredSequenceId);
    }

    public void send(ProducerBrokerExchange context, Message messageSend) throws Exception {
        next.send(context, messageSend);
    }

    public void start() throws Exception {
        next.start();
    }

    public void stop() throws Exception {
        next.stop();
    }

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

    public MessageStore getMessageStore() {
        return next.getMessageStore();
    }

    public boolean isProducerFlowControl() {
        return next.isProducerFlowControl();
    }

    public void setProducerFlowControl(boolean value) {
        next.setProducerFlowControl(value);
    }
    
    public boolean isAlwaysRetroactive() {
    	return next.isAlwaysRetroactive();
    }
    
    public void setAlwaysRetroactive(boolean value) {
    	next.setAlwaysRetroactive(value);
    }

    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        next.setBlockedProducerWarningInterval(blockedProducerWarningInterval);
    }

    public long getBlockedProducerWarningInterval() {
        return next.getBlockedProducerWarningInterval();
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.addProducer(context, info);

    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        next.removeProducer(context, info);
    }

    public int getMaxAuditDepth() {
        return next.getMaxAuditDepth();
    }

    public int getMaxProducersToAudit() {
        return next.getMaxProducersToAudit();
    }

    public boolean isEnableAudit() {
        return next.isEnableAudit();
    }

    public void setEnableAudit(boolean enableAudit) {
        next.setEnableAudit(enableAudit);
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
        next.setMaxAuditDepth(maxAuditDepth);
    }

    public void setMaxProducersToAudit(int maxProducersToAudit) {
        next.setMaxProducersToAudit(maxProducersToAudit);
    }

    public boolean isActive() {
        return next.isActive();
    }

    public int getMaxPageSize() {
        return next.getMaxPageSize();
    }

    public void setMaxPageSize(int maxPageSize) {
        next.setMaxPageSize(maxPageSize);
    }

    public boolean isUseCache() {
        return next.isUseCache();
    }

    public void setUseCache(boolean useCache) {
        next.setUseCache(useCache);
    }

    public int getMinimumMessageSize() {
        return next.getMinimumMessageSize();
    }

    public void setMinimumMessageSize(int minimumMessageSize) {
        next.setMinimumMessageSize(minimumMessageSize);
    }

    public void wakeup() {
        next.wakeup();
    }

    public boolean isLazyDispatch() {
        return next.isLazyDispatch();
    }

    public void setLazyDispatch(boolean value) {
        next.setLazyDispatch(value);
    }

    public void messageExpired(ConnectionContext context, PrefetchSubscription prefetchSubscription, MessageReference node) {
        next.messageExpired(context, prefetchSubscription, node);
    }

    public boolean iterate() {
        return next.iterate();
    }

    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo) {
        next.fastProducer(context, producerInfo);
    }

    public void isFull(ConnectionContext context, Usage usage) {
        next.isFull(context, usage);
    }

    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        next.messageConsumed(context, messageReference);
    }

    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        next.messageDelivered(context, messageReference);
    }

    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        next.messageDiscarded(context, sub, messageReference);
    }

    public void slowConsumer(ConnectionContext context, Subscription subs) {
        next.slowConsumer(context, subs);
    }

    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference node) {
        next.messageExpired(context, subs, node);
    }

    public int getMaxBrowsePageSize() {
        return next.getMaxBrowsePageSize();
    }

    public void setMaxBrowsePageSize(int maxPageSize) {
        next.setMaxBrowsePageSize(maxPageSize);
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        next.processDispatchNotification(messageDispatchNotification);
    }

    public int getCursorMemoryHighWaterMark() {
        return next.getCursorMemoryHighWaterMark();
    }

    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        next.setCursorMemoryHighWaterMark(cursorMemoryHighWaterMark);
    }

    public boolean isPrioritizedMessages() {
        return next.isPrioritizedMessages();
    }

    public SlowConsumerStrategy getSlowConsumerStrategy() {
        return next.getSlowConsumerStrategy();
    }
}
