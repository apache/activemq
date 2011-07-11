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
import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.SlowConsumerStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.Task;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.Usage;

/**
 *
 */
public interface Destination extends Service, Task {

    public static final DeadLetterStrategy DEFAULT_DEAD_LETTER_STRATEGY = new SharedDeadLetterStrategy();
    public static final long DEFAULT_BLOCKED_PRODUCER_WARNING_INTERVAL = 30000;

    void addSubscription(ConnectionContext context, Subscription sub) throws Exception;

    void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception;

    void addProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception;

    void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception;

    void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException;

    void gc();

    ActiveMQDestination getActiveMQDestination();

    MemoryUsage getMemoryUsage();

    void dispose(ConnectionContext context) throws IOException;

    boolean isDisposed();

    DestinationStatistics getDestinationStatistics();

    DeadLetterStrategy getDeadLetterStrategy();

    Message[] browse();

    String getName();

    MessageStore getMessageStore();

    boolean isProducerFlowControl();

    void setProducerFlowControl(boolean value);
    
    boolean isAlwaysRetroactive();
    
    void setAlwaysRetroactive(boolean value);

    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     *
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval);

    /**
     *
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    public long getBlockedProducerWarningInterval();

    int getMaxProducersToAudit();

    void setMaxProducersToAudit(int maxProducersToAudit);

    int getMaxAuditDepth();

    void setMaxAuditDepth(int maxAuditDepth);

    boolean isEnableAudit();

    void setEnableAudit(boolean enableAudit);

    boolean isActive();

    int getMaxPageSize();

    public void setMaxPageSize(int maxPageSize);

    public int getMaxBrowsePageSize();

    public void setMaxBrowsePageSize(int maxPageSize);

    public boolean isUseCache();

    public void setUseCache(boolean useCache);

    public int getMinimumMessageSize();

    public void setMinimumMessageSize(int minimumMessageSize);

    public int getCursorMemoryHighWaterMark();

    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark);

    /**
     * optionally called by a Subscriber - to inform the Destination its ready
     * for more messages
     */
    public void wakeup();

    /**
     * @return true if lazyDispatch is enabled
     */
    public boolean isLazyDispatch();

    /**
     * set the lazy dispatch - default is false
     *
     * @param value
     */
    public void setLazyDispatch(boolean value);

    /**
     * Inform the Destination a message has expired
     *
     * @param context
     * @param subs
     * @param node
     */
    void messageExpired(ConnectionContext context, Subscription subs, MessageReference node);

    /**
     * called when message is consumed
     *
     * @param context
     * @param messageReference
     */
    void messageConsumed(ConnectionContext context, MessageReference messageReference);

    /**
     * Called when message is delivered to the broker
     *
     * @param context
     * @param messageReference
     */
    void messageDelivered(ConnectionContext context, MessageReference messageReference);

    /**
     * Called when a message is discarded - e.g. running low on memory This will
     * happen only if the policy is enabled - e.g. non durable topics
     *
     * @param context
     * @param messageReference
     * @param sub
     */
    void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference);

    /**
     * Called when there is a slow consumer
     *
     * @param context
     * @param subs
     */
    void slowConsumer(ConnectionContext context, Subscription subs);

    /**
     * Called to notify a producer is too fast
     *
     * @param context
     * @param producerInfo
     */
    void fastProducer(ConnectionContext context, ProducerInfo producerInfo);

    /**
     * Called when a Usage reaches a limit
     *
     * @param context
     * @param usage
     */
    void isFull(ConnectionContext context, Usage usage);

    List<Subscription> getConsumers();

    /**
     * called on Queues in slave mode to allow dispatch to follow subscription
     * choice of master
     *
     * @param messageDispatchNotification
     * @throws Exception
     */
    void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception;

    boolean isPrioritizedMessages();

    SlowConsumerStrategy getSlowConsumerStrategy();
}
