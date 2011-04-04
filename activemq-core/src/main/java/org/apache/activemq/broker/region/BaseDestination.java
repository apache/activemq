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
import java.util.Collection;
import java.util.List;
import javax.jms.ResourceAllocationException;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.SlowConsumerStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;
import org.slf4j.Logger;

/**
 * 
 */
public abstract class BaseDestination implements Destination {
    /**
     * The maximum number of messages to page in to the destination from
     * persistent storage
     */
    public static final int MAX_PAGE_SIZE = 200;
    public static final int MAX_BROWSE_PAGE_SIZE = MAX_PAGE_SIZE * 2;
    public static final long EXPIRE_MESSAGE_PERIOD = 30 * 1000;
    public static final long DEFAULT_INACTIVE_TIMEOUT_BEFORE_GC = 60 * 1000;
    public static final int MAX_PRODUCERS_TO_AUDIT = 64;
    public static final int MAX_AUDIT_DEPTH = 2048;

    protected final ActiveMQDestination destination;
    protected final Broker broker;
    protected final MessageStore store;
    protected SystemUsage systemUsage;
    protected MemoryUsage memoryUsage;
    private boolean producerFlowControl = true;
    protected boolean warnOnProducerFlowControl = true;
    protected long blockedProducerWarningInterval = DEFAULT_BLOCKED_PRODUCER_WARNING_INTERVAL;

    private int maxProducersToAudit = 1024;
    private int maxAuditDepth = 2048;
    private boolean enableAudit = true;
    private int maxPageSize = MAX_PAGE_SIZE;
    private int maxBrowsePageSize = MAX_BROWSE_PAGE_SIZE;
    private boolean useCache = true;
    private int minimumMessageSize = 1024;
    private boolean lazyDispatch = false;
    private boolean advisoryForSlowConsumers;
    private boolean advisdoryForFastProducers;
    private boolean advisoryForDiscardingMessages;
    private boolean advisoryWhenFull;
    private boolean advisoryForDelivery;
    private boolean advisoryForConsumed;
    private boolean sendAdvisoryIfNoConsumers;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();
    protected final BrokerService brokerService;
    protected final Broker regionBroker;
    protected DeadLetterStrategy deadLetterStrategy = DEFAULT_DEAD_LETTER_STRATEGY;
    protected long expireMessagesPeriod = EXPIRE_MESSAGE_PERIOD;
    private int maxExpirePageSize = MAX_BROWSE_PAGE_SIZE;
    protected int cursorMemoryHighWaterMark = 70;
    protected int storeUsageHighWaterMark = 100;
    private SlowConsumerStrategy slowConsumerStrategy;
    private boolean prioritizedMessages;
    private long inactiveTimoutBeforeGC = DEFAULT_INACTIVE_TIMEOUT_BEFORE_GC;
    private boolean gcIfInactive;
    private boolean gcWithNetworkConsumers;
    private long lastActiveTime=0l;
    private boolean reduceMemoryFootprint = false;

    /**
     * @param brokerService
     * @param store
     * @param destination
     * @param parentStats
     * @throws Exception
     */
    public BaseDestination(BrokerService brokerService, MessageStore store, ActiveMQDestination destination, DestinationStatistics parentStats) throws Exception {
        this.brokerService = brokerService;
        this.broker = brokerService.getBroker();
        this.store = store;
        this.destination = destination;
        // let's copy the enabled property from the parent DestinationStatistics
        this.destinationStatistics.setEnabled(parentStats.isEnabled());
        this.destinationStatistics.setParent(parentStats);
        this.systemUsage = new SystemUsage(brokerService.getProducerSystemUsage(), destination.toString());
        this.memoryUsage = this.systemUsage.getMemoryUsage();
        this.memoryUsage.setUsagePortion(1.0f);
        this.regionBroker = brokerService.getRegionBroker();
    }

    /**
     * initialize the destination
     * 
     * @throws Exception
     */
    public void initialize() throws Exception {
        // Let the store know what usage manager we are using so that he can
        // flush messages to disk when usage gets high.
        if (store != null) {
            store.setMemoryUsage(this.memoryUsage);
        }
    }

    /**
     * @return the producerFlowControl
     */
    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }

    /**
     * @param producerFlowControl the producerFlowControl to set
     */
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }

    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     * 
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        this.blockedProducerWarningInterval = blockedProducerWarningInterval;
    }

    /**
     * 
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    public long getBlockedProducerWarningInterval() {
        return blockedProducerWarningInterval;
    }

    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }

    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }

    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().increment();
        this.lastActiveTime=0l;
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().decrement();
    }
    
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception{
        destinationStatistics.getConsumers().increment();
        this.lastActiveTime=0l;
    }

    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception{
        destinationStatistics.getConsumers().decrement();
    }


    public final MemoryUsage getMemoryUsage() {
        return memoryUsage;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public final String getName() {
        return getActiveMQDestination().getPhysicalName();
    }

    public final MessageStore getMessageStore() {
        return store;
    }

    public boolean isActive() {
        boolean isActive = destinationStatistics.getConsumers().getCount() != 0 ||
                           destinationStatistics.getProducers().getCount() != 0;
        if (isActive && isGcWithNetworkConsumers() && destinationStatistics.getConsumers().getCount() != 0) {
            isActive = hasRegularConsumers(getConsumers());
        }
        return isActive;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public int getMaxBrowsePageSize() {
        return this.maxBrowsePageSize;
    }

    public void setMaxBrowsePageSize(int maxPageSize) {
        this.maxBrowsePageSize = maxPageSize;
    }

    public int getMaxExpirePageSize() {
        return this.maxExpirePageSize;
    }

    public void setMaxExpirePageSize(int maxPageSize) {
        this.maxExpirePageSize = maxPageSize;
    }

    public void setExpireMessagesPeriod(long expireMessagesPeriod) {
        this.expireMessagesPeriod = expireMessagesPeriod;
    }

    public long getExpireMessagesPeriod() {
        return expireMessagesPeriod;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public int getMinimumMessageSize() {
        return minimumMessageSize;
    }

    public void setMinimumMessageSize(int minimumMessageSize) {
        this.minimumMessageSize = minimumMessageSize;
    }

    public boolean isLazyDispatch() {
        return lazyDispatch;
    }

    public void setLazyDispatch(boolean lazyDispatch) {
        this.lazyDispatch = lazyDispatch;
    }

    protected long getDestinationSequenceId() {
        return regionBroker.getBrokerSequenceId();
    }

    /**
     * @return the advisoryForSlowConsumers
     */
    public boolean isAdvisoryForSlowConsumers() {
        return advisoryForSlowConsumers;
    }

    /**
     * @param advisoryForSlowConsumers the advisoryForSlowConsumers to set
     */
    public void setAdvisoryForSlowConsumers(boolean advisoryForSlowConsumers) {
        this.advisoryForSlowConsumers = advisoryForSlowConsumers;
    }

    /**
     * @return the advisoryForDiscardingMessages
     */
    public boolean isAdvisoryForDiscardingMessages() {
        return advisoryForDiscardingMessages;
    }

    /**
     * @param advisoryForDiscardingMessages the advisoryForDiscardingMessages to
     *            set
     */
    public void setAdvisoryForDiscardingMessages(boolean advisoryForDiscardingMessages) {
        this.advisoryForDiscardingMessages = advisoryForDiscardingMessages;
    }

    /**
     * @return the advisoryWhenFull
     */
    public boolean isAdvisoryWhenFull() {
        return advisoryWhenFull;
    }

    /**
     * @param advisoryWhenFull the advisoryWhenFull to set
     */
    public void setAdvisoryWhenFull(boolean advisoryWhenFull) {
        this.advisoryWhenFull = advisoryWhenFull;
    }

    /**
     * @return the advisoryForDelivery
     */
    public boolean isAdvisoryForDelivery() {
        return advisoryForDelivery;
    }

    /**
     * @param advisoryForDelivery the advisoryForDelivery to set
     */
    public void setAdvisoryForDelivery(boolean advisoryForDelivery) {
        this.advisoryForDelivery = advisoryForDelivery;
    }

    /**
     * @return the advisoryForConsumed
     */
    public boolean isAdvisoryForConsumed() {
        return advisoryForConsumed;
    }

    /**
     * @param advisoryForConsumed the advisoryForConsumed to set
     */
    public void setAdvisoryForConsumed(boolean advisoryForConsumed) {
        this.advisoryForConsumed = advisoryForConsumed;
    }

    /**
     * @return the advisdoryForFastProducers
     */
    public boolean isAdvisdoryForFastProducers() {
        return advisdoryForFastProducers;
    }

    /**
     * @param advisdoryForFastProducers the advisdoryForFastProducers to set
     */
    public void setAdvisdoryForFastProducers(boolean advisdoryForFastProducers) {
        this.advisdoryForFastProducers = advisdoryForFastProducers;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }

    /**
     * @return the dead letter strategy
     */
    public DeadLetterStrategy getDeadLetterStrategy() {
        return deadLetterStrategy;
    }

    /**
     * set the dead letter strategy
     * 
     * @param deadLetterStrategy
     */
    public void setDeadLetterStrategy(DeadLetterStrategy deadLetterStrategy) {
        this.deadLetterStrategy = deadLetterStrategy;
    }

    public int getCursorMemoryHighWaterMark() {
        return this.cursorMemoryHighWaterMark;
    }

    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        this.cursorMemoryHighWaterMark = cursorMemoryHighWaterMark;
    }

    /**
     * called when message is consumed
     * 
     * @param context
     * @param messageReference
     */
    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        if (advisoryForConsumed) {
            broker.messageConsumed(context, messageReference);
        }
    }

    /**
     * Called when message is delivered to the broker
     * 
     * @param context
     * @param messageReference
     */
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        if (advisoryForDelivery) {
            broker.messageDelivered(context, messageReference);
        }
    }

    /**
     * Called when a message is discarded - e.g. running low on memory This will
     * happen only if the policy is enabled - e.g. non durable topics
     * 
     * @param context
     * @param messageReference
     */
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        if (advisoryForDiscardingMessages) {
            broker.messageDiscarded(context, sub, messageReference);
        }
    }

    /**
     * Called when there is a slow consumer
     * 
     * @param context
     * @param subs
     */
    public void slowConsumer(ConnectionContext context, Subscription subs) {
        if (advisoryForSlowConsumers) {
            broker.slowConsumer(context, this, subs);
        }
        if (slowConsumerStrategy != null) {
            slowConsumerStrategy.slowConsumer(context, subs);
        }
    }

    /**
     * Called to notify a producer is too fast
     * 
     * @param context
     * @param producerInfo
     */
    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo) {
        if (advisdoryForFastProducers) {
            broker.fastProducer(context, producerInfo);
        }
    }

    /**
     * Called when a Usage reaches a limit
     * 
     * @param context
     * @param usage
     */
    public void isFull(ConnectionContext context, Usage usage) {
        if (advisoryWhenFull) {
            broker.isFull(context, this, usage);
        }
    }

    public void dispose(ConnectionContext context) throws IOException {
        if (this.store != null) {
            this.store.removeAllMessages(context);
            this.store.dispose(context);
        }
        this.destinationStatistics.setParent(null);
        this.memoryUsage.stop();
    }

    /**
     * Provides a hook to allow messages with no consumer to be processed in
     * some way - such as to send to a dead letter queue or something..
     */
    protected void onMessageWithNoConsumers(ConnectionContext context, Message msg) throws Exception {
        if (!msg.isPersistent()) {
            if (isSendAdvisoryIfNoConsumers()) {
                // allow messages with no consumers to be dispatched to a dead
                // letter queue
                if (destination.isQueue() || !AdvisorySupport.isAdvisoryTopic(destination)) {

                    Message message = msg.copy();
                    // The original destination and transaction id do not get
                    // filled when the message is first sent,
                    // it is only populated if the message is routed to another
                    // destination like the DLQ
                    if (message.getOriginalDestination() != null) {
                        message.setOriginalDestination(message.getDestination());
                    }
                    if (message.getOriginalTransactionId() != null) {
                        message.setOriginalTransactionId(message.getTransactionId());
                    }

                    ActiveMQTopic advisoryTopic;
                    if (destination.isQueue()) {
                        advisoryTopic = AdvisorySupport.getNoQueueConsumersAdvisoryTopic(destination);
                    } else {
                        advisoryTopic = AdvisorySupport.getNoTopicConsumersAdvisoryTopic(destination);
                    }
                    message.setDestination(advisoryTopic);
                    message.setTransactionId(null);

                    // Disable flow control for this since since we don't want
                    // to block.
                    boolean originalFlowControl = context.isProducerFlowControl();
                    try {
                        context.setProducerFlowControl(false);
                        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
                        producerExchange.setMutable(false);
                        producerExchange.setConnectionContext(context);
                        producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
                        context.getBroker().send(producerExchange, message);
                    } finally {
                        context.setProducerFlowControl(originalFlowControl);
                    }

                }
            }
        }
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
    }

    public final int getStoreUsageHighWaterMark() {
        return this.storeUsageHighWaterMark;
    }

    public void setStoreUsageHighWaterMark(int storeUsageHighWaterMark) {
        this.storeUsageHighWaterMark = storeUsageHighWaterMark;
    }

    protected final void waitForSpace(ConnectionContext context, Usage<?> usage, String warning) throws IOException, InterruptedException, ResourceAllocationException {
        waitForSpace(context, usage, 100, warning);
    }
    
    protected final void waitForSpace(ConnectionContext context, Usage<?> usage, int highWaterMark, String warning) throws IOException, InterruptedException, ResourceAllocationException {
        if (systemUsage.isSendFailIfNoSpace()) {
            getLog().debug("sendFailIfNoSpace, forcing exception on send: " + warning);
            throw new ResourceAllocationException(warning);
        }
        if (systemUsage.getSendFailIfNoSpaceAfterTimeout() != 0) {
            if (!usage.waitForSpace(systemUsage.getSendFailIfNoSpaceAfterTimeout(), highWaterMark)) {
                getLog().debug("sendFailIfNoSpaceAfterTimeout expired, forcing exception on send: " + warning);
                throw new ResourceAllocationException(warning);
            }
        } else {
            long start = System.currentTimeMillis();
            long nextWarn = start;
            while (!usage.waitForSpace(1000, highWaterMark)) {
                if (context.getStopping().get()) {
                    throw new IOException("Connection closed, send aborted.");
                }
    
                long now = System.currentTimeMillis();
                if (now >= nextWarn) {
                    getLog().info(warning + " (blocking for: " + (now - start) / 1000 + "s)");
                    nextWarn = now + blockedProducerWarningInterval;
                }
            }
        }
    }

    protected abstract Logger getLog();

    public void setSlowConsumerStrategy(SlowConsumerStrategy slowConsumerStrategy) {
        this.slowConsumerStrategy = slowConsumerStrategy;
    }

    public SlowConsumerStrategy getSlowConsumerStrategy() {
        return this.slowConsumerStrategy;
    }

   
    public boolean isPrioritizedMessages() {
        return this.prioritizedMessages;
    }

    public void setPrioritizedMessages(boolean prioritizedMessages) {
        this.prioritizedMessages = prioritizedMessages;
        if (store != null) {
            store.setPrioritizedMessages(prioritizedMessages);
        }
    }

    /**
     * @return the inactiveTimoutBeforeGC
     */
    public long getInactiveTimoutBeforeGC() {
        return this.inactiveTimoutBeforeGC;
    }

    /**
     * @param inactiveTimoutBeforeGC the inactiveTimoutBeforeGC to set
     */
    public void setInactiveTimoutBeforeGC(long inactiveTimoutBeforeGC) {
        this.inactiveTimoutBeforeGC = inactiveTimoutBeforeGC;
    }

    /**
     * @return the gcIfInactive
     */
    public boolean isGcIfInactive() {
        return this.gcIfInactive;
    }

    /**
     * @param gcIfInactive the gcIfInactive to set
     */
    public void setGcIfInactive(boolean gcIfInactive) {
        this.gcIfInactive = gcIfInactive;
    }

    /**
     * Indicate if it is ok to gc destinations that have only network consumers
     * @param gcWithNetworkConsumers
     */
    public void setGcWithNetworkConsumers(boolean gcWithNetworkConsumers) {
        this.gcWithNetworkConsumers = gcWithNetworkConsumers;
    }

    public boolean isGcWithNetworkConsumers() {
        return gcWithNetworkConsumers;
    }

    public void markForGC(long timeStamp) {
        if (isGcIfInactive() && this.lastActiveTime == 0 && isActive() == false
                && destinationStatistics.messages.getCount() == 0 && getInactiveTimoutBeforeGC() > 0l) {
            this.lastActiveTime = timeStamp;
        }
    }

    public boolean canGC() {
        boolean result = false;
        if (isGcIfInactive()&& this.lastActiveTime != 0l) {
            if ((System.currentTimeMillis() - this.lastActiveTime) >= getInactiveTimoutBeforeGC()) {
                result = true;
            }
        }
        return result;
    }

    public void setReduceMemoryFootprint(boolean reduceMemoryFootprint) {
        this.reduceMemoryFootprint = reduceMemoryFootprint;
    }

    protected boolean isReduceMemoryFootprint() {
        return this.reduceMemoryFootprint;
    }

    public abstract List<Subscription> getConsumers();

    protected boolean hasRegularConsumers(List<Subscription> consumers) {
        boolean hasRegularConsumers = false;
        for (Subscription subscription: consumers) {
            if (!subscription.getConsumerInfo().isNetworkSubscription()) {
                hasRegularConsumers = true;
                break;
            }
        }
        return hasRegularConsumers;
    }
}
