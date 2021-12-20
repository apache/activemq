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
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
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
    public static final int MAX_AUDIT_DEPTH = 10000;
    public static final String DUPLICATE_FROM_STORE_MSG_PREFIX = "duplicate from store for ";

    protected final AtomicBoolean started = new AtomicBoolean();
    protected final ActiveMQDestination destination;
    protected final Broker broker;
    protected final MessageStore store;
    protected SystemUsage systemUsage;
    protected MemoryUsage memoryUsage;
    private boolean producerFlowControl = true;
    private boolean alwaysRetroactive = false;
    protected long lastBlockedProducerWarnTime = 0l;
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
    private boolean advisoryForFastProducers;
    private boolean advisoryForDiscardingMessages;
    private boolean advisoryWhenFull;
    private boolean advisoryForDelivery;
    private boolean advisoryForConsumed;
    private boolean sendAdvisoryIfNoConsumers;
    private boolean sendDuplicateFromStoreToDLQ = true;
    private boolean includeBodyForAdvisory;
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
    private long inactiveTimeoutBeforeGC = DEFAULT_INACTIVE_TIMEOUT_BEFORE_GC;
    private boolean gcIfInactive;
    private boolean gcWithNetworkConsumers;
    private long lastActiveTime=0l;
    private boolean reduceMemoryFootprint = false;
    protected final Scheduler scheduler;
    private boolean disposed = false;
    private boolean doOptimzeMessageStorage = true;
    /*
     * percentage of in-flight messages above which optimize message store is disabled
     */
    private int optimizeMessageStoreInFlightLimit = 10;
    private boolean persistJMSRedelivered;

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
        this.scheduler = brokerService.getBroker().getScheduler();
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
    @Override
    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }

    /**
     * @param producerFlowControl the producerFlowControl to set
     */
    @Override
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }

    @Override
    public boolean isAlwaysRetroactive() {
        return alwaysRetroactive;
    }

    @Override
    public void setAlwaysRetroactive(boolean alwaysRetroactive) {
        this.alwaysRetroactive = alwaysRetroactive;
    }

    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     *
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    @Override
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        this.blockedProducerWarningInterval = blockedProducerWarningInterval;
    }

    /**
     *
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    @Override
    public long getBlockedProducerWarningInterval() {
        return blockedProducerWarningInterval;
    }

    /**
     * @return the maxProducersToAudit
     */
    @Override
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }

    /**
     * @return the maxAuditDepth
     */
    @Override
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }

    /**
     * @return the enableAudit
     */
    @Override
    public boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    @Override
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().increment();
        this.lastActiveTime=0l;
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().decrement();
    }

    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception{
        destinationStatistics.getConsumers().increment();
        this.lastActiveTime=0l;
    }

    @Override
    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception{
        destinationStatistics.getConsumers().decrement();
        this.lastActiveTime=0l;
    }


    @Override
    public final MemoryUsage getMemoryUsage() {
        return memoryUsage;
    }

    @Override
    public void setMemoryUsage(MemoryUsage memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    @Override
    public TempUsage getTempUsage() {
        return systemUsage.getTempUsage();
    }

    @Override
    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    @Override
    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    @Override
    public final String getName() {
        return getActiveMQDestination().getPhysicalName();
    }

    @Override
    public final MessageStore getMessageStore() {
        return store;
    }

    @Override
    public boolean isActive() {
        boolean isActive = destinationStatistics.getConsumers().getCount() > 0 ||
                           destinationStatistics.getProducers().getCount() > 0;
        if (isActive && isGcWithNetworkConsumers() && destinationStatistics.getConsumers().getCount() > 0) {
            isActive = hasRegularConsumers(getConsumers());
        }
        return isActive;
    }

    @Override
    public int getMaxPageSize() {
        return maxPageSize;
    }

    @Override
    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    @Override
    public int getMaxBrowsePageSize() {
        return this.maxBrowsePageSize;
    }

    @Override
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

    @Override
    public boolean isUseCache() {
        return useCache;
    }

    @Override
    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    @Override
    public int getMinimumMessageSize() {
        return minimumMessageSize;
    }

    @Override
    public void setMinimumMessageSize(int minimumMessageSize) {
        this.minimumMessageSize = minimumMessageSize;
    }

    @Override
    public boolean isLazyDispatch() {
        return lazyDispatch;
    }

    @Override
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
    public boolean isAdvisoryForFastProducers() {
        return advisoryForFastProducers;
    }

    /**
     * @param advisoryForFastProducers the advisdoryForFastProducers to set
     */
    public void setAdvisoryForFastProducers(boolean advisoryForFastProducers) {
        this.advisoryForFastProducers = advisoryForFastProducers;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }

    public boolean isSendDuplicateFromStoreToDLQ() {
        return this.sendDuplicateFromStoreToDLQ;
    }

    public void setSendDuplicateFromStoreToDLQ(boolean sendDuplicateFromStoreToDLQ) {
        this.sendDuplicateFromStoreToDLQ = sendDuplicateFromStoreToDLQ;
    }

    public boolean isIncludeBodyForAdvisory() {
        return includeBodyForAdvisory;
    }

    public void setIncludeBodyForAdvisory(boolean includeBodyForAdvisory) {
        this.includeBodyForAdvisory = includeBodyForAdvisory;
    }

    /**
     * @return the dead letter strategy
     */
    @Override
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

    @Override
    public int getCursorMemoryHighWaterMark() {
        return this.cursorMemoryHighWaterMark;
    }

    @Override
    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        this.cursorMemoryHighWaterMark = cursorMemoryHighWaterMark;
    }

    /**
     * called when message is consumed
     *
     * @param context
     * @param messageReference
     */
    @Override
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
    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        this.lastActiveTime = 0L;
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
    @Override
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
    @Override
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
    @Override
    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo) {
        if (advisoryForFastProducers) {
            broker.fastProducer(context, producerInfo, getActiveMQDestination());
        }
    }

    /**
     * Called when a Usage reaches a limit
     *
     * @param context
     * @param usage
     */
    @Override
    public void isFull(ConnectionContext context, Usage<?> usage) {
        if (advisoryWhenFull) {
            broker.isFull(context, this, usage);
        }
    }

    @Override
    public void dispose(ConnectionContext context) throws IOException {
        if (this.store != null) {
            this.store.removeAllMessages(context);
            this.store.dispose(context);
        }
        this.destinationStatistics.setParent(null);
        this.memoryUsage.stop();
        this.disposed = true;
    }

    @Override
    public boolean isDisposed() {
        return this.disposed;
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

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
    }

    public final int getStoreUsageHighWaterMark() {
        return this.storeUsageHighWaterMark;
    }

    public void setStoreUsageHighWaterMark(int storeUsageHighWaterMark) {
        this.storeUsageHighWaterMark = storeUsageHighWaterMark;
    }

    protected final void waitForSpace(ConnectionContext context,ProducerBrokerExchange producerBrokerExchange, Usage<?> usage, String warning) throws IOException, InterruptedException, ResourceAllocationException {
        waitForSpace(context, producerBrokerExchange, usage, 100, warning);
    }

    protected final void waitForSpace(ConnectionContext context, ProducerBrokerExchange producerBrokerExchange, Usage<?> usage, int highWaterMark, String warning) throws IOException, InterruptedException, ResourceAllocationException {
        if (!context.isNetworkConnection() && systemUsage.isSendFailIfNoSpace()) {
            if (isFlowControlLogRequired()) {
                getLog().info("sendFailIfNoSpace, forcing exception on send, usage: {}: {}", usage, warning);
            } else {
                getLog().debug("sendFailIfNoSpace, forcing exception on send, usage: {}: {}", usage, warning);
            }
            throw new ResourceAllocationException(warning);
        }
        if (!context.isNetworkConnection() && systemUsage.getSendFailIfNoSpaceAfterTimeout() != 0) {
            if (!usage.waitForSpace(systemUsage.getSendFailIfNoSpaceAfterTimeout(), highWaterMark)) {
                if (isFlowControlLogRequired()) {
                    getLog().info("sendFailIfNoSpaceAfterTimeout expired, forcing exception on send, usage: {}: {}", usage, warning);
                } else {
                    getLog().debug("sendFailIfNoSpaceAfterTimeout expired, forcing exception on send, usage: {}: {}", usage, warning);
                }
                throw new ResourceAllocationException(warning);
            }
        } else {
            long start = System.currentTimeMillis();
            producerBrokerExchange.blockingOnFlowControl(true);
            destinationStatistics.getBlockedSends().increment();
            while (!usage.waitForSpace(1000, highWaterMark)) {
                if (context.getStopping().get()) {
                    throw new IOException("Connection closed, send aborted.");
                }

                if (isFlowControlLogRequired()) {
                    getLog().warn("{}: {} (blocking for: {}s)", new Object[]{ usage, warning, new Long(((System.currentTimeMillis() - start) / 1000))});
                } else {
                    getLog().debug("{}: {} (blocking for: {}s)", new Object[]{ usage, warning, new Long(((System.currentTimeMillis() - start) / 1000))});
                }
            }
            long finish = System.currentTimeMillis();
            long totalTimeBlocked = finish - start;
            destinationStatistics.getBlockedTime().addTime(totalTimeBlocked);
            producerBrokerExchange.incrementTimeBlocked(this,totalTimeBlocked);
            producerBrokerExchange.blockingOnFlowControl(false);
        }
    }

    protected boolean isFlowControlLogRequired() {
        boolean answer = false;
        if (blockedProducerWarningInterval > 0) {
            long now = System.currentTimeMillis();
            if (lastBlockedProducerWarnTime + blockedProducerWarningInterval <= now) {
                lastBlockedProducerWarnTime = now;
                answer = true;
            }
        }
        return answer;
    }

    protected abstract Logger getLog();

    public void setSlowConsumerStrategy(SlowConsumerStrategy slowConsumerStrategy) {
        this.slowConsumerStrategy = slowConsumerStrategy;
    }

    @Override
    public SlowConsumerStrategy getSlowConsumerStrategy() {
        return this.slowConsumerStrategy;
    }


    @Override
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
     * @return the inactiveTimeoutBeforeGC
     */
    @Override
    public long getInactiveTimeoutBeforeGC() {
        return this.inactiveTimeoutBeforeGC;
    }

    /**
     * @param inactiveTimeoutBeforeGC the inactiveTimeoutBeforeGC to set
     */
    public void setInactiveTimeoutBeforeGC(long inactiveTimeoutBeforeGC) {
        this.inactiveTimeoutBeforeGC = inactiveTimeoutBeforeGC;
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

    @Override
    public void markForGC(long timeStamp) {
        if (isGcIfInactive() && this.lastActiveTime == 0 && isActive() == false
                && destinationStatistics.messages.getCount() == 0 && getInactiveTimeoutBeforeGC() > 0l) {
            this.lastActiveTime = timeStamp;
        }
    }

    @Override
    public boolean canGC() {
        boolean result = false;
        final long currentLastActiveTime = this.lastActiveTime;
        if (isGcIfInactive() && currentLastActiveTime != 0l && destinationStatistics.messages.getCount() == 0L ) {
            if ((System.currentTimeMillis() - currentLastActiveTime) >= getInactiveTimeoutBeforeGC()) {
                result = true;
            }
        }
        return result;
    }

    public void setReduceMemoryFootprint(boolean reduceMemoryFootprint) {
        this.reduceMemoryFootprint = reduceMemoryFootprint;
    }

    public boolean isReduceMemoryFootprint() {
        return this.reduceMemoryFootprint;
    }

    @Override
    public boolean isDoOptimzeMessageStorage() {
        return doOptimzeMessageStorage;
    }

    @Override
    public void setDoOptimzeMessageStorage(boolean doOptimzeMessageStorage) {
        this.doOptimzeMessageStorage = doOptimzeMessageStorage;
    }

    public int getOptimizeMessageStoreInFlightLimit() {
        return optimizeMessageStoreInFlightLimit;
    }

    public void setOptimizeMessageStoreInFlightLimit(int optimizeMessageStoreInFlightLimit) {
        this.optimizeMessageStoreInFlightLimit = optimizeMessageStoreInFlightLimit;
    }


    @Override
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

    public ConnectionContext createConnectionContext() {
        ConnectionContext answer = new ConnectionContext();
        answer.setBroker(this.broker);
        answer.getMessageEvaluationContext().setDestination(getActiveMQDestination());
        answer.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        return answer;
    }

    protected MessageAck convertToNonRangedAck(MessageAck ack, MessageReference node) {
        // the original ack may be a ranged ack, but we are trying to delete
        // a specific
        // message store here so we need to convert to a non ranged ack.
        if (ack.getMessageCount() > 0) {
            // Dup the ack
            MessageAck a = new MessageAck();
            ack.copy(a);
            ack = a;
            // Convert to non-ranged.
            ack.setMessageCount(1);
        }
        // always use node messageId so we can access entry/data Location
        ack.setFirstMessageId(node.getMessageId());
        ack.setLastMessageId(node.getMessageId());
        return ack;
    }

    protected boolean isDLQ() {
        return destination.isDLQ();
    }

    @Override
    public void duplicateFromStore(Message message, Subscription subscription) {
        destinationStatistics.getDuplicateFromStore().increment();
        ConnectionContext connectionContext = createConnectionContext();
        getLog().warn("{}{}, redirecting {} for dlq processing", DUPLICATE_FROM_STORE_MSG_PREFIX, destination, message.getMessageId());
        Throwable cause = new Throwable(DUPLICATE_FROM_STORE_MSG_PREFIX + destination);
        message.setRegionDestination(this);
        if(this.isSendDuplicateFromStoreToDLQ()) {
            broker.getRoot().sendToDeadLetterQueue(connectionContext, message, null, cause);
        }
        MessageAck messageAck = new MessageAck(message, MessageAck.POISON_ACK_TYPE, 1);
        messageAck.setPoisonCause(cause);
        try {
            acknowledge(connectionContext, subscription, messageAck, message);
        } catch (IOException e) {
            getLog().error("Failed to acknowledge duplicate message {} from {} with {}", message.getMessageId(), destination, messageAck);
        }
    }

    public void setPersistJMSRedelivered(boolean persistJMSRedelivered) {
        this.persistJMSRedelivered = persistJMSRedelivered;
    }

    public boolean isPersistJMSRedelivered() {
        return persistJMSRedelivered;
    }

    public SystemUsage getSystemUsage() {
        return systemUsage;
    }
}
