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

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
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

/**
 * @version $Revision: 1.12 $
 */
public abstract class BaseDestination implements Destination {
    /**
     * The maximum number of messages to page in to the destination from persistent storage
     */
    public static final int MAX_PAGE_SIZE = 200;
    public static final int MAX_BROWSE_PAGE_SIZE = MAX_PAGE_SIZE * 2;
    protected final ActiveMQDestination destination;
    protected final Broker broker;
    protected final MessageStore store;
    protected SystemUsage systemUsage;
    protected MemoryUsage memoryUsage;
    private boolean producerFlowControl = true;
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

    /**
     * @param broker
     * @param store
     * @param destination
     * @param parentStats
     * @throws Exception
     */
    public BaseDestination(BrokerService brokerService, MessageStore store, ActiveMQDestination destination,
            DestinationStatistics parentStats) throws Exception {
        this.brokerService = brokerService;
        this.broker = brokerService.getBroker();
        this.store = store;
        this.destination = destination;
        // let's copy the enabled property from the parent DestinationStatistics
        this.destinationStatistics.setEnabled(parentStats.isEnabled());
        this.destinationStatistics.setParent(parentStats);
        this.systemUsage = brokerService.getProducerSystemUsage();
        this.memoryUsage = new MemoryUsage(systemUsage.getMemoryUsage(), destination.toString());
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
     * @param producerFlowControl
     *            the producerFlowControl to set
     */
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }

    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit
     *            the maxProducersToAudit to set
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
     * @param maxAuditDepth
     *            the maxAuditDepth to set
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
     * @param enableAudit
     *            the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().increment();
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        destinationStatistics.getProducers().decrement();
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

    public final boolean isActive() {
        return destinationStatistics.getConsumers().getCount() != 0
                || destinationStatistics.getProducers().getCount() != 0;
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
     * @param advisoryForSlowConsumers
     *            the advisoryForSlowConsumers to set
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
     * @param advisoryForDiscardingMessages
     *            the advisoryForDiscardingMessages to set
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
     * @param advisoryWhenFull
     *            the advisoryWhenFull to set
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
     * @param advisoryForDelivery
     *            the advisoryForDelivery to set
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
     * @param advisoryForConsumed
     *            the advisoryForConsumed to set
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
     * @param advisdoryForFastProducers
     *            the advisdoryForFastProducers to set
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
     * Called when a message is discarded - e.g. running low on memory This will happen only if the policy is enabled -
     * e.g. non durable topics
     * 
     * @param context
     * @param messageReference
     */
    public void messageDiscarded(ConnectionContext context, MessageReference messageReference) {
        if (advisoryForDiscardingMessages) {
            broker.messageDiscarded(context, messageReference);
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
    protected void onMessageWithNoConsumers(ConnectionContext context, Message message) throws Exception { 	
    	if (!message.isPersistent()) {
            if (isSendAdvisoryIfNoConsumers()) {
                // allow messages with no consumers to be dispatched to a dead
                // letter queue
                if (destination.isQueue() || !AdvisorySupport.isAdvisoryTopic(destination)) {

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
    
    public void processDispatchNotification(
            MessageDispatchNotification messageDispatchNotification) throws Exception {
    }

}
