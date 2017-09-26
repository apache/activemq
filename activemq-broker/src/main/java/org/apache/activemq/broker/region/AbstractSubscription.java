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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NoLocalExpression;
import org.apache.activemq.selector.SelectorParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSubscription implements Subscription {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSubscription.class);

    protected Broker broker;
    protected ConnectionContext context;
    protected ConsumerInfo info;
    protected final DestinationFilter destinationFilter;
    protected final CopyOnWriteArrayList<Destination> destinations = new CopyOnWriteArrayList<Destination>();
    protected final AtomicInteger prefetchExtension = new AtomicInteger(0);

    private boolean usePrefetchExtension = true;
    private BooleanExpression selectorExpression;
    private ObjectName objectName;
    private int cursorMemoryHighWaterMark = 70;
    private boolean slowConsumer;
    private long lastAckTime;
    private final SubscriptionStatistics subscriptionStatistics = new SubscriptionStatistics();

    public AbstractSubscription(Broker broker,ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        this.broker = broker;
        this.context = context;
        this.info = info;
        this.destinationFilter = DestinationFilter.parseFilter(info.getDestination());
        this.selectorExpression = parseSelector(info);
        this.lastAckTime = System.currentTimeMillis();
    }

    private static BooleanExpression parseSelector(ConsumerInfo info) throws InvalidSelectorException {
        BooleanExpression rc = null;
        if (info.getSelector() != null) {
            rc = SelectorParser.parse(info.getSelector());
        }
        if (info.isNoLocal()) {
            if (rc == null) {
                rc = new NoLocalExpression(info.getConsumerId().getConnectionId());
            } else {
                rc = LogicExpression.createAND(new NoLocalExpression(info.getConsumerId().getConnectionId()), rc);
            }
        }
        if (info.getAdditionalPredicate() != null) {
            if (rc == null) {
                rc = info.getAdditionalPredicate();
            } else {
                rc = LogicExpression.createAND(info.getAdditionalPredicate(), rc);
            }
        }
        return rc;
    }

    @Override
    public synchronized void acknowledge(final ConnectionContext context, final MessageAck ack) throws Exception {
        this.lastAckTime = System.currentTimeMillis();
        subscriptionStatistics.getConsumedCount().increment();
    }

    @Override
    public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
        ConsumerId targetConsumerId = node.getTargetConsumerId();
        if (targetConsumerId != null) {
            if (!targetConsumerId.equals(info.getConsumerId())) {
                return false;
            }
        }
        try {
            return (selectorExpression == null || selectorExpression.matches(context)) && this.context.isAllowedToConsume(node);
        } catch (JMSException e) {
            LOG.info("Selector failed to evaluate: {}", e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean isWildcard() {
        return destinationFilter.isWildcard();
    }

    @Override
    public boolean matches(ActiveMQDestination destination) {
        return destinationFilter.matches(destination);
    }

    @Override
    public void add(ConnectionContext context, Destination destination) throws Exception {
        destinations.add(destination);
    }

    @Override
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        destinations.remove(destination);
        return Collections.EMPTY_LIST;
    }

    @Override
    public ConsumerInfo getConsumerInfo() {
        return info;
    }

    @Override
    public void gc() {
    }

    @Override
    public ConnectionContext getContext() {
        return context;
    }

    public ConsumerInfo getInfo() {
        return info;
    }

    public BooleanExpression getSelectorExpression() {
        return selectorExpression;
    }

    @Override
    public String getSelector() {
        return info.getSelector();
    }

    @Override
    public void setSelector(String selector) throws InvalidSelectorException {
        ConsumerInfo copy = info.copy();
        copy.setSelector(selector);
        BooleanExpression newSelector = parseSelector(copy);
        // its valid so lets actually update it now
        info.setSelector(selector);
        this.selectorExpression = newSelector;
    }

    @Override
    public ObjectName getObjectName() {
        return objectName;
    }

    @Override
    public void setObjectName(ObjectName objectName) {
        this.objectName = objectName;
    }

    @Override
    public int getPrefetchSize() {
        return info.getPrefetchSize();
    }

    public boolean isUsePrefetchExtension() {
        return usePrefetchExtension;
    }

    public void setUsePrefetchExtension(boolean usePrefetchExtension) {
        this.usePrefetchExtension = usePrefetchExtension;
    }

    public void setPrefetchSize(int newSize) {
        info.setPrefetchSize(newSize);
    }

    @Override
    public boolean isRecoveryRequired() {
        return true;
    }

    @Override
    public boolean isSlowConsumer() {
        return slowConsumer;
    }

    public void setSlowConsumer(boolean val) {
        slowConsumer = val;
    }

    @Override
    public boolean addRecoveredMessage(ConnectionContext context, MessageReference message) throws Exception {
        boolean result = false;
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            Destination regionDestination = (Destination) message.getRegionDestination();
            msgContext.setDestination(regionDestination.getActiveMQDestination());
            msgContext.setMessageReference(message);
            result = matches(message, msgContext);
            if (result) {
                doAddRecoveredMessage(message);
            }
        } finally {
            msgContext.clear();
        }
        return result;
    }

    @Override
    public ActiveMQDestination getActiveMQDestination() {
        return info != null ? info.getDestination() : null;
    }

    @Override
    public boolean isBrowser() {
        return info != null && info.isBrowser();
    }

    @Override
    public long getInFlightMessageSize() {
        return subscriptionStatistics.getInflightMessageSize().getTotalSize();
    }

    @Override
    public int getInFlightUsage() {
        int prefetchSize = info.getPrefetchSize();
        if (prefetchSize > 0) {
            return (getInFlightSize() * 100) / prefetchSize;
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Add a destination
     * @param destination
     */
    public void addDestination(Destination destination) {
    }

    /**
     * Remove a destination
     * @param destination
     */
    public void removeDestination(Destination destination) {
    }

    @Override
    public int getCursorMemoryHighWaterMark(){
        return this.cursorMemoryHighWaterMark;
    }

    @Override
    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark){
        this.cursorMemoryHighWaterMark=cursorMemoryHighWaterMark;
    }

    @Override
    public int countBeforeFull() {
        return info.getPrefetchSize() - getDispatchedQueueSize();
    }

    @Override
    public void unmatched(MessageReference node) throws IOException {
        // only durable topic subs have something to do here
    }

    protected void doAddRecoveredMessage(MessageReference message) throws Exception {
        add(message);
    }

    @Override
    public long getTimeOfLastMessageAck() {
        return lastAckTime;
    }

    public void setTimeOfLastMessageAck(long value) {
        this.lastAckTime = value;
    }

    @Override
    public long getConsumedCount(){
        return subscriptionStatistics.getConsumedCount().getCount();
    }

    @Override
    public void incrementConsumedCount(){
        subscriptionStatistics.getConsumedCount().increment();
    }

    @Override
    public void resetConsumedCount(){
        subscriptionStatistics.getConsumedCount().reset();
    }

    @Override
    public SubscriptionStatistics getSubscriptionStatistics() {
        return subscriptionStatistics;
    }

    public void wakeupDestinationsForDispatch() {
        for (Destination dest : destinations) {
            dest.wakeup();
        }
    }

    public AtomicInteger getPrefetchExtension() {
        return this.prefetchExtension;
    }

    protected void contractPrefetchExtension(int amount) {
        if (isUsePrefetchExtension() && getPrefetchSize() != 0) {
            decrementPrefetchExtension(amount);
        }
    }

    protected void expandPrefetchExtension(int amount) {
        if (isUsePrefetchExtension() && getPrefetchSize() != 0) {
            incrementPrefetchExtension(amount);
        }
    }

    protected void decrementPrefetchExtension(int amount) {
        while (true) {
            int currentExtension = prefetchExtension.get();
            int newExtension = Math.max(0, currentExtension - amount);
            if (prefetchExtension.compareAndSet(currentExtension, newExtension)) {
                break;
            }
        }
    }

    private void incrementPrefetchExtension(int amount) {
        while (true) {
            int currentExtension = prefetchExtension.get();
            int newExtension = Math.max(currentExtension, currentExtension + amount);
            if (prefetchExtension.compareAndSet(currentExtension, newExtension)) {
                break;
            }
        }
    }

}
