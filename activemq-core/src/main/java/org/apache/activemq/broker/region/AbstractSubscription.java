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

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.management.ObjectName;

import java.io.IOException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NoLocalExpression;
import org.apache.activemq.selector.SelectorParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CopyOnWriteArrayList;

abstract public class AbstractSubscription implements Subscription {

    static private final Log log = LogFactory.getLog(AbstractSubscription.class);

    protected Broker broker;
    protected ConnectionContext context;
    protected ConsumerInfo info;
    final protected DestinationFilter destinationFilter;
    private BooleanExpression selectorExpression;
    private ObjectName objectName;

    final protected CopyOnWriteArrayList destinations = new CopyOnWriteArrayList();

    public AbstractSubscription(Broker broker, ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        this.broker = broker;
        this.context = context;
        this.info = info;
        this.destinationFilter = DestinationFilter.parseFilter(info.getDestination());
        this.selectorExpression = parseSelector(info);
    }

    static private BooleanExpression parseSelector(ConsumerInfo info) throws InvalidSelectorException {
        BooleanExpression rc = null;
        if (info.getSelector() != null) {
            rc = new SelectorParser().parse(info.getSelector());
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

    public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
        ConsumerId targetConsumerId = node.getTargetConsumerId();
        if (targetConsumerId != null) {
            if (!targetConsumerId.equals(info.getConsumerId()))
                return false;
        }
        try {
            return (selectorExpression == null || selectorExpression.matches(context)) && this.context.isAllowedToConsume(node);
        } catch (JMSException e) {
            log.info("Selector failed to evaluate: " + e.getMessage(), e);
            return false;
        }
    }

    public boolean matches(ActiveMQDestination destination) {
        return destinationFilter.matches(destination);
    }

    public void add(ConnectionContext context, Destination destination) throws Exception {
        destinations.add(destination);
    }

    public void remove(ConnectionContext context, Destination destination) throws Exception {
        destinations.remove(destination);
    }

    public ConsumerInfo getConsumerInfo() {
        return info;
    }

    public void gc() {
    }

    public boolean isSlave() {
        return getContext().isSlave();
    }

    public ConnectionContext getContext() {
        return context;
    }

    public ConsumerInfo getInfo() {
        return info;
    }

    public BooleanExpression getSelectorExpression() {
        return selectorExpression;
    }

    public String getSelector() {
        return info.getSelector();
    }

    public void setSelector(String selector) throws InvalidSelectorException {
        ConsumerInfo copy = info.copy();
        copy.setSelector(selector);
        BooleanExpression newSelector = parseSelector(copy);
        // its valid so lets actually update it now
        info.setSelector(selector);
        this.selectorExpression = newSelector;
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    public void setObjectName(ObjectName objectName) {
        this.objectName = objectName;
    }

    public int getPrefetchSize() {
        return info.getPrefetchSize();
    }

    public boolean isRecoveryRequired() {
        return true;
    }

    public boolean addRecoveredMessage(ConnectionContext context, MessageReference message) throws Exception {
        boolean result = false;
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            msgContext.setDestination(message.getRegionDestination().getActiveMQDestination());
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

    public ActiveMQDestination getActiveMQDestination() {
        return info != null ? info.getDestination() : null;
    }

    protected void doAddRecoveredMessage(MessageReference message) throws Exception {
        add(message);
    }
}
