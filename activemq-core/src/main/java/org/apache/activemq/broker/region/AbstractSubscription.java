/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

abstract public class AbstractSubscription implements Subscription {
    
    static private final Log log = LogFactory.getLog(AbstractSubscription.class);
    
    protected Broker broker;
    protected ConnectionContext context;
    protected ConsumerInfo info;
    final protected DestinationFilter destinationFilter;
    final protected BooleanExpression selector;
   
    final protected CopyOnWriteArrayList destinations = new CopyOnWriteArrayList();

    public AbstractSubscription(Broker broker,ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {        
        this.broker = broker;
        this.context = context;
        this.info = info;
        this.destinationFilter = DestinationFilter.parseFilter(info.getDestination());
        this.selector = parseSelector(info);
    }
    
    static private BooleanExpression parseSelector(ConsumerInfo info) throws InvalidSelectorException {
        BooleanExpression rc=null;
        if( info.getSelector() !=null ) {
            rc = new SelectorParser().parse(info.getSelector());
        }
        if( info.isNoLocal() ) {
            if( rc == null ) {
                rc = new NoLocalExpression(info.getConsumerId().getConnectionId());
            } else {
                rc = LogicExpression.createAND(new NoLocalExpression(info.getConsumerId().getConnectionId()), rc);
            }
        }
        if( info.getAdditionalPredicate() != null ) {
            if( rc == null ) {
                rc = info.getAdditionalPredicate();
            } else {
                rc = LogicExpression.createAND(info.getAdditionalPredicate(), rc);
            }
        }
        return rc;
    }

    public boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException {
        ConsumerId targetConsumerId = node.getTargetConsumerId();
        if ( targetConsumerId!=null) {
            if( !targetConsumerId.equals(info.getConsumerId()) )
                return false;
        }
        try {
            return (selector == null || selector.matches(context)) && this.context.isAllowedToConsume(node);
        } catch (JMSException e) {
            log.info("Selector failed to evaluate: " + e.getMessage(), e);
            return false;
        }
    }
    
    public boolean matches(ActiveMQDestination destination) {
        return destinationFilter.matches(destination);
    }

    public void add(ConnectionContext context, Destination destination) throws Throwable {
        destinations.add(destination);
    }

    public void remove(ConnectionContext context, Destination destination) throws Throwable {
        destinations.remove(destination);
    }
    
    public ConsumerInfo getConsumerInfo() {
        return info;
    }
    
    public void gc() {        
    }
    
    public boolean isSlaveBroker(){
        return broker.isSlaveBroker();
    }

    public ConnectionContext getContext() {
        return context;
    }

    public ConsumerInfo getInfo() {
        return info;
    }

    public BooleanExpression getSelector() {
        return selector;
    }
}
