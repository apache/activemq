/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.broker.region;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.filter.BooleanExpression;
import org.activemq.filter.DestinationFilter;
import org.activemq.filter.LogicExpression;
import org.activemq.filter.MessageEvaluationContext;
import org.activemq.filter.NoLocalExpression;
import org.activemq.selector.SelectorParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

abstract public class AbstractSubscription implements Subscription {
    
    protected final Log log;
    
    protected ConnectionContext context;
    protected ConsumerInfo info;
    final protected DestinationFilter destinationFilter;
    final protected BooleanExpression selector;
   
    final protected CopyOnWriteArrayList destinations = new CopyOnWriteArrayList();

    public AbstractSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {        
        this.context = context;
        this.info = info;
        this.destinationFilter = DestinationFilter.parseFilter(info.getDestination());
        this.selector = parseSelector(info);
        this.log = LogFactory.getLog(getClass().getName()+"."+info.getConsumerId());
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

    public boolean matches(MessageReference node, MessageEvaluationContext context) {
        ConsumerId targetConsumerId = node.getTargetConsumerId();
        if ( targetConsumerId!=null) {
            if( !targetConsumerId.equals(info.getConsumerId()) )
                return false;
        }
        try {
            return selector == null || selector.matches(context);
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
}
