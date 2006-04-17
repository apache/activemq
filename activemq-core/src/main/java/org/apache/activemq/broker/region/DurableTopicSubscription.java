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

import java.io.IOException;
import java.util.Iterator;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.util.SubscriptionKey;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

public class DurableTopicSubscription extends PrefetchSubscription {
    
    private final ConcurrentHashMap redeliveredMessages = new ConcurrentHashMap();
    private final ConcurrentHashMap destinations = new ConcurrentHashMap();
    private final SubscriptionKey subscriptionKey;
    private final boolean keepDurableSubsActive;
    private boolean active=false;
    
    public DurableTopicSubscription(Broker broker,ConnectionContext context, ConsumerInfo info, boolean keepDurableSubsActive) throws InvalidSelectorException {
        super(broker,context, info);
        this.keepDurableSubsActive = keepDurableSubsActive;
        subscriptionKey = new SubscriptionKey(context.getClientId(), info.getSubcriptionName());
    }
    
    synchronized public boolean isActive() {
        return active;
    }
    
    protected boolean isFull() {
        return !active || super.isFull();
    }
    
    synchronized public void gc() {
    }

    synchronized public void add(ConnectionContext context, Destination destination) throws Exception {
        super.add(context, destination);
        destinations.put(destination.getActiveMQDestination(), destination);
        if( active || keepDurableSubsActive ) {
            Topic topic = (Topic) destination;            
            topic.activate(context, this);
        }
        if( !isFull() ) {
            dispatchMatched();
        }
    }
   
    synchronized public void activate(ConnectionContext context, ConsumerInfo info) throws Exception {
        if( !active ) {
            this.active = true;
            this.context = context;
            this.info = info;
            if( !keepDurableSubsActive ) {
                for (Iterator iter = destinations.values().iterator(); iter.hasNext();) {
                    Topic topic = (Topic) iter.next();
                    topic.activate(context, this);
                }
            }
            if( !isFull() ) {
                dispatchMatched();
            }
        }
    }

    synchronized public void deactivate(boolean keepDurableSubsActive) throws Exception {        
        active=false;
        if( !keepDurableSubsActive ) {
            for (Iterator iter = destinations.values().iterator(); iter.hasNext();) {
                Topic topic = (Topic) iter.next();
                topic.deactivate(context, this);
            }
        }
        for (Iterator iter = dispatched.iterator(); iter.hasNext();) {

            // Mark the dispatched messages as redelivered for next time.
            MessageReference node = (MessageReference) iter.next();
            Integer count = (Integer) redeliveredMessages.get(node.getMessageId());
            if( count !=null ) {
                redeliveredMessages.put(node.getMessageId(), new Integer(count.intValue()+1));
            } else {
                redeliveredMessages.put(node.getMessageId(), new Integer(1));
            }
            if( keepDurableSubsActive ) {
                pending.addFirst(node);
            } else {
                node.decrementReferenceCount();
            }
            iter.remove();
        }
        
        if( !keepDurableSubsActive ) {
            for (Iterator iter = pending.iterator(); iter.hasNext();) {
                MessageReference node = (MessageReference) iter.next();
                node.decrementReferenceCount();
                iter.remove();
            }
        }
        prefetchExtension=0;
    }

    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        MessageDispatch md = super.createMessageDispatch(node, message);
        Integer count = (Integer) redeliveredMessages.get(node.getMessageId());
        if( count !=null ) {
            md.setRedeliveryCounter(count.intValue());
        }
        return md;
    }

    synchronized public void add(MessageReference node) throws Exception {
        if( !active && !keepDurableSubsActive ) {
            return;
        }
        node.incrementReferenceCount();
        super.add(node);
    }
    
    public int getPendingQueueSize() {
        if( active || keepDurableSubsActive ) {
            return super.getPendingQueueSize();
        }
        //TODO: need to get from store
        return 0;
    }
   
    public void setSelector(String selector) throws InvalidSelectorException {
        throw new UnsupportedOperationException("You cannot dynamically change the selector for durable topic subscriptions");
    }

    protected boolean canDispatch(MessageReference node) {
        return active;
    }
    
    protected void acknowledge(ConnectionContext context, MessageAck ack, MessageReference node) throws IOException {
        node.getRegionDestination().acknowledge(context, this, ack, node);
        redeliveredMessages.remove(node.getMessageId());
        node.decrementReferenceCount();
    }
    
    public String getSubscriptionName() {
        return subscriptionKey.getSubscriptionName();
    }
    
    public String toString() {
        return 
            "DurableTopicSubscription:" +
            " consumer="+info.getConsumerId()+
            ", destinations="+destinations.size()+
            ", dispatched="+dispatched.size()+
            ", delivered="+this.prefetchExtension+
            ", pending="+this.pending.size();
    }

    public String getClientId() {
        return subscriptionKey.getClientId();
    }

    public SubscriptionKey getSubscriptionKey() {
        return subscriptionKey;
    }
    
    /**
     * Release any references that we are holding.
     */
    synchronized public void destroy() {
        
        for (Iterator iter = pending.iterator(); iter.hasNext();) {
            MessageReference node = (MessageReference) iter.next();
            node.decrementReferenceCount();
        }
        pending.clear();
        
        for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
            MessageReference node = (MessageReference) iter.next();
            node.decrementReferenceCount();
        }
        dispatched.clear();
        
    }

}
