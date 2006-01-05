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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.SubscriptionInfo;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

public class DurableTopicSubscription extends PrefetchSubscription {
    
    final protected String clientId;
    final protected String subscriptionName;
    final ConcurrentHashMap redeliveredMessages = new ConcurrentHashMap();
    
    boolean active=true;
    boolean recovered=true;
    
    public DurableTopicSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        super(context, info);
        this.clientId = context.getClientId();
        this.subscriptionName = info.getSubcriptionName();
    }
    
    public DurableTopicSubscription(SubscriptionInfo info) throws InvalidSelectorException {
        super(null, createFakeConsumerInfo(info));
        this.clientId = info.getClientId();
        this.subscriptionName = info.getSubcriptionName();
        active=false;
        recovered=false;        
    }

    private static ConsumerInfo createFakeConsumerInfo(SubscriptionInfo info) {
        ConsumerInfo rc = new ConsumerInfo();
        rc.setSelector(info.getSelector());
        rc.setSubcriptionName(info.getSubcriptionName());
        rc.setDestination(info.getDestination());
        return rc;
    }

    synchronized public boolean isActive() {
        return active;
    }
    synchronized public boolean isRecovered() {
        return recovered;
    }
    
    protected boolean isFull() {
        return !active || super.isFull();
    }
    
    synchronized public void gc() {
        if( !active && recovered ) {
            recovered = false;
            
            for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
                MessageReference node = (MessageReference) iter.next();
                // node.decrementTargetCount();
                iter.remove();
            }
            
            for (Iterator iter = matched.iterator(); iter.hasNext();) {
                MessageReference node = (MessageReference) iter.next();
                // node.decrementTargetCount();
                iter.remove();
            }
            
            delivered=0;
        }
    }

    synchronized public void deactivate() {        
        active=false;
        for (Iterator iter = dispatched.iterator(); iter.hasNext();) {
            
            MessageReference node = (MessageReference) iter.next();
            Integer count = (Integer) redeliveredMessages.get(node.getMessageId());
            if( count !=null ) {
                redeliveredMessages.put(node.getMessageId(), new Integer(count.intValue()+1));
            } else {
                redeliveredMessages.put(node.getMessageId(), new Integer(1));
            }
            
            // Undo the dispatch.
            matched.addFirst(node);
            iter.remove();
        }
        delivered=0;
    }

    synchronized public void activate(ConnectionContext context, ConsumerInfo info) throws Throwable {
        if( !active ) {
            this.active = true;
            this.context = context;
            this.info = info;
            if( !recovered ) {
                recovered=true;
                for (Iterator iter = destinations.iterator(); iter.hasNext();) {
                    Topic topic = (Topic) iter.next();
                    topic.recover(this, false);
                }
            } else {
                if( !isFull() ) {                            
                    dispatchMatched();
                }
            }
        }
    }

    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        MessageDispatch md = super.createMessageDispatch(node, message);
        Integer count = (Integer) redeliveredMessages.get(node.getMessageId());
        if( count !=null ) {
            md.setRedeliveryCounter(count.intValue());
        }
        return md;
    }

    synchronized public void add(MessageReference node) throws Throwable {
        assert recovered;
        node = new IndirectMessageReference(node.getRegionDestination(), (Message) node);
        super.add(node);
        node.decrementReferenceCount();
    }

    protected boolean canDispatch(MessageReference node) {
        return active;
    }
    
    public synchronized void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        assert recovered;
        super.acknowledge(context, ack);
    }

    protected void acknowledge(ConnectionContext context, MessageAck ack, MessageReference node) throws IOException {
        node.getRegionDestination().acknowledge(context, this, ack, node);
        redeliveredMessages.remove(node.getMessageId());
        ((IndirectMessageReference)node).drop();
    }
    
    public String getSubscriptionName() {
        return subscriptionName;
    }
    
    public String toString() {
        return 
            "DurableTopicSubscription:" +
            " consumer="+info.getConsumerId()+
            ", destinations="+destinations.size()+
            ", dispatched="+dispatched.size()+
            ", delivered="+this.delivered+
            ", matched="+this.matched.size();
    }

    public String getClientId() {
        return clientId;
    }

}
