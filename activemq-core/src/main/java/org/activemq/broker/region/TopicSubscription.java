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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.MessageDispatch;
import org.activemq.memory.UsageManager;
import org.activemq.transaction.Synchronization;

public class TopicSubscription extends AbstractSubscription {
    
    final protected LinkedList matched = new LinkedList();
    final protected ActiveMQDestination dlqDestination = new ActiveMQQueue("ActiveMQ.DLQ");
    final protected UsageManager usageManager;
    protected int dispatched=0;
    protected int delivered=0;
    
    public TopicSubscription(ConnectionContext context, ConsumerInfo info, UsageManager usageManager) throws InvalidSelectorException {
        super(context, info);
        this.usageManager=usageManager;
    }

    public void add(MessageReference node) throws InterruptedException, IOException {
        node.incrementReferenceCount();
        if( !isFull() ) {
            dispatch(node);
        } else {
            matched.addLast(node);
        }        
    }
    
    public void acknowledge(final ConnectionContext context, final MessageAck ack) throws Throwable {
        
        // Handle the standard acknowledgment case.
        boolean wasFull = isFull();
        if( ack.isStandardAck() || ack.isPoisonAck() ) {
            if ( context.isInTransaction() ) {
                delivered += ack.getMessageCount();
                context.getTransaction().addSynchronization(new Synchronization(){
                    public void afterCommit() throws Throwable {
                        synchronized(TopicSubscription.this) {
                            dispatched -= ack.getMessageCount();
                            delivered = Math.max(0, delivered - ack.getMessageCount());
                        }
                    }
                });
            } else {
                dispatched -= ack.getMessageCount();
                delivered = Math.max(0, delivered - ack.getMessageCount());
            }
            
            if( wasFull && !isFull() ) {                            
                dispatchMatched();
            }
            return;
            
        } else if( ack.isDeliveredAck() ) {
            // Message was delivered but not acknowledged: update pre-fetch counters.
            delivered += ack.getMessageCount();
            if( wasFull && !isFull() ) {                            
                dispatchMatched();
            }
            return;
        }
        
        throw new JMSException("Invalid acknowledgment: "+ack);
    }
    
    private boolean isFull() {
        return dispatched-delivered >= info.getPrefetchSize();
    }
    
    private void dispatchMatched() throws IOException {
        for (Iterator iter = matched.iterator(); iter.hasNext() && !isFull();) {
            MessageReference message = (MessageReference) iter.next();
            iter.remove();
            dispatch(message);
        }
    }

    private void dispatch(final MessageReference node) throws IOException {
                
        Message message = (Message) node;
        
        // Make sure we can dispatch a message.        
        MessageDispatch md = new MessageDispatch();
        md.setMessage(message);
        md.setConsumerId( info.getConsumerId() );
        md.setDestination( node.getRegionDestination().getActiveMQDestination() );

        dispatched++;
        if( info.isDispatchAsync() ) {
            md.setConsumer(new Runnable(){
                public void run() {
                    node.decrementReferenceCount();
                }
            });
            context.getConnection().dispatchAsync(md);
        } else {
            context.getConnection().dispatchSync(md);                
            node.decrementReferenceCount();
        }        
    }
    
    public String toString() {
        return 
            "TopicSubscription:" +
            " consumer="+info.getConsumerId()+
            ", destinations="+destinations.size()+
            ", dispatched="+dispatched+
            ", delivered="+this.delivered+
            ", matched="+this.matched.size();
    }

}
