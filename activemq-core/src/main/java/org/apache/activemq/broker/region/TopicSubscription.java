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
import java.util.LinkedList;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.transaction.Synchronization;

public class TopicSubscription extends AbstractSubscription {
    
    final protected LinkedList matched = new LinkedList();
    final protected ActiveMQDestination dlqDestination = new ActiveMQQueue("ActiveMQ.DLQ");
    final protected UsageManager usageManager;
    protected int dispatched=0;
    protected int delivered=0;
    private int maximumPendingMessages = 0;
    
    public TopicSubscription(Broker broker,ConnectionContext context, ConsumerInfo info, UsageManager usageManager) throws InvalidSelectorException {
        super(broker,context, info);
        this.usageManager=usageManager;
    }

    public void add(MessageReference node) throws InterruptedException, IOException {
        node.incrementReferenceCount();
        if( !isFull() && !isSlaveBroker()) {
            // if maximumPendingMessages is set we will only discard messages which
            // have not been dispatched (i.e. we allow the prefetch buffer to be filled)
            dispatch(node);
        } else {
            synchronized (matched) {
                matched.addLast(node);
                
                // NOTE - be careful about the slaveBroker!
                if (maximumPendingMessages > 0) {
                    // lets discard old messages as we are a slow consumer
                    while (matched.size() > maximumPendingMessages) {
                        MessageReference oldMessage = (MessageReference) matched.removeFirst();
                        oldMessage.decrementReferenceCount();
                    }
                }
            }
        }        
    }
    
    public void processMessageDispatchNotification(MessageDispatchNotification  mdn){
        synchronized(matched){
            for (Iterator i = matched.iterator(); i.hasNext();){
                MessageReference node = (MessageReference)i.next();
                if (node.getMessageId().equals(mdn.getMessageId())){
                    i.remove();
                    dispatched++;
                    node.decrementReferenceCount();
                    break;
                }
            }
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
    
    public int pending(){
        return matched.size() - dispatched;
    }
    
    public int dispatched(){
        return dispatched;
    }
    
    public int delivered(){
        return delivered;
    }
    
    public int getMaximumPendingMessages() {
        return maximumPendingMessages;
    }

    /**
     * Sets the maximum number of pending messages that can be matched against this consumer
     * before old messages are discarded.
     */
    public void setMaximumPendingMessages(int maximumPendingMessages) {
        this.maximumPendingMessages = maximumPendingMessages;
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
