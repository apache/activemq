/**
 *
 * Copyright 2004 The Apache Software Foundation
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

import javax.jms.InvalidSelectorException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.transaction.Synchronization;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

public class QueueSubscription extends PrefetchSubscription {
    
    public QueueSubscription(ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        super(context, info);
    }
    
    public void add(MessageReference node) throws Throwable {
        super.add(node);
    }
    /**
     * In the queue case, mark the node as dropped and then a gc cycle will remove it from 
     * the queue.
     * @throws IOException 
     */
    protected void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference n) throws IOException {
        
        final IndirectMessageReference node = (IndirectMessageReference) n;

        final Queue queue = (Queue)node.getRegionDestination();
        queue.acknowledge(context, this, ack, node);
        
        if( !ack.isInTransaction() ) {
            node.drop();            
            queue.dropEvent();
        } else {
            node.setAcked(true);
            context.getTransaction().addSynchronization(new Synchronization(){
                public void afterCommit() throws Throwable {                    
                    node.drop();            
                    queue.dropEvent();
                }
                public void afterRollback() throws Throwable {
                    node.setAcked(false);
                }
            });
        }
    }
    
    protected boolean canDispatch(MessageReference n) {
        IndirectMessageReference node = (IndirectMessageReference) n;
        if( node.isAcked() )
            return false;
        
        // Keep message groups together.
        String groupId = node.getGroupID();
        int sequence = node.getGroupSequence();
        if( groupId!=null ) {
            
            ConcurrentHashMap messageGroupOwners = ((Queue)node.getRegionDestination()).getMessageGroupOwners();            
            
            // If we can own the first, then no-one else should own the rest.
            if( sequence==1 ) {
                if( node.lock(this) ) {
                    messageGroupOwners.put(groupId, info.getConsumerId());
                    return true;
                } else {
                    return false;
                }
            }
            
            // Make sure that the previous owner is still valid, we may 
            // need to become the new owner.
            ConsumerId groupOwner;
            synchronized(node) {
                groupOwner = (ConsumerId) messageGroupOwners.get(groupId);
                if( groupOwner==null ) {
                    if( node.lock(this) ) {
                        messageGroupOwners.put(groupId, info.getConsumerId());
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            
            if( groupOwner.equals(info.getConsumerId()) ) {
                // A group sequence < 1 is an end of group signal.
                if ( sequence < 1 ) {
                    messageGroupOwners.remove(groupId);
                }
                return true;
            }
            
            return false;
            
        } else {
            return node.lock(this);
        }
        
    }
    
    public String toString() {
        return 
            "QueueSubscription:" +
            " consumer="+info.getConsumerId()+
            ", destinations="+destinations.size()+
            ", dispatched="+dispatched.size()+
            ", delivered="+this.delivered+
            ", matched="+this.matched.size();
    }

}
