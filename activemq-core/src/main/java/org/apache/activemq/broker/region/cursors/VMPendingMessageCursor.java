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
package org.apache.activemq.broker.region.cursors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.QueueMessageReference;

/**
 * hold pending messages in a linked list (messages awaiting disptach to a
 * consumer) cursor
 * 
 * @version $Revision$
 */
public class VMPendingMessageCursor extends AbstractPendingMessageCursor {
    private LinkedList<MessageReference> list = new LinkedList<MessageReference>();
    private Iterator<MessageReference> iter;
    private MessageReference last;

    
    @Override
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        List<MessageReference> rc = new ArrayList<MessageReference>();
        for (MessageReference r : list) {
            if( r.getRegionDestination()==destination ) {
                rc.add(r);
            }
        }
        return rc ;        
    }
    
    /**
     * @return true if there are no pending messages
     */
    public synchronized boolean isEmpty() {
        if (list.isEmpty()) {
            return true;
        } else {
            for (Iterator<MessageReference> iterator = list.iterator(); iterator.hasNext();) {
                MessageReference node = iterator.next();
                if (node== QueueMessageReference.NULL_MESSAGE){
                	continue;
                }
                if (!node.isDropped()) {
                    return false;
                }
                // We can remove dropped references.
                iterator.remove();
            }
            return true;
        }
    }

    /**
     * reset the cursor
     */
    public synchronized void reset() {
        iter = list.listIterator();
        last = null;
    }

    /**
     * add message to await dispatch
     * 
     * @param node
     */
    public synchronized void addMessageLast(MessageReference node) {
        node.incrementReferenceCount();
        list.addLast(node);
    }

    /**
     * add message to await dispatch
     * 
     * @param position
     * @param node
     */
    public synchronized void addMessageFirst(MessageReference node) {
        node.incrementReferenceCount();
        list.addFirst(node);
    }

    /**
     * @return true if there pending messages to dispatch
     */
    public synchronized boolean hasNext() {
        return iter.hasNext();
    }

    /**
     * @return the next pending message
     */
    public synchronized MessageReference next() {
        last = (MessageReference)iter.next();
        return last;
    }

    /**
     * remove the message at the cursor position
     */
    public synchronized void remove() {
        if (last != null) {
            last.decrementReferenceCount();
        }
        iter.remove();
    }

    /**
     * @return the number of pending messages
     */
    public synchronized int size() {
        return list.size();
    }

    /**
     * clear all pending messages
     */
    public synchronized void clear() {
        list.clear();
    }

    public synchronized void remove(MessageReference node) {
        for (Iterator<MessageReference> i = list.iterator(); i.hasNext();) {
            MessageReference ref = i.next();
            if (node.getMessageId().equals(ref.getMessageId())) {
                ref.decrementReferenceCount();
                i.remove();
                break;
            }
        }
    }

    /**
     * Page in a restricted number of messages
     * 
     * @param maxItems
     * @return a list of paged in messages
     */
    public LinkedList<MessageReference> pageInList(int maxItems) {
        return list;
    }
    
    public boolean isTransient() {
        return true;
    }
}
