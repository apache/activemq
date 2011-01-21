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
    private final PendingList list;
    private Iterator<MessageReference> iter;
    
    public VMPendingMessageCursor(boolean prioritizedMessages) {
        super(prioritizedMessages);
        if (this.prioritizedMessages) {
            this.list= new PrioritizedPendingList();
        }else {
            this.list = new OrderedPendingList();
        }
    }

    
    public synchronized List<MessageReference> remove(ConnectionContext context, Destination destination)
            throws Exception {
        List<MessageReference> rc = new ArrayList<MessageReference>();
        for (Iterator<MessageReference> iterator = list.iterator(); iterator.hasNext();) {
            MessageReference r = iterator.next();
            if (r.getRegionDestination() == destination) {
                r.decrementReferenceCount();
                rc.add(r);
                iterator.remove();
            }
        }
        return rc;
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
                if (node == QueueMessageReference.NULL_MESSAGE) {
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
        iter = list.iterator();
        last = null;
    }

    /**
     * add message to await dispatch
     * 
     * @param node
     */
    
    public synchronized void addMessageLast(MessageReference node) {
        node.incrementReferenceCount();
        list.addMessageLast(node);
    }

    /**
     * add message to await dispatch
     * 
     * @param position
     * @param node
     */
    
    public synchronized void addMessageFirst(MessageReference node) {
        node.incrementReferenceCount();
        list.addMessageFirst(node);
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
        last = iter.next();
        if (last != null) {
            last.incrementReferenceCount();
        }
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
        for (Iterator<MessageReference> i = list.iterator(); i.hasNext();) {
            MessageReference ref = i.next();
            ref.decrementReferenceCount();
        }
        list.clear();
    }

    
    public synchronized void remove(MessageReference node) {
        list.remove(node);
        node.decrementReferenceCount();
    }

    /**
     * Page in a restricted number of messages
     * 
     * @param maxItems
     * @return a list of paged in messages
     */
    
    public LinkedList<MessageReference> pageInList(int maxItems) {
        LinkedList<MessageReference> result = new LinkedList<MessageReference>();
        for (Iterator<MessageReference>i = list.iterator();i.hasNext();) {
            MessageReference ref = i.next();
            ref.incrementReferenceCount();
            result.add(ref);
            if (result.size() >= maxItems) {
                break;
            }
        }
        return result;
    }

    
    public boolean isTransient() {
        return true;
    }

    
    public void destroy() throws Exception {
        super.destroy();
        clear();
    }
}
