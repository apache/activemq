/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.memory.list;

import org.activemq.broker.region.MessageReference;
import org.activemq.broker.region.Subscription;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * A simple fixed size {@link MessageList} where there is a single, fixed size
 * list that all messages are added to for simplicity. Though this
 * will lead to possibly slow recovery times as many more messages
 * than is necessary will have to be iterated through for each subscription.
 * 
 * @version $Revision: 1.1 $
 */
public class SimpleMessageList implements MessageList {

    private LinkedList list = new LinkedList();
    private int maximumSize = 100 * 64 * 1024;
    private int size;
    private Object lock = new Object();

    public SimpleMessageList() {
    }

    public SimpleMessageList(int maximumSize) {
        this.maximumSize = maximumSize;
    }

    public void add(MessageReference node) {
        int delta = node.getMessageHardRef().getSize();
        synchronized (lock) {
            list.add(node);
            size += delta;
            while (size > maximumSize) {
                MessageReference evicted = (MessageReference) list.removeFirst();
                size -= evicted.getMessageHardRef().getSize();
            }
        }
    }

    public List getMessages(Subscription sub) {
        return getList();
    }

    /**
     * Returns a copy of the list
     */
    public List getList() {
        synchronized (lock) {
            return new ArrayList(list);
        }
    }

    public int getSize() {
        synchronized (lock) {
            return size;
        }
    }

    public void clear() {
        synchronized (lock) {
            list.clear();
            size = 0;
        }
    }

}
