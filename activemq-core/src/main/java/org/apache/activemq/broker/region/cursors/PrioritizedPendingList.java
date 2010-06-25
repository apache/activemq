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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;

public class PrioritizedPendingList implements PendingList {
    static final Integer MAX_PRIORITY = 10;
    private final OrderedPendingList[] lists = new OrderedPendingList[MAX_PRIORITY];
    final Map<MessageId, PendingNode> map = new HashMap<MessageId, PendingNode>();

    public PrioritizedPendingList() {
        for (int i = 0; i < MAX_PRIORITY; i++) {
            this.lists[i] = new OrderedPendingList();
        }
    }
    public PendingNode addMessageFirst(MessageReference message) {
        PendingNode node = getList(message).addMessageFirst(message);
        this.map.put(message.getMessageId(), node);
        return node;
    }

    public PendingNode addMessageLast(MessageReference message) {
        PendingNode node = getList(message).addMessageLast(message);
        this.map.put(message.getMessageId(), node);
        return node;
    }

    public void clear() {
        for (int i = 0; i < MAX_PRIORITY; i++) {
            this.lists[i].clear();
        }
        this.map.clear();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    public Iterator<MessageReference> iterator() {
        return new PrioritizedPendingListIterator();
    }

    public void remove(MessageReference message) {
        if (message != null) {
            PendingNode node = this.map.remove(message.getMessageId());
            if (node != null) {
                node.getList().removeNode(node);
            }
        }
    }

    public int size() {
        return this.map.size();
    }

    @Override
    public String toString() {
        return "PrioritizedPendingList(" + System.identityHashCode(this) + ")";
    }

    protected int getPriority(MessageReference message) {
        int priority = javax.jms.Message.DEFAULT_PRIORITY;
        if (message.getMessageId() != null) {
            Math.max(message.getMessage().getPriority(), 0);
            priority = Math.min(priority, 9);
        }
        return priority;
    }

    protected OrderedPendingList getList(MessageReference msg) {
        return lists[getPriority(msg)];
    }

    private class PrioritizedPendingListIterator implements Iterator<MessageReference> {
        private int index = 0;
        private int currentIndex = 0;
        List<PendingNode> list = new ArrayList<PendingNode>(size());

        PrioritizedPendingListIterator() {
            for (int i = MAX_PRIORITY - 1; i >= 0; i--) {
                OrderedPendingList orderedPendingList = lists[i];
                if (!orderedPendingList.isEmpty()) {
                    list.addAll(orderedPendingList.getAsList());
                }
            }
        }
        public boolean hasNext() {
            return list.size() > index;
        }

        public MessageReference next() {
            PendingNode node = list.get(this.index);
            this.currentIndex = this.index;
            this.index++;
            return node.getMessage();
        }

        public void remove() {
            PendingNode node = list.get(this.currentIndex);
            if (node != null) {
                map.remove(node.getMessage().getMessageId());
                node.getList().removeNode(node);
            }

        }

    }

}
