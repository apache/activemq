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

import static org.apache.activemq.broker.region.cursors.OrderedPendingList.getValues;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.management.SizeStatisticImpl;

public class PrioritizedPendingList implements PendingList {

    private static final Integer MAX_PRIORITY = 10;
    private final OrderedPendingList[] lists = new OrderedPendingList[MAX_PRIORITY];
    private final Map<MessageId, PendingNode> map = new HashMap<MessageId, PendingNode>();
    private final SizeStatisticImpl messageSize;
    private final PendingMessageHelper pendingMessageHelper;


    public PrioritizedPendingList() {
        for (int i = 0; i < MAX_PRIORITY; i++) {
            this.lists[i] = new OrderedPendingList();
        }
        messageSize = new SizeStatisticImpl("messageSize", "The size in bytes of the pending messages");
        messageSize.setEnabled(true);
        pendingMessageHelper = new PendingMessageHelper(map, messageSize);
    }

    @Override
    public PendingNode addMessageFirst(MessageReference message) {
        PendingNode node = getList(message).addMessageFirst(message);
        this.pendingMessageHelper.addToMap(message, node);
        return node;
    }

    @Override
    public PendingNode addMessageLast(MessageReference message) {
        PendingNode node = getList(message).addMessageLast(message);
        this.pendingMessageHelper.addToMap(message, node);
        return node;
    }

    @Override
    public void clear() {
        for (int i = 0; i < MAX_PRIORITY; i++) {
            this.lists[i].clear();
        }
        this.map.clear();
        this.messageSize.reset();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public Iterator<MessageReference> iterator() {
        return new PrioritizedPendingListIterator();
    }

    @Override
    public PendingNode remove(MessageReference message) {
        PendingNode node = null;
        if (message != null) {
            node = this.pendingMessageHelper.removeFromMap(message);
            if (node != null) {
                node.getList().removeNode(node);
            }
        }
        return node;
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public long messageSize() {
        return this.messageSize.getTotalSize();
    }

    @Override
    public String toString() {
        return "PrioritizedPendingList(" + System.identityHashCode(this) + ")";
    }

    protected int getPriority(MessageReference message) {
        int priority = javax.jms.Message.DEFAULT_PRIORITY;
        if (message.getMessageId() != null) {
            priority = Math.max(message.getMessage().getPriority(), 0);
            priority = Math.min(priority, 9);
        }
        return priority;
    }

    protected OrderedPendingList getList(MessageReference msg) {
        return lists[getPriority(msg)];
    }

    private final class PrioritizedPendingListIterator implements Iterator<MessageReference> {

        private final Deque<Iterator<MessageReference>> iterators = new ArrayDeque<Iterator<MessageReference>>();

        private Iterator<MessageReference> current;
        private MessageReference currentMessage;

        PrioritizedPendingListIterator() {
            for (OrderedPendingList list : lists) {
                if (!list.isEmpty()) {
                    iterators.push(list.iterator());
                }
            }

            current = iterators.poll();
        }

        @Override
        public boolean hasNext() {
            while (current != null) {
                if (current.hasNext()) {
                    return true;
                } else {
                    current = iterators.poll();
                }
            }

            return false;
        }

        @Override
        public MessageReference next() {
            MessageReference result = null;

            while (current != null) {
                if (current.hasNext()) {
                    result = currentMessage = current.next();
                    break;
                } else {
                    current = iterators.poll();
                }
            }

            return result;
        }

        @Override
        public void remove() {
            if (currentMessage != null) {
                pendingMessageHelper.removeFromMap(currentMessage);
                current.remove();
                currentMessage = null;
            }
        }
    }

    @Override
    public boolean contains(MessageReference message) {
        if (message != null) {
            return this.map.containsKey(message.getMessageId());
        }
        return false;
    }

    @Override
    public Collection<MessageReference> values() {
        return getValues(this);
    }

    @Override
    public void addAll(PendingList pendingList) {
        for(MessageReference messageReference : pendingList) {
            addMessageLast(messageReference);
        }
    }

    @Override
    public MessageReference get(MessageId messageId) {
        PendingNode node = map.get(messageId);
        if (node != null) {
            return node.getMessage();
        }
        return null;
    }

}
