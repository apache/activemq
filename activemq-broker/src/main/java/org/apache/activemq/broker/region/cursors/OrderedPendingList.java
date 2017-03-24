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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.management.SizeStatisticImpl;

public class OrderedPendingList implements PendingList {

    private PendingNode root = null;
    private PendingNode tail = null;
    private final Map<MessageId, PendingNode> map = new HashMap<MessageId, PendingNode>();
    private final SizeStatisticImpl messageSize;
    private final PendingMessageHelper pendingMessageHelper;

    public OrderedPendingList() {
        messageSize = new SizeStatisticImpl("messageSize", "The size in bytes of the pending messages");
        messageSize.setEnabled(true);
        pendingMessageHelper = new PendingMessageHelper(map, messageSize);
    }

    @Override
    public PendingNode addMessageFirst(MessageReference message) {
        PendingNode node = new PendingNode(this, message);
        if (root == null) {
            root = node;
            tail = node;
        } else {
            root.linkBefore(node);
            root = node;
        }
        pendingMessageHelper.addToMap(message, node);
        return node;
    }

    @Override
    public PendingNode addMessageLast(MessageReference message) {
        PendingNode node = new PendingNode(this, message);
        if (root == null) {
            root = node;
        } else {
            tail.linkAfter(node);
        }
        tail = node;
        pendingMessageHelper.addToMap(message, node);
        return node;
    }

    @Override
    public void clear() {
        this.root = null;
        this.tail = null;
        this.map.clear();
        this.messageSize.reset();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public Iterator<MessageReference> iterator() {
        return new Iterator<MessageReference>() {
            private PendingNode current = null;
            private PendingNode next = root;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public MessageReference next() {
                MessageReference result = null;
                this.current = this.next;
                result = this.current.getMessage();
                this.next = (PendingNode) this.next.getNext();
                return result;
            }

            @Override
            public void remove() {
                if (this.current != null && this.current.getMessage() != null) {
                    pendingMessageHelper.removeFromMap(this.current.getMessage());
                }
                removeNode(this.current);
            }
        };
    }

    @Override
    public PendingNode remove(MessageReference message) {
        PendingNode node = null;
        if (message != null) {
            node = pendingMessageHelper.removeFromMap(message);
            removeNode(node);
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

    void removeNode(PendingNode node) {
        if (node != null) {
            pendingMessageHelper.removeFromMap(node.getMessage());
            if (root == node) {
                root = (PendingNode) node.getNext();
            }
            if (tail == node) {
                tail = (PendingNode) node.getPrevious();
            }
            node.unlink();
        }
    }

    List<PendingNode> getAsList() {
        List<PendingNode> result = new ArrayList<PendingNode>(size());
        PendingNode node = root;
        while (node != null) {
            result.add(node);
            node = (PendingNode) node.getNext();
        }
        return result;
    }

    @Override
    public String toString() {
        return "OrderedPendingList(" + System.identityHashCode(this) + ")";
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

    public static Collection<MessageReference> getValues(final PendingList pendingList) {
        List<MessageReference> messageReferences = new ArrayList<MessageReference>();
        Iterator<MessageReference> iterator = pendingList.iterator();
        while (iterator.hasNext()) {
            messageReferences.add(iterator.next());
        }
        return messageReferences;
    }

    @Override
    public void addAll(PendingList pendingList) {
        if (pendingList != null) {
            for(MessageReference messageReference : pendingList) {
                addMessageLast(messageReference);
            }
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

    public void insertAtHead(List<MessageReference> list) {
        if (list != null && !list.isEmpty()) {
            PendingNode newHead = null;
            PendingNode appendNode = null;
            for (MessageReference ref : list) {
                PendingNode node = new PendingNode(this, ref);
                pendingMessageHelper.addToMap(ref, node);
                if (newHead == null) {
                    newHead = node;
                    appendNode = node;
                    continue;
                }
                appendNode.linkAfter(node);
                appendNode = node;
            }
            // insert this new list at root
            if (root == null) {
                root = newHead;
                tail = appendNode;
            } else {
                appendNode.linkAfter(root);
                root = newHead;
            }
        }
    }

}
