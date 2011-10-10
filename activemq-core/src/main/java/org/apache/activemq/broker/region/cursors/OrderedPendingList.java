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

public class OrderedPendingList implements PendingList {
    PendingNode root = null;
    PendingNode tail = null;
    final Map<MessageId, PendingNode> map = new HashMap<MessageId, PendingNode>();

    public PendingNode addMessageFirst(MessageReference message) {
        PendingNode node = new PendingNode(this, message);
        if (root == null) {
            root = node;
            tail = node;
        } else {
            root.linkBefore(node);
        }
        this.map.put(message.getMessageId(), node);
        return node;
    }

    public PendingNode addMessageLast(MessageReference message) {
        PendingNode node = new PendingNode(this, message);
        if (root == null) {
            root = node;
        } else {
            tail.linkAfter(node);
        }
        tail = node;
        this.map.put(message.getMessageId(), node);
        return node;
    }

    public void clear() {
        this.root = null;
        this.tail = null;
        this.map.clear();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    public Iterator<MessageReference> iterator() {
        return new Iterator<MessageReference>() {
            private PendingNode current = null;
            private PendingNode next = root;

            public boolean hasNext() {
                return next != null;
            }

            public MessageReference next() {
                MessageReference result = null;
                this.current = this.next;
                result = this.current.getMessage();
                this.next = (PendingNode) this.next.getNext();
                return result;
            }

            public void remove() {
                if (this.current != null && this.current.getMessage() != null) {
                    map.remove(this.current.getMessage().getMessageId());
                }
                removeNode(this.current);
            }
        };
    }

    public PendingNode remove(MessageReference message) {
        PendingNode node = null;
        if (message != null) {
            node = this.map.remove(message.getMessageId());
            removeNode(node);
        }
        return node;
    }

    public int size() {
        return this.map.size();
    }

    void removeNode(PendingNode node) {
        if (node != null) {
            map.remove(node.getMessage().getMessageId());
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

}
