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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.IdGenerator;
import org.junit.Test;

public class OrderPendingListTest {

    @Test
    public void testAddMessageFirst() throws Exception {

        OrderedPendingList list = new OrderedPendingList();

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            assertEquals(lastId--, iter.next().getMessageId().getProducerSequenceId());
        }
    }

    @Test
    public void testAddMessageLast() throws Exception {

        OrderedPendingList list = new OrderedPendingList();

        list.addMessageLast(new TestMessageReference(1));
        list.addMessageLast(new TestMessageReference(2));
        list.addMessageLast(new TestMessageReference(3));
        list.addMessageLast(new TestMessageReference(4));
        list.addMessageLast(new TestMessageReference(5));

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        Iterator<MessageReference> iter = list.iterator();
        int lastId = 1;
        while (iter.hasNext()) {
            assertEquals(lastId++, iter.next().getMessageId().getProducerSequenceId());
        }
    }

    @Test
    public void testClear() throws Exception {
        OrderedPendingList list = new OrderedPendingList();

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertFalse(list.isEmpty());
        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        list.clear();

        assertTrue(list.isEmpty());
        assertTrue(list.size() == 0);
        assertEquals(0, list.getAsList().size());

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageLast(new TestMessageReference(2));
        list.addMessageLast(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageLast(new TestMessageReference(5));

        assertFalse(list.isEmpty());
        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());
    }

    @Test
    public void testIsEmpty() throws Exception {
        OrderedPendingList list = new OrderedPendingList();
        assertTrue(list.isEmpty());

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertFalse(list.isEmpty());
        list.clear();
        assertTrue(list.isEmpty());
    }

    @Test
    public void testSize() {
        OrderedPendingList list = new OrderedPendingList();
        assertTrue(list.isEmpty());

        assertTrue(list.size() == 0);
        list.addMessageFirst(new TestMessageReference(1));
        assertTrue(list.size() == 1);
        list.addMessageLast(new TestMessageReference(2));
        assertTrue(list.size() == 2);
        list.addMessageFirst(new TestMessageReference(3));
        assertTrue(list.size() == 3);
        list.addMessageLast(new TestMessageReference(4));
        assertTrue(list.size() == 4);
        list.addMessageFirst(new TestMessageReference(5));
        assertTrue(list.size() == 5);

        assertFalse(list.isEmpty());
        list.clear();
        assertTrue(list.isEmpty());
        assertTrue(list.size() == 0);
    }

    @Test
    public void testRemove() throws Exception {

        OrderedPendingList list = new OrderedPendingList();

        TestMessageReference toRemove = new TestMessageReference(6);

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        list.addMessageLast(toRemove);
        list.remove(toRemove);

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        list.remove(toRemove);

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            assertEquals(lastId--, iter.next().getMessageId().getProducerSequenceId());
        }

        list.remove(null);
    }

    @Test
    public void testContains() throws Exception {

        OrderedPendingList list = new OrderedPendingList();

        TestMessageReference toRemove = new TestMessageReference(6);

        assertFalse(list.contains(toRemove));
        assertFalse(list.contains(null));

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());

        list.addMessageLast(toRemove);
        assertTrue(list.size() == 6);
        assertTrue(list.contains(toRemove));
        list.remove(toRemove);
        assertFalse(list.contains(toRemove));

        assertTrue(list.size() == 5);
        assertEquals(5, list.getAsList().size());
    }

    @Test
    public void testValues() throws Exception {

        OrderedPendingList list = new OrderedPendingList();

        TestMessageReference toRemove = new TestMessageReference(6);

        assertFalse(list.contains(toRemove));

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        Collection<MessageReference> values = list.values();
        assertEquals(5, values.size());

        for (MessageReference msg : values) {
            assertTrue(values.contains(msg));
        }

        assertFalse(values.contains(toRemove));

        list.addMessageLast(toRemove);
        values = list.values();
        assertEquals(6, values.size());
        for (MessageReference msg : values) {
            assertTrue(values.contains(msg));
        }

        assertTrue(values.contains(toRemove));
    }

    @Test
    public void testAddAll() throws Exception {
        OrderedPendingList list = new OrderedPendingList();
        TestPendingList source = new TestPendingList();

        source.addMessageFirst(new TestMessageReference(1));
        source.addMessageFirst(new TestMessageReference(2));
        source.addMessageFirst(new TestMessageReference(3));
        source.addMessageFirst(new TestMessageReference(4));
        source.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.isEmpty());
        assertEquals(5, source.size());
        list.addAll(source);
        assertEquals(5, list.size());

        for (MessageReference message : source) {
            assertTrue(list.contains(message));
        }

        list.addAll(null);
    }

    @Test
    public void testInsertAtHead() throws Exception {
        OrderedPendingList underTest = new OrderedPendingList();

        TestPendingList source = new TestPendingList();
        source.addMessageLast(new TestMessageReference(1));
        source.addMessageLast(new TestMessageReference(2));
        source.addMessageLast(new TestMessageReference(3));
        source.addMessageLast(new TestMessageReference(4));
        source.addMessageLast(new TestMessageReference(5));

        assertTrue(underTest.isEmpty());
        assertEquals(5, source.size());

        LinkedList linkedList = new LinkedList();
        linkedList.addAll(source.values());
        underTest.insertAtHead(linkedList);
        assertEquals(5, underTest.size());

        underTest.insertAtHead(null);

        linkedList.clear();

        Iterator<MessageReference> iterator = underTest.iterator();
        for (int i=0; i < 2 && iterator.hasNext(); i++ ) {
            MessageReference ref = iterator.next();
            linkedList.addLast(ref);
            iterator.remove();
            assertEquals(ref.getMessageId().getProducerSequenceId(), i + 1);
        }

        assertEquals(3, underTest.size());

        underTest.insertAtHead(linkedList);
        assertEquals(5, underTest.size());

        iterator = underTest.iterator();
        for (int i=0; iterator.hasNext(); i++ ) {
            MessageReference ref = iterator.next();
            linkedList.addLast(ref);
            iterator.remove();
            assertEquals(ref.getMessageId().getProducerSequenceId(), i + 1);
        }
        assertEquals(0, underTest.size());

    }

    static class TestPendingList implements PendingList {

        private final LinkedList<MessageReference> theList = new LinkedList<MessageReference>();

        @Override
        public boolean isEmpty() {
            return theList.isEmpty();
        }

        @Override
        public void clear() {
            theList.clear();
        }

        @Override
        public PendingNode addMessageFirst(MessageReference message) {
            theList.addFirst(message);
            return new PendingNode(null, message);
        }

        @Override
        public PendingNode addMessageLast(MessageReference message) {
            theList.addLast(message);
            return new PendingNode(null, message);
        }

        @Override
        public PendingNode remove(MessageReference message) {
            if (theList.remove(message)) {
                return new PendingNode(null, message);
            } else {
                return null;
            }
        }

        @Override
        public int size() {
            return theList.size();
        }

        @Override
        public long messageSize() {
            long size = 0;
            Iterator<MessageReference> i = theList.iterator();
            while (i.hasNext()) {
                size += i.next().getMessage().getSize();
            }
            return size;
        }

        @Override
        public Iterator<MessageReference> iterator() {
            return theList.iterator();
        }

        @Override
        public boolean contains(MessageReference message) {
            return theList.contains(message);
        }

        @Override
        public Collection<MessageReference> values() {
            return theList;
        }

        @Override
        public void addAll(PendingList pendingList) {
            for(MessageReference messageReference : pendingList) {
                theList.add(messageReference);
            }
        }

        @Override
        public MessageReference get(MessageId messageId) {
            for(MessageReference messageReference : theList) {
                if (messageReference.getMessageId().equals(messageId)) {
                    return messageReference;
                }
            }
            return null;
        }
    }

    static class TestMessageReference implements MessageReference {

        private static final IdGenerator id = new IdGenerator();

        private MessageId messageId;
        private int referenceCount = 0;

        public TestMessageReference(int sequenceId) {
            messageId = new MessageId(id.generateId() + ":1", sequenceId);
        }

        @Override
        public MessageId getMessageId() {
            return messageId;
        }

        @Override
        public Message getMessageHardRef() {
            return null;
        }

        @Override
        public Message getMessage() {
            return null;
        }

        @Override
        public boolean isPersistent() {
            return false;
        }

        @Override
        public Destination getRegionDestination() {
            return null;
        }

        @Override
        public int getRedeliveryCounter() {
            return 0;
        }

        @Override
        public void incrementRedeliveryCounter() {
        }

        @Override
        public int getReferenceCount() {
            return this.referenceCount;
        }

        @Override
        public int incrementReferenceCount() {
            return this.referenceCount++;
        }

        @Override
        public int decrementReferenceCount() {
            return this.referenceCount--;
        }

        @Override
        public ConsumerId getTargetConsumerId() {
            return null;
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public long getExpiration() {
            return 0;
        }

        @Override
        public String getGroupID() {
            return null;
        }

        @Override
        public int getGroupSequence() {
            return 0;
        }

        @Override
        public boolean isExpired() {
            return false;
        }

        @Override
        public boolean isDropped() {
            return false;
        }

        @Override
        public boolean isAdvisory() {
            return false;
        }

        @Override
        public boolean canProcessAsExpired() {
            return false;
        }
    }
}
