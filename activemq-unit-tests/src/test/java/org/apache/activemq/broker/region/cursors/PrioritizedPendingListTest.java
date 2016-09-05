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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.IdGenerator;
import org.junit.Test;

public class PrioritizedPendingListTest {

    @Test
    public void testAddMessageFirst() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.size() == 5);

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            assertEquals(lastId--, iter.next().getMessageId().getProducerSequenceId());
        }
    }

    @Test
    public void testAddMessageLast() {

        PrioritizedPendingList list = new PrioritizedPendingList();

        list.addMessageLast(new TestMessageReference(1));
        list.addMessageLast(new TestMessageReference(2));
        list.addMessageLast(new TestMessageReference(3));
        list.addMessageLast(new TestMessageReference(4));
        list.addMessageLast(new TestMessageReference(5));

        assertTrue(list.size() == 5);

        Iterator<MessageReference> iter = list.iterator();
        int lastId = 1;
        while (iter.hasNext()) {
            assertEquals(lastId++, iter.next().getMessageId().getProducerSequenceId());
        }
    }

    @Test
    public void testClear() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertFalse(list.isEmpty());
        assertTrue(list.size() == 5);

        list.clear();

        assertTrue(list.isEmpty());
        assertTrue(list.size() == 0);

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageLast(new TestMessageReference(2));
        list.addMessageLast(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageLast(new TestMessageReference(5));

        assertFalse(list.isEmpty());
        assertTrue(list.size() == 5);
    }

    @Test
    public void testIsEmpty() {
        PrioritizedPendingList list = new PrioritizedPendingList();
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
    public void testRemove() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        TestMessageReference toRemove = new TestMessageReference(6);

        list.addMessageFirst(new TestMessageReference(1));
        list.addMessageFirst(new TestMessageReference(2));
        list.addMessageFirst(new TestMessageReference(3));
        list.addMessageFirst(new TestMessageReference(4));
        list.addMessageFirst(new TestMessageReference(5));

        assertTrue(list.size() == 5);

        list.addMessageLast(toRemove);
        list.remove(toRemove);

        assertTrue(list.size() == 5);

        list.remove(toRemove);

        assertTrue(list.size() == 5);

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            assertEquals(lastId--, iter.next().getMessageId().getProducerSequenceId());
        }

        list.remove(null);
    }

    @Test
    public void testSize() {
        PrioritizedPendingList list = new PrioritizedPendingList();
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
    public void testPrioritization() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        list.addMessageFirst(new TestMessageReference(1, 5));
        list.addMessageFirst(new TestMessageReference(2, 4));
        list.addMessageFirst(new TestMessageReference(3, 3));
        list.addMessageFirst(new TestMessageReference(4, 2));
        list.addMessageFirst(new TestMessageReference(5, 1));

        assertTrue(list.size() == 5);

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            MessageReference nextMessage = iter.next();
            assertNotNull(nextMessage);
            assertEquals(lastId--, nextMessage.getMessage().getPriority());
        }
    }

    @Test
    public void testValuesPriority() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        list.addMessageFirst(new TestMessageReference(1, 2));
        list.addMessageFirst(new TestMessageReference(2, 1));
        list.addMessageFirst(new TestMessageReference(3, 3));
        list.addMessageFirst(new TestMessageReference(4, 5));
        list.addMessageFirst(new TestMessageReference(5, 4));

        assertTrue(list.size() == 5);

        Iterator<MessageReference> iter = list.iterator();
        int lastId = list.size();
        while (iter.hasNext()) {
            assertEquals(lastId--, iter.next().getMessage().getPriority());
        }

        lastId = list.size();
        for (MessageReference messageReference : list.values()) {
            assertEquals(lastId--, messageReference.getMessage().getPriority());
        }
    }

    @Test
    public void testFullRangeIteration() {
        PrioritizedPendingList list = new PrioritizedPendingList();

        int totalElements = 0;

        for (int i = 0; i < 10; ++i) {
            list.addMessageFirst(new TestMessageReference(totalElements++, i));
            list.addMessageFirst(new TestMessageReference(totalElements++, i));
        }

        assertTrue(list.size() == totalElements);

        int totalIterated = 0;
        Iterator<MessageReference> iter = list.iterator();
        while (iter.hasNext()) {
            MessageReference nextMessage = iter.next();
            assertNotNull(nextMessage);
            totalIterated++;
        }

        assertEquals(totalElements, totalIterated);
    }

    static class TestMessageReference implements MessageReference {

        private static final IdGenerator id = new IdGenerator();

        private Message message;
        private MessageId messageId;
        private int referenceCount = 0;

        public TestMessageReference(int sequenceId) {
            messageId = new MessageId(id.generateId() + ":1", sequenceId);
            message = new ActiveMQMessage();
            message.setPriority((byte) javax.jms.Message.DEFAULT_PRIORITY);
        }

        public TestMessageReference(int sequenceId, int priority) {
            messageId = new MessageId(id.generateId() + ":1", sequenceId);
            message = new ActiveMQMessage();
            message.setPriority((byte) priority);
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
            return message;
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
