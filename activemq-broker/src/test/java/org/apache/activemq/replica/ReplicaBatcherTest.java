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
package org.apache.activemq.replica;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicaBatcherTest {

    @Test
    public void batchesSmallMessages() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        for (int i = 0; i < 1347; i++) {
            ActiveMQMessage message = new ActiveMQMessage();
            message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
            list.add(new DummyMessageReference(new MessageId("1:0:0:" + i), message, 1));
        }

        List<List<MessageReference>> batches = ReplicaBatcher.batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(ReplicaBatcher.MAX_BATCH_LENGTH);
        for (int i = 0; i < ReplicaBatcher.MAX_BATCH_LENGTH; i++) {
            assertThat(batches.get(0).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + i);
        }
        assertThat(batches.get(1).size()).isEqualTo(ReplicaBatcher.MAX_BATCH_LENGTH);
        for (int i = 0; i < ReplicaBatcher.MAX_BATCH_LENGTH; i++) {
            assertThat(batches.get(1).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + (i + ReplicaBatcher.MAX_BATCH_LENGTH));
        }
        assertThat(batches.get(2).size()).isEqualTo(347);
        for (int i = 0; i < 347; i++) {
            assertThat(batches.get(2).get(i).getMessageId().toString()).isEqualTo("1:0:0:" + (i + ReplicaBatcher.MAX_BATCH_LENGTH * 2));
        }
    }

    @Test
    public void batchesBigMessages() throws Exception {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        List<MessageReference> list = new ArrayList<>();
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), message, ReplicaBatcher.MAX_BATCH_SIZE + 1));
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), message, ReplicaBatcher.MAX_BATCH_SIZE / 2 + 1));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), message, ReplicaBatcher.MAX_BATCH_SIZE / 2));

        List<List<MessageReference>> batches = ReplicaBatcher.batches(list);
        assertThat(batches.size()).isEqualTo(3);
        assertThat(batches.get(0).size()).isEqualTo(1);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(2).size()).isEqualTo(1);
        assertThat(batches.get(2).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesAcksAfterSendsSameId() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:1"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));

        List<List<MessageReference>> batches = ReplicaBatcher.batches(list);
        assertThat(batches.size()).isEqualTo(2);
        assertThat(batches.get(0).size()).isEqualTo(2);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(1).size()).isEqualTo(1);
        assertThat(batches.get(1).get(0).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    @Test
    public void batchesAcksAfterSendsDifferentIds() throws Exception {
        List<MessageReference> list = new ArrayList<>();
        ActiveMQMessage activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:1");
        list.add(new DummyMessageReference(new MessageId("1:0:0:1"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_SEND.toString());
        activeMQMessage.setStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, "1:0:0:2");
        list.add(new DummyMessageReference(new MessageId("1:0:0:2"), activeMQMessage, 1));
        activeMQMessage = new ActiveMQMessage();
        activeMQMessage.setStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY, "test");
        activeMQMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, ReplicaEventType.MESSAGE_ACK.toString());
        activeMQMessage.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, List.of("1:0:0:4"));
        list.add(new DummyMessageReference(new MessageId("1:0:0:3"), activeMQMessage, 1));

        List<List<MessageReference>> batches = ReplicaBatcher.batches(list);
        assertThat(batches.size()).isEqualTo(1);
        assertThat(batches.get(0).size()).isEqualTo(3);
        assertThat(batches.get(0).get(0).getMessageId().toString()).isEqualTo("1:0:0:1");
        assertThat(batches.get(0).get(1).getMessageId().toString()).isEqualTo("1:0:0:2");
        assertThat(batches.get(0).get(2).getMessageId().toString()).isEqualTo("1:0:0:3");
    }

    private static class DummyMessageReference implements MessageReference {

        private final MessageId messageId;
        private Message message;
        private final int size;

        DummyMessageReference(MessageId messageId, Message message, int size) {
            this.messageId = messageId;
            this.message = message;
            this.size = size;
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
        public Message.MessageDestination getRegionDestination() {
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
            return 0;
        }

        @Override
        public int incrementReferenceCount() {
            return 0;
        }

        @Override
        public int decrementReferenceCount() {
            return 0;
        }

        @Override
        public ConsumerId getTargetConsumerId() {
            return null;
        }

        @Override
        public int getSize() {
            return size;
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
