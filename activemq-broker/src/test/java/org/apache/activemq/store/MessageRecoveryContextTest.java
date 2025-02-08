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
package org.apache.activemq.store;

import static org.junit.Assert.*;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.junit.Test;

public class MessageRecoveryContextTest {

    @Test
    public void testConfigOffset() {
        MessageRecoveryContext messageRecoveryContext =
                new MessageRecoveryContext.Builder()
                    .maxMessageCountReturned(999)
                    .messageRecoveryListener(new TestMessageRecoveryListener())
                    .offset(10_000)
                    .build();

        assertNotNull(messageRecoveryContext);
        assertNull(messageRecoveryContext.getEndMessageId());
        assertEquals(Integer.valueOf(999), Integer.valueOf(messageRecoveryContext.getMaxMessageCountReturned()));
        assertNotNull(messageRecoveryContext.getMessageRecoveryListener());
        assertEquals(Long.valueOf(10_000l), Long.valueOf(messageRecoveryContext.getOffset()));
        assertNull(messageRecoveryContext.getStartMessageId());
        assertTrue(messageRecoveryContext.isUseDedicatedCursor());
    }

    @Test
    public void testConfigOffsetNoDedicatedCursor() {
        MessageRecoveryContext messageRecoveryContext =
                new MessageRecoveryContext.Builder()
                    .maxMessageCountReturned(999)
                    .messageRecoveryListener(new TestMessageRecoveryListener())
                    .offset(10_000)
                    .useDedicatedCursor(false)
                    .build();

        assertNotNull(messageRecoveryContext);
        assertNull(messageRecoveryContext.getEndMessageId());
        assertEquals(Integer.valueOf(999), Integer.valueOf(messageRecoveryContext.getMaxMessageCountReturned()));
        assertNotNull(messageRecoveryContext.getMessageRecoveryListener());
        assertEquals(Long.valueOf(10_000l), Long.valueOf(messageRecoveryContext.getOffset()));
        assertNull(messageRecoveryContext.getStartMessageId());
        assertFalse(messageRecoveryContext.isUseDedicatedCursor());
    }

    @Test
    public void testConfigStartEndMsgId() {
        MessageRecoveryContext messageRecoveryContext =
                new MessageRecoveryContext.Builder()
                    .endMessageId("ID-end-99")
                    .maxMessageCountReturned(77)
                    .messageRecoveryListener(new TestMessageRecoveryListener())
                    .startMessageId("ID-start-12")
                    .build();

        assertNotNull(messageRecoveryContext);
        assertEquals("ID-end-99", messageRecoveryContext.getEndMessageId());
        assertEquals(Integer.valueOf(77), Integer.valueOf(messageRecoveryContext.getMaxMessageCountReturned()));
        assertNotNull(messageRecoveryContext.getMessageRecoveryListener());
        assertNull(messageRecoveryContext.getOffset());
        assertEquals("ID-start-12", messageRecoveryContext.getStartMessageId());
        assertTrue(messageRecoveryContext.isUseDedicatedCursor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxReturned() {
        new MessageRecoveryContext.Builder()
            .maxMessageCountReturned(-33)
            .messageRecoveryListener(new TestMessageRecoveryListener())
            .build();
}

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMessageRecoveryListener() {
        new MessageRecoveryContext.Builder()
            .maxMessageCountReturned(44)
            .messageRecoveryListener(null)
            .startMessageId("ID-start-12")
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOffset() {
        new MessageRecoveryContext.Builder()
            .maxMessageCountReturned(33)
            .messageRecoveryListener(new TestMessageRecoveryListener())
            .offset(-1_000L)
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOffsetAndStartMessageId() {
        new MessageRecoveryContext.Builder()
            .maxMessageCountReturned(33)
            .messageRecoveryListener(new TestMessageRecoveryListener())
            .offset(1_000L)
            .startMessageId("ID-start-37")
            .build();
    }

    static class TestMessageRecoveryListener implements MessageRecoveryListener {
        @Override
        public boolean recoverMessageReference(MessageId ref) throws Exception {
            return false;
        }
        @Override
        public boolean recoverMessage(Message message) throws Exception {
            return false;
        }
        @Override
        public boolean isDuplicate(MessageId ref) {
            return false;
        }
        @Override
        public boolean hasSpace() {
            return false;
        }
    }
}
