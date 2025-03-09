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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

public class MessageRecoveryContext implements MessageRecoveryListener {

    public static final int DEFAULT_MAX_MESSAGE_COUNT_RETURNED = 100;
    public static final boolean DEFAULT_USE_DEDICATED_CURSOR = true;

    // Config
    private final boolean useDedicatedCursor;
    private final int maxMessageCountReturned;
    private final Long offset;
    private final String startMessageId;
    private String endMessageId = null;
    private final MessageRecoveryListener messageRecoveryListener;

    // State
    private Long endSequenceId = Long.MAX_VALUE;

    // Stats
    private final AtomicInteger recoveredCount = new AtomicInteger(0);

    MessageRecoveryContext(final MessageRecoveryListener messageRecoveryListener, final String startMessageId, 
            final String endMessageId, final Long offset, final Integer maxMessageCountReturned, 
            final Boolean useDedicatedCursor) {
        if(maxMessageCountReturned != null && maxMessageCountReturned < 0) {
            throw new IllegalArgumentException("maxMessageCountReturned must be a positive integer value");
        }
        if(messageRecoveryListener == null) {
            throw new IllegalArgumentException("MessageRecoveryListener must be specified");
        }
        if(offset != null) {
            if(offset < 0L) {
                throw new IllegalArgumentException("offset must be a non-negative integer value");
            }
            if(startMessageId != null) { 
                throw new IllegalArgumentException("Only one of offset and startMessageId may be specified");
            }
        }
        this.endMessageId = endMessageId;
        this.maxMessageCountReturned = (maxMessageCountReturned != null ? maxMessageCountReturned : DEFAULT_MAX_MESSAGE_COUNT_RETURNED);
        this.messageRecoveryListener = messageRecoveryListener;
        this.offset = offset;
        this.startMessageId = startMessageId;
        this.useDedicatedCursor = (useDedicatedCursor != null ? useDedicatedCursor : DEFAULT_USE_DEDICATED_CURSOR);
    }

    public boolean isUseDedicatedCursor() {
        return this.useDedicatedCursor;
    }

    public int getMaxMessageCountReturned() {
        return this.maxMessageCountReturned;
    }

    public Long getOffset() {
        return this.offset;
    }

    public String getEndMessageId() {
        return this.endMessageId;
    }

    public String getStartMessageId() {
        return this.startMessageId;
    }

    public MessageRecoveryListener getMessageRecoveryListener() {
        return this.messageRecoveryListener;
    }

    // MessageRecoveryContext functions
    public void setEndSequenceId(long endSequenceId) {
        this.endSequenceId = endSequenceId;
    }

    public boolean canRecoveryNextMessage(Long sequenceId) {
        if (getRecoveredCount() >= getMaxMessageCountReturned() || 
            !this.messageRecoveryListener.canRecoveryNextMessage() ||
            sequenceId >= endSequenceId) {
            return false;
        }
        return true;
    }

    // MessageRecoveryListener functions
    public boolean recoverMessage(Message message) throws Exception {
        boolean tmpReturned = this.messageRecoveryListener.recoverMessage(message);
        if(tmpReturned) {
            this.recoveredCount.incrementAndGet();
        }
        return tmpReturned;
    }

    @Override
    public boolean recoverMessageReference(MessageId ref) throws Exception {
        return this.messageRecoveryListener.recoverMessageReference(ref);
    }

    @Override
    public boolean hasSpace() {
        return this.messageRecoveryListener.hasSpace();
    }

    @Override
    public boolean isDuplicate(MessageId ref) {
        return this.messageRecoveryListener.isDuplicate(ref);
    }

    // Metrics
    public int getRecoveredCount() {
        return this.recoveredCount.get();
    }

    @Override
    public String toString() {
        return "MessageRecoveryContext [useDedicatedCursor=" + useDedicatedCursor + ", maxMessageCountReturned="
                + maxMessageCountReturned + ", offset=" + offset + ", startMessageId=" + startMessageId
                + ", endMessageId=" + endMessageId + ", messageRecoveryListener=" + messageRecoveryListener
                + ", endSequenceId=" + endSequenceId + ", recoveredCount=" + recoveredCount + "]";
    }

    public static class Builder {

        private Boolean useDedicatedCursor;
        private Integer maxMessageCountReturned;
        private Long offset;
        private String startMessageId;
        private String endMessageId;
        private MessageRecoveryListener messageRecoveryListener;

        public Builder useDedicatedCursor(final boolean useDedicatedCursor) {
            this.useDedicatedCursor = useDedicatedCursor;
            return this;
        }

        public Builder maxMessageCountReturned(final int maxMessageCountReturned) {
            this.maxMessageCountReturned = maxMessageCountReturned;
            return this;
        }

        public Builder offset(final long offset) {
            this.offset = offset;
            return this;
        }

        public Builder endMessageId(final String endMessageId) {
            this.endMessageId = endMessageId;
            return this;
        }

        public Builder startMessageId(final String startMessageId) {
            this.startMessageId = startMessageId;
            return this;
        }

        public Builder messageRecoveryListener(final MessageRecoveryListener messageRecoveryListener) {
            this.messageRecoveryListener = messageRecoveryListener;
            return this;
        }

        public MessageRecoveryContext build() {
            return new MessageRecoveryContext(messageRecoveryListener, startMessageId, endMessageId, offset, maxMessageCountReturned, useDedicatedCursor);
        }
    }
}

