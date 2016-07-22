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
package org.apache.activemq.broker.region;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

/**
 * Keeps track of a message that is flowing through the Broker. This object may
 * hold a hard reference to the message or only hold the id of the message if
 * the message has been persisted on in a MessageStore.
 *
 *
 */
public class IndirectMessageReference implements QueueMessageReference {

    /** The subscription that has locked the message */
    private LockOwner lockOwner;
    /** Has the message been dropped? */
    private boolean dropped;
    /** Has the message been acked? */
    private boolean acked;
    /** Direct reference to the message */
    private final Message message;
    private final MessageId messageId;

    /**
     * @param message
     */
    public IndirectMessageReference(final Message message) {
        this.message = message;
        this.messageId = message.getMessageId().copy();
        message.getMessageId();
        message.getGroupID();
        message.getGroupSequence();
    }

    @Override
    public Message getMessageHardRef() {
        return message;
    }

    @Override
    public int getReferenceCount() {
        return message.getReferenceCount();
    }

    @Override
    public int incrementReferenceCount() {
        return message.incrementReferenceCount();
    }

    @Override
    public int decrementReferenceCount() {
        return message.decrementReferenceCount();
    }

    @Override
    public Message getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Message " + message.getMessageId() + " dropped=" + dropped + " acked=" + acked + " locked=" + (lockOwner != null);
    }

    @Override
    public void incrementRedeliveryCounter() {
        message.incrementRedeliveryCounter();
    }

    @Override
    public synchronized boolean isDropped() {
        return dropped;
    }

    @Override
    public synchronized void drop() {
        dropped = true;
        lockOwner = null;
        message.decrementReferenceCount();
    }

    /**
     * Check if the message has already been dropped before
     * dropping. Return true if dropped, else false.
     * This method exists so that this can be done atomically
     * under the intrinisic lock
     */
    @Override
    public synchronized boolean dropIfLive() {
        if (isDropped()) {
            return false;
        } else {
            drop();
            return true;
        }
    }

    @Override
    public boolean lock(LockOwner subscription) {
        synchronized (this) {
            if (dropped || lockOwner != null) {
                return false;
            }
            lockOwner = subscription;
            return true;
        }
    }

    @Override
    public synchronized boolean unlock() {
        boolean result = lockOwner != null;
        lockOwner = null;
        return result;
    }

    @Override
    public synchronized LockOwner getLockOwner() {
        return lockOwner;
    }

    @Override
    public int getRedeliveryCounter() {
        return message.getRedeliveryCounter();
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public Message.MessageDestination getRegionDestination() {
        return message.getRegionDestination();
    }

    @Override
    public boolean isPersistent() {
        return message.isPersistent();
    }

    public synchronized boolean isLocked() {
        return lockOwner != null;
    }

    @Override
    public synchronized boolean isAcked() {
        return acked;
    }

    @Override
    public synchronized void setAcked(boolean b) {
        acked = b;
    }

    @Override
    public String getGroupID() {
        return message.getGroupID();
    }

    @Override
    public int getGroupSequence() {
        return message.getGroupSequence();
    }

    @Override
    public ConsumerId getTargetConsumerId() {
        return message.getTargetConsumerId();
    }

    @Override
    public long getExpiration() {
        return message.getExpiration();
    }

    @Override
    public boolean isExpired() {
        return message.isExpired();
    }

    @Override
    public synchronized int getSize() {
       return message.getSize();
    }

    @Override
    public boolean isAdvisory() {
       return message.isAdvisory();
    }

    @Override
    public boolean canProcessAsExpired() {
        return message.canProcessAsExpired();
    }
}
