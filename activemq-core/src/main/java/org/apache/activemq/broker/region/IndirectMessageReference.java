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
 * @version $Revision: 1.15 $
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
    
    /**
     * @param message
     */
    public IndirectMessageReference(final Message message) {
        this.message = message;
        message.getMessageId();
        message.getGroupID();
        message.getGroupSequence();
    }

    public Message getMessageHardRef() {
        return message;
    }

    public int getReferenceCount() {
        return message.getReferenceCount();
    }

    public int incrementReferenceCount() {
        return message.incrementReferenceCount();
    }

    public int decrementReferenceCount() {
        return message.decrementReferenceCount();
    }

    public Message getMessage() {
        return message;
    }

    public String toString() {
        return "Message " + message.getMessageId() + " dropped=" + dropped + " locked=" + (lockOwner != null);
    }

    public void incrementRedeliveryCounter() {
        message.incrementRedeliveryCounter();
    }

    public synchronized boolean isDropped() {
        return dropped;
    }

    public synchronized void drop() {
        dropped = true;
        lockOwner = null;
        message.decrementReferenceCount();
    }

    public boolean lock(LockOwner subscription) {
        synchronized (this) {
            if (dropped || (lockOwner != null && lockOwner != subscription)) {
                return false;
            }
            lockOwner = subscription;
            return true;
        }
    }

    public synchronized boolean unlock() {
        boolean result = lockOwner != null;
        lockOwner = null;
        return result;
    }

    public synchronized LockOwner getLockOwner() {
        return lockOwner;
    }

    public int getRedeliveryCounter() {
        return message.getRedeliveryCounter();
    }

    public MessageId getMessageId() {
        return message.getMessageId();
    }

    public Destination getRegionDestination() {
        return message.getRegionDestination();
    }

    public boolean isPersistent() {
        return message.isPersistent();
    }

    public synchronized boolean isLocked() {
        return lockOwner != null;
    }

    public synchronized boolean isAcked() {
        return acked;
    }

    public synchronized void setAcked(boolean b) {
        acked = b;
    }

    public String getGroupID() {
        return message.getGroupID();
    }

    public int getGroupSequence() {
        return message.getGroupSequence();
    }

    public ConsumerId getTargetConsumerId() {
        return message.getTargetConsumerId();
    }

    public long getExpiration() {
        return message.getExpiration();
    }

    public boolean isExpired() {
        return message.isExpired();
    }

    public synchronized int getSize() {
       return message.getSize();
    }

    public boolean isAdvisory() {
       return message.isAdvisory();
    }
}
