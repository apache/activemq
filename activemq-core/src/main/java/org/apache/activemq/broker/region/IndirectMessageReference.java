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

import java.io.IOException;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;

/**
 * Keeps track of a message that is flowing through the Broker. This object may
 * hold a hard reference to the message or only hold the id of the message if
 * the message has been persisted on in a MessageStore.
 * 
 * @version $Revision: 1.15 $
 */
public class IndirectMessageReference implements QueueMessageReference {

    /** The destination that is managing the message */
    private final Destination regionDestination;

    private final MessageStore destinationStore;

    /** The id of the message is always valid */
    private final MessageId messageId;
    /** Is the message persistent? */
    private final boolean persistent;
    private final String groupID;
    private final int groupSequence;
    private final ConsumerId targetConsumerId;

    /** The number of times the message has been delivered. */
    private short redeliveryCounter;
    /** The subscription that has locked the message */
    private LockOwner lockOwner;
    /** Has the message been dropped? */
    private boolean dropped;
    /** Has the message been acked? */
    private boolean acked;
    /** Direct reference to the message */
    private Message message;
    /** The number of times the message has requested being hardened */
    private int referenceCount;
    /** the size of the message * */
    private int cachedSize;
    /** the expiration time of the message */
    private long expiration;

    public IndirectMessageReference(Queue destination, MessageStore destinationStore, Message message) {
        this.regionDestination = destination;
        this.destinationStore = destinationStore;
        this.message = message;
        this.messageId = message.getMessageId();
        this.persistent = message.isPersistent() && destination.getMessageStore() != null;
        this.groupID = message.getGroupID();
        this.groupSequence = message.getGroupSequence();
        this.targetConsumerId = message.getTargetConsumerId();
        this.expiration = message.getExpiration();

        this.referenceCount = 1;
        message.incrementReferenceCount();
        this.cachedSize = message.getSize();
    }

    synchronized public Message getMessageHardRef() {
        return message;
    }

    synchronized public int getReferenceCount() {
        return referenceCount;
    }

    synchronized public int incrementReferenceCount() {
        int rc = ++referenceCount;
        if (persistent && rc == 1 && message == null) {

            try {
                message = destinationStore.getMessage(messageId);
                if (message == null) {
                    dropped = true;
                } else {
                    message.setRegionDestination(regionDestination);
                    message.incrementReferenceCount();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return rc;
    }

    synchronized public int decrementReferenceCount() {
        int rc = --referenceCount;
        if (persistent && rc == 0 && message != null) {
            message.decrementReferenceCount();
            // message=null;
        }
        return rc;
    }

    synchronized public Message getMessage() {
        return message;
    }

    public String toString() {
        return "Message " + messageId + " dropped=" + dropped + " locked=" + (lockOwner != null);
    }

    synchronized public void incrementRedeliveryCounter() {
        this.redeliveryCounter++;
    }

    synchronized public boolean isDropped() {
        return dropped;
    }

    synchronized public void drop() {
        dropped = true;
        lockOwner = null;
        if (!persistent && message != null) {
            message.decrementReferenceCount();
            message = null;
        }
    }

    public boolean lock(LockOwner subscription) {
        if (!regionDestination.lock(this, subscription))
            return false;
        synchronized (this) {
            if (dropped || (lockOwner != null && lockOwner != subscription))
                return false;
            lockOwner = subscription;
            return true;
        }
    }

    synchronized public void unlock() {
        lockOwner = null;
    }

    synchronized public LockOwner getLockOwner() {
        return lockOwner;
    }

    synchronized public int getRedeliveryCounter() {
        return redeliveryCounter;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public Destination getRegionDestination() {
        return regionDestination;
    }

    public boolean isPersistent() {
        return persistent;
    }

    synchronized public boolean isLocked() {
        return lockOwner != null;
    }

    synchronized public boolean isAcked() {
        return acked;
    }

    synchronized public void setAcked(boolean b) {
        acked = b;
    }

    public String getGroupID() {
        return groupID;
    }

    public int getGroupSequence() {
        return groupSequence;
    }

    public ConsumerId getTargetConsumerId() {
        return targetConsumerId;
    }

    public long getExpiration() {
        return expiration;
    }

    public boolean isExpired() {
        long expireTime = getExpiration();
        if (expireTime > 0 && System.currentTimeMillis() > expireTime) {
            return true;
        }
        return false;
    }

    public synchronized int getSize() {
        Message msg = message;
        if (msg != null) {
            return msg.getSize();
        }
        return cachedSize;
    }
}
