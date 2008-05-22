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

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

/**
 * Only used by the {@link QueueMessageReference#NULL_MESSAGE}
 */
final class NullMessageReference implements QueueMessageReference {

    private ActiveMQMessage message = new ActiveMQMessage();
    private volatile int references;

    public void drop() {
        throw new RuntimeException("not implemented");
    }

    public LockOwner getLockOwner() {
        throw new RuntimeException("not implemented");
    }

    public boolean isAcked() {
        return false;
    }

    public boolean isDropped() {
        return false;
    }

    public boolean lock(LockOwner subscription) {
        return true;
    }

    public void setAcked(boolean b) {
        throw new RuntimeException("not implemented");
    }

    public boolean unlock() {
        return true;
    }

    public int decrementReferenceCount() {
        return --references;
    }

    public long getExpiration() {
        throw new RuntimeException("not implemented");
    }

    public String getGroupID() {
        return null;
    }

    public int getGroupSequence() {
        return 0;
    }

    public Message getMessage() throws IOException {
        return message;
    }

    public Message getMessageHardRef() {
        throw new RuntimeException("not implemented");
    }

    public MessageId getMessageId() {
        return message.getMessageId();
    }

    public int getRedeliveryCounter() {
        throw new RuntimeException("not implemented");
    }

    public int getReferenceCount() {
        return references;
    }

    public Destination getRegionDestination() {
        return null;
    }

    public int getSize() {
        throw new RuntimeException("not implemented");
    }

    public ConsumerId getTargetConsumerId() {
        throw new RuntimeException("not implemented");
    }

    public void incrementRedeliveryCounter() {
        throw new RuntimeException("not implemented");
    }

    public int incrementReferenceCount() {
        return ++references;
    }

    public boolean isExpired() {
        throw new RuntimeException("not implemented");
    }

    public boolean isPersistent() {
        throw new RuntimeException("not implemented");
    }

    public boolean isAdvisory() {
        return false;
    }

}
