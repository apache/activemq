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

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

/**
 * Used by the {@link QueueMessageReference#NULL_MESSAGE}
 */
public final class NullMessageReference implements QueueMessageReference {

    private final ActiveMQMessage message = new ActiveMQMessage();
    private volatile int references;

    @Override
    public void drop() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public synchronized boolean dropIfLive() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public LockOwner getLockOwner() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isAcked() {
        return false;
    }

    @Override
    public boolean isDropped() {
        return false;
    }

    @Override
    public boolean lock(LockOwner subscription) {
        return true;
    }

    @Override
    public void setAcked(boolean b) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean unlock() {
        return true;
    }

    @Override
    public int decrementReferenceCount() {
        return --references;
    }

    @Override
    public long getExpiration() {
        throw new RuntimeException("not implemented");
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
    public Message getMessage()  {
        return message;
    }

    @Override
    public Message getMessageHardRef() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public MessageId getMessageId() {
        return message.getMessageId();
    }

    @Override
    public int getRedeliveryCounter() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int getReferenceCount() {
        return references;
    }

    @Override
    public Destination getRegionDestination() {
        return null;
    }

    @Override
    public int getSize() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public ConsumerId getTargetConsumerId() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void incrementRedeliveryCounter() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int incrementReferenceCount() {
        return ++references;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public boolean isPersistent() {
        throw new RuntimeException("not implemented");
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
