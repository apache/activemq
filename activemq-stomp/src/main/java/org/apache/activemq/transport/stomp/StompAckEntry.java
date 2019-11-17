/*
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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;

/**
 * Tracker object for Messages that carry STOMP v1.2 ACK IDs
 */
public class StompAckEntry {

    private final String ackId;
    private final MessageId messageId;
    private final StompSubscription subscription;
    private final MessageDispatch dispatch;

    public StompAckEntry(MessageDispatch dispatch, String ackId, StompSubscription subscription) {
        this.messageId = dispatch.getMessage().getMessageId();
        this.subscription = subscription;
        this.ackId = ackId;
        this.dispatch = dispatch;
    }

    public MessageAck onMessageAck(TransactionId transactionId) {
        return subscription.onStompMessageAck(messageId.toString(), transactionId);
    }

    public MessageAck onMessageNack(TransactionId transactionId) throws ProtocolException {
        return subscription.onStompMessageNack(messageId.toString(), transactionId);
    }

    public MessageId getMessageId() {
        return this.messageId;
    }

    public MessageDispatch getMessageDispatch() {
        return this.dispatch;
    }

    public String getAckId() {
        return this.ackId;
    }

    public StompSubscription getSubscription() {
        return this.subscription;
    }

    @Override
    public String toString() {
        return "AckEntry[ msgId:" + messageId + ", ackId:" + ackId + ", sub:" + subscription + " ]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        StompAckEntry other = (StompAckEntry) obj;
        if (messageId == null) {
            if (other.messageId != null) {
                return false;
            }
        } else if (!messageId.equals(other.messageId)) {
            return false;
        }

        return true;
    }
}
