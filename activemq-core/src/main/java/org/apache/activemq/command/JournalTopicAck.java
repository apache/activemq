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
package org.apache.activemq.command;

import org.apache.activemq.util.IntrospectionSupport;

/**
 * @openwire:marshaller code="50"
 * @version $Revision$
 */
public class JournalTopicAck implements DataStructure {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.JOURNAL_ACK;

    ActiveMQDestination destination;
    String clientId;
    String subscritionName;
    MessageId messageId;
    long messageSequenceId;
    TransactionId transactionId;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * @openwire:property version=1
     */
    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    /**
     * @openwire:property version=1
     */
    public long getMessageSequenceId() {
        return messageSequenceId;
    }

    public void setMessageSequenceId(long messageSequenceId) {
        this.messageSequenceId = messageSequenceId;
    }

    /**
     * @openwire:property version=1
     */
    public String getSubscritionName() {
        return subscritionName;
    }

    public void setSubscritionName(String subscritionName) {
        this.subscritionName = subscritionName;
    }

    /**
     * @openwire:property version=1
     */
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * @openwire:property version=1
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transaction) {
        this.transactionId = transaction;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public String toString() {
        return IntrospectionSupport.toString(this, JournalTopicAck.class);
    }
}
