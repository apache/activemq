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

import org.apache.activemq.state.CommandVisitor;

/**
 * @openwire:marshaller code="22"
 * @version $Revision: 1.11 $
 */
public class MessageAck extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.MESSAGE_ACK;

    /**
     * Used to let the broker know that the message has been delivered to the
     * client. Message will still be retained until an standard ack is received.
     * This is used get the broker to send more messages past prefetch limits
     * when an standard ack has not been sent.
     */
    public static final byte DELIVERED_ACK_TYPE = 0;

    /**
     * The standard ack case where a client wants the message to be discarded.
     */
    public static final byte STANDARD_ACK_TYPE = 2;

    /**
     * In case the client want's to explicitly let the broker know that a
     * message was not processed and the message was considered a poison
     * message.
     */
    public static final byte POSION_ACK_TYPE = 1;

    /**
     * In case the client want's to explicitly let the broker know that a
     * message was not processed and it was re-delivered to the consumer
     * but it was not yet considered to be a poison message.  The messageCount 
     * field will hold the number of times the message was re-delivered. 
     */
    public static final byte REDELIVERED_ACK_TYPE = 3;
    
    protected byte ackType;
    protected ConsumerId consumerId;
    protected MessageId firstMessageId;
    protected MessageId lastMessageId;
    protected ActiveMQDestination destination;
    protected TransactionId transactionId;
    protected int messageCount;

    protected transient String consumerKey;

    public MessageAck() {
    }

    public MessageAck(MessageDispatch md, byte ackType, int messageCount) {
        this.ackType = ackType;
        this.consumerId = md.getConsumerId();
        this.destination = md.getDestination();
        this.lastMessageId = md.getMessage().getMessageId();
        this.messageCount = messageCount;
    }

    public void copy(MessageAck copy) {
        super.copy(copy);
        copy.firstMessageId = firstMessageId;
        copy.lastMessageId = lastMessageId;
        copy.destination = destination;
        copy.transactionId = transactionId;
        copy.ackType = ackType;
        copy.consumerId = consumerId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isMessageAck() {
        return true;
    }

    public boolean isPoisonAck() {
        return ackType == POSION_ACK_TYPE;
    }

    public boolean isStandardAck() {
        return ackType == STANDARD_ACK_TYPE;
    }

    public boolean isDeliveredAck() {
        return ackType == DELIVERED_ACK_TYPE;
    }
    
    public boolean isRedeliveredAck() {
        return ackType == REDELIVERED_ACK_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isInTransaction() {
        return transactionId != null;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * @openwire:property version=1
     */
    public byte getAckType() {
        return ackType;
    }

    public void setAckType(byte ackType) {
        this.ackType = ackType;
    }

    /**
     * @openwire:property version=1
     */
    public MessageId getFirstMessageId() {
        return firstMessageId;
    }

    public void setFirstMessageId(MessageId firstMessageId) {
        this.firstMessageId = firstMessageId;
    }

    /**
     * @openwire:property version=1
     */
    public MessageId getLastMessageId() {
        return lastMessageId;
    }

    public void setLastMessageId(MessageId lastMessageId) {
        this.lastMessageId = lastMessageId;
    }

    /**
     * The number of messages being acknowledged in the range.
     * 
     * @openwire:property version=1
     */
    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processMessageAck(this);
    }

    /**
     * A helper method to allow a single message ID to be acknowledged
     */
    public void setMessageID(MessageId messageID) {
        setFirstMessageId(messageID);
        setLastMessageId(messageID);
        setMessageCount(1);
    }

}
