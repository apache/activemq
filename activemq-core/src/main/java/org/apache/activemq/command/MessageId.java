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

/**
 * @openwire:marshaller code="110"
 * @version $Revision: 1.12 $
 */
public class MessageId implements DataStructure, Comparable<MessageId> {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.MESSAGE_ID;

    protected ProducerId producerId;
    protected long producerSequenceId;
    protected long brokerSequenceId;

    private transient String key;
    private transient int hashCode;

    public MessageId() {
        this.producerId = new ProducerId();
    }

    public MessageId(ProducerInfo producerInfo, long producerSequenceId) {
        this.producerId = producerInfo.getProducerId();
        this.producerSequenceId = producerSequenceId;
    }

    public MessageId(String messageKey) {
        setValue(messageKey);
    }

    public MessageId(String producerId, long producerSequenceId) {
        this(new ProducerId(producerId), producerSequenceId);
    }

    public MessageId(ProducerId producerId, long producerSequenceId) {
        this.producerId = producerId;
        this.producerSequenceId = producerSequenceId;
    }

    /**
     * Sets the value as a String
     */
    public void setValue(String messageKey) {
        key = messageKey;
        // Parse off the sequenceId
        int p = messageKey.lastIndexOf(":");
        if (p >= 0) {
            producerSequenceId = Long.parseLong(messageKey.substring(p + 1));
            messageKey = messageKey.substring(0, p);
        }
        producerId = new ProducerId(messageKey);
    }

    /**
     * Sets the transient text view of the message which will be ignored if the
     * message is marshaled on a transport; so is only for in-JVM changes to
     * accommodate foreign JMS message IDs
     */
    public void setTextView(String key) {
        this.key = key;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        MessageId id = (MessageId)o;
        return producerSequenceId == id.producerSequenceId && producerId.equals(id.producerId);
    }

    public int hashCode() {
        if (hashCode == 0) {
            hashCode = producerId.hashCode() ^ (int)producerSequenceId;
        }
        return hashCode;
    }

    public String toString() {
        if (key == null) {
            key = producerId.toString() + ":" + producerSequenceId;
        }
        return key;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    /**
     * @openwire:property version=1
     */
    public long getProducerSequenceId() {
        return producerSequenceId;
    }

    public void setProducerSequenceId(long producerSequenceId) {
        this.producerSequenceId = producerSequenceId;
    }

    /**
     * @openwire:property version=1
     */
    public long getBrokerSequenceId() {
        return brokerSequenceId;
    }

    public void setBrokerSequenceId(long brokerSequenceId) {
        this.brokerSequenceId = brokerSequenceId;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public MessageId copy() {
        MessageId copy = new MessageId(producerId, producerSequenceId);
        copy.key = key;
        copy.brokerSequenceId = brokerSequenceId;
        return copy;
    }

    /**
     * @param o
     * @return
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(MessageId other) {
        int result = -1;
        if (other != null) {
            result = this.toString().compareTo(other.toString());
        }
        return result;
    }
}
