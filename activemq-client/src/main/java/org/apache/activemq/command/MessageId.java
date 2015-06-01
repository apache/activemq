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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @openwire:marshaller code="110"
 *
 */
public class MessageId implements DataStructure, Comparable<MessageId> {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.MESSAGE_ID;

    protected String textView;
    protected ProducerId producerId;
    protected long producerSequenceId;
    protected long brokerSequenceId;

    private transient String key;
    private transient int hashCode;

    private transient AtomicReference<Object> dataLocator = new AtomicReference<Object>();
    private transient Object entryLocator;
    private transient Object plistLocator;
    private transient Object futureOrSequenceLong;

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
        } else {
            throw new NumberFormatException();
        }
        producerId = new ProducerId(messageKey);
    }

    /**
     * Sets the transient text view of the message which will be ignored if the message is marshaled on a transport; so
     * is only for in-JVM changes to accommodate foreign JMS message IDs
     */
    public void setTextView(String key) {
        this.textView = key;
    }

    /**
     * @openwire:property version=10
     * @return
     */
    public String getTextView() {
        return textView;
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        MessageId id = (MessageId) o;
        return producerSequenceId == id.producerSequenceId && producerId.equals(id.producerId);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = producerId.hashCode() ^ (int) producerSequenceId;
        }
        return hashCode;
    }

    public String toProducerKey() {
        if (textView == null) {
            return toString();
        } else {
            return producerId.toString() + ":" + producerSequenceId;
        }
    }

    @Override
    public String toString() {
        if (key == null) {
            if (textView != null) {
                if (textView.startsWith("ID:")) {
                    key = textView;
                } else {
                    key = "ID:" + textView;
                }
            } else {
                key = producerId.toString() + ":" + producerSequenceId;
            }
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

    @Override
    public boolean isMarshallAware() {
        return false;
    }

    public MessageId copy() {
        MessageId copy = new MessageId(producerId, producerSequenceId);
        copy.key = key;
        copy.brokerSequenceId = brokerSequenceId;
        copy.dataLocator = dataLocator;
        copy.entryLocator = entryLocator;
        copy.futureOrSequenceLong = futureOrSequenceLong;
        copy.plistLocator = plistLocator;
        copy.textView = textView;
        return copy;
    }

    /**
     * @param
     * @return
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(MessageId other) {
        int result = -1;
        if (other != null) {
            result = this.toString().compareTo(other.toString());
        }
        return result;
    }

    /**
     * @return a locator which aids a message store in loading a message faster. Only used by the message stores.
     */
    public Object getDataLocator() {
        return dataLocator.get();
    }

    /**
     * Sets a locator which aids a message store in loading a message faster. Only used by the message stores.
     */
    public void setDataLocator(Object value) {
        this.dataLocator.set(value);
    }

    public Object getFutureOrSequenceLong() {
        return futureOrSequenceLong;
    }

    public void setFutureOrSequenceLong(Object futureOrSequenceLong) {
        this.futureOrSequenceLong = futureOrSequenceLong;
    }

    public Object getEntryLocator() {
        return entryLocator;
    }

    public void setEntryLocator(Object entryLocator) {
        this.entryLocator = entryLocator;
    }

    public Object getPlistLocator() {
        return plistLocator;
    }

    public void setPlistLocator(Object plistLocator) {
        this.plistLocator = plistLocator;
    }

    private Object readResolve() {
        dataLocator = new AtomicReference<Object>();
        return this;
    }
}
