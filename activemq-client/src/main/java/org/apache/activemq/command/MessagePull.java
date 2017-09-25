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
 * Used to pull messages on demand.
 *
 * @openwire:marshaller code="20"
 *
 *
 */
public class MessagePull extends BaseCommand implements TransientInitializer {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.MESSAGE_PULL;

    protected ConsumerId consumerId;
    protected ActiveMQDestination destination;
    protected long timeout;
    private MessageId messageId;
    private String correlationId;

    private transient int quantity = 1;
    private transient boolean alwaysSignalDone;
    private transient boolean tracked = false;

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processMessagePull(this);
    }

    /**
     * Configures a message pull from the consumer information
     */
    public void configure(ConsumerInfo info) {
        setConsumerId(info.getConsumerId());
        setDestination(info.getDestination());
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
     * @openwire:property version=1 cache=true
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
    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * An optional correlation ID which could be used by a broker to decide which messages are pulled
     * on demand from a queue for a consumer
     *
     * @openwire:property version=3
     */
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }


    /**
     * An optional message ID which could be used by a broker to decide which messages are pulled
     * on demand from a queue for a consumer
     *
     * @openwire:property version=3
     */
    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public void setTracked(boolean tracked) {
        this.tracked = tracked;
    }

    public boolean isTracked() {
        return this.tracked;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public boolean isAlwaysSignalDone() {
        return alwaysSignalDone;
    }

    public void setAlwaysSignalDone(boolean alwaysSignalDone) {
        this.alwaysSignalDone = alwaysSignalDone;
    }

    @Override
    public void initTransients() {
        quantity = 1;
        alwaysSignalDone = false;
        tracked = false;
    }
}
