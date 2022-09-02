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
package org.apache.activemq.jms2;

import javax.jms.Destination;

public class MessageData {

    private javax.jms.Message message = null;
    private String messageType = null;
    private String messagePayload = null; 
    private Long deliveryDelay = null;
    private Integer deliveryMode = null;
    private Boolean disableMessageID = null;
    private Boolean disableMessageTimestamp = null;
    private String correlationID = null;
    private Destination replyTo = null;
    private String jmsType = null;
    private Integer priority = null;
    private Long expiration = null;
    private Long timestamp = null;
    private Long deliveryTime = null;
    private String messageID = null;
    private Long timeToLive = null;

    MessageData() {}

    // builder-style setters
    public MessageData setMessage(javax.jms.Message message) { this.message = message; return this; }
    public MessageData setMessageType(String messageType) { this.messageType = messageType; return this; }
    public MessageData setMessagePayload(String messagePayload) { this.messagePayload = messagePayload; return this; } 
    public MessageData setDeliveryDelay(Long deliveryDelay) { this.deliveryDelay = deliveryDelay; return this; }
    public MessageData setDeliveryMode(Integer deliveryMode) { this.deliveryMode = deliveryMode; return this; }
    public MessageData setDisableMessageID(Boolean disableMessageID) { this.disableMessageID = disableMessageID; return this; }
    public MessageData setDisableMessageTimestamp(Boolean disableMessageTimestamp) { this.disableMessageTimestamp = disableMessageTimestamp; return this; }
    public MessageData setCorrelationID(String correlationID) { this.correlationID = correlationID; return this; }
    public MessageData setReplyTo(Destination replyTo) { this.replyTo = replyTo; return this; }
    public MessageData setJMSType(String jmsType) { this.jmsType = jmsType; return this; }
    public MessageData setPriority(Integer priority) { this.priority = priority; return this; }
    public MessageData setExpiration(Long expiration) { this.expiration = expiration; return this; }
    public MessageData setTimestamp(Long timestamp) { this.timestamp = timestamp; return this; }
    public MessageData setDeliveryTime(Long deliveryTime) { this.deliveryTime = deliveryTime; return this; }
    public MessageData setMessageID(String messageID) { this.messageID = messageID; return this; }
    public MessageData setTimeToLive(Long timeToLive) { this.timeToLive = timeToLive; return this; }

    // standard getters
    public String getMessagePayload() {
        return messagePayload;
    }

    public Long getDeliveryDelay() {
        return deliveryDelay;
    }

    public Integer getDeliveryMode() {
        return deliveryMode;
    }

    public Boolean getDisableMessageID() {
        return disableMessageID;
    }

    public Boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    public String getCorrelationID() {
        return correlationID;
    }

    public Destination getReplyTo() {
        return replyTo;
    }

    public String getJmsType() {
        return jmsType;
    }

    public Integer getPriority() {
        return priority;
    }

    public Long getExpiration() {
        return expiration;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getDeliveryTime() {
        return deliveryTime;
    }

    public String getMessageID() {
        return messageID;
    }

    public javax.jms.Message getMessage() {
        return message;
    }

    public String getMessageType() {
        return messageType;
    }

    public Long getTimeToLive() {
        return timeToLive;
    }
}
