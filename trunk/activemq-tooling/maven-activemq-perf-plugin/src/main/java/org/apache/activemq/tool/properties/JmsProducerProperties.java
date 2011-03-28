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
package org.apache.activemq.tool.properties;

public class JmsProducerProperties extends JmsClientProperties {
    public static final String TIME_BASED_SENDING  = "time"; // Produce messages base on a time interval
    public static final String COUNT_BASED_SENDING = "count"; // Produce a specific count of messages
    public static final String DELIVERY_MODE_PERSISTENT     = "persistent"; // Persistent message delivery
    public static final String DELIVERY_MODE_NON_PERSISTENT = "nonpersistent"; // Non-persistent message delivery

    protected String deliveryMode = DELIVERY_MODE_NON_PERSISTENT; // Message delivery mode
    protected int messageSize = 1024; // Send 1kb messages by default
    protected long sendCount  = 1000000; // Send a million messages by default
    protected long sendDuration = 5 * 60 * 1000; // Send for 5 mins by default
    protected String sendType = TIME_BASED_SENDING;
    protected long sendDelay = 0;  // delay in milliseconds between each producer send
    
    // If true, create a different message on each send, otherwise reuse.
    protected boolean createNewMsg; 

    public String getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(String deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public long getSendCount() {
        return sendCount;
    }

    public void setSendCount(long sendCount) {
        this.sendCount = sendCount;
    }

    public long getSendDuration() {
        return sendDuration;
    }

    public void setSendDuration(long sendDuration) {
        this.sendDuration = sendDuration;
    }

    public String getSendType() {
        return sendType;
    }

    public void setSendType(String sendType) {
        this.sendType = sendType;
    }

    public boolean isCreateNewMsg() {
        return createNewMsg;
    }

    public void setCreateNewMsg(boolean createNewMsg) {
        this.createNewMsg = createNewMsg;
    }
    
    public void setSendDelay(long delay) {
    	this.sendDelay = delay;
    }
    
    public long getSendDelay() {
    	return this.sendDelay;
    }
}
