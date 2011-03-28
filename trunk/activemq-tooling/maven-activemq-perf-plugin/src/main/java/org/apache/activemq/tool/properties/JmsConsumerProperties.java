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

public class JmsConsumerProperties extends JmsClientProperties {
    public static final String TIME_BASED_RECEIVING = "time"; // Receive messages on a time-based interval
    public static final String COUNT_BASED_RECEIVING = "count"; // Receive a specific count of messages

    protected boolean durable; // Consumer is a durable subscriber
    protected boolean unsubscribe = true; // If true, unsubscribe a durable subscriber after it finishes running
    protected boolean asyncRecv = true;  // If true, use onMessage() to receive messages, else use receive()

    protected long recvCount    = 1000000;       // Receive a million messages by default
    protected long recvDuration = 5 * 60 * 1000; // Receive for 5 mins by default
    protected long recvDelay = 0; // delay in milliseconds for processing received msg 
    protected String recvType   = TIME_BASED_RECEIVING;

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isUnsubscribe() {
        return unsubscribe;
    }

    public void setUnsubscribe(boolean unsubscribe) {
        this.unsubscribe = unsubscribe;
    }

    public boolean isAsyncRecv() {
        return asyncRecv;
    }

    public void setAsyncRecv(boolean asyncRecv) {
        this.asyncRecv = asyncRecv;
    }

    public long getRecvCount() {
        return recvCount;
    }

    public void setRecvCount(long recvCount) {
        this.recvCount = recvCount;
    }

    public long getRecvDuration() {
        return recvDuration;
    }

    public void setRecvDuration(long recvDuration) {
        this.recvDuration = recvDuration;
    }

    public String getRecvType() {
        return recvType;
    }

    public void setRecvType(String recvType) {
        this.recvType = recvType;
    }
    
    public void setRecvDelay(long delay) {
    	this.recvDelay = delay;
    }
    
    public long getRecvDelay() {
    	return this.recvDelay;
    }
}
