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
package org.apache.activemq.network.jms;

/**
 * Create an Outbound Queue Bridge
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class OutboundQueueBridge extends QueueBridge {

    String outboundQueueName;
    String localQueueName;

    /**
     * Constructor that takes a foreign destination as an argument
     * 
     * @param outboundQueueName
     */
    public OutboundQueueBridge(String outboundQueueName) {
        this.outboundQueueName = outboundQueueName;
        this.localQueueName = outboundQueueName;
    }

    /**
     * Default Contructor
     */
    public OutboundQueueBridge() {
    }

    /**
     * @return Returns the outboundQueueName.
     */
    public String getOutboundQueueName() {
        return outboundQueueName;
    }

    /**
     * @param outboundQueueName The outboundQueueName to set.
     */
    public void setOutboundQueueName(String outboundQueueName) {
        this.outboundQueueName = outboundQueueName;
        if (this.localQueueName == null) {
            this.localQueueName = outboundQueueName;
        }
    }

    /**
     * @return the localQueueName
     */
    public String getLocalQueueName() {
        return localQueueName;
    }

    /**
     * @param localQueueName the localQueueName to set
     */
    public void setLocalQueueName(String localQueueName) {
        this.localQueueName = localQueueName;
    }

}
