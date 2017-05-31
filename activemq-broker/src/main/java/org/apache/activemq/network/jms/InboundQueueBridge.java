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
 * Create an Inbound Queue Bridge.  By default this class uses the sname name for
 * both the inbound and outbound queue.  This behavior can be overridden however
 * by using the setter methods to configure both the inbound and outboud queue names
 * separately.
 *
 * @org.apache.xbean.XBean
 */
public class InboundQueueBridge extends QueueBridge {

    String inboundQueueName;
    String localQueueName;

    /**
     * Constructor that takes a foreign destination as an argument
     *
     * @param inboundQueueName
     */
    public InboundQueueBridge(String inboundQueueName) {
        this.inboundQueueName = inboundQueueName;
        this.localQueueName = inboundQueueName;
    }

    /**
     * Default Constructor
     */
    public InboundQueueBridge() {
    }

    /**
     * @return Returns the inboundQueueName.
     */
    public String getInboundQueueName() {
        return inboundQueueName;
    }

    /**
     * Sets the queue name used for the inbound queue, if the outbound queue
     * name has not been set, then this method uses the same name to configure
     * the outbound queue name.
     *
     * @param inboundQueueName The inboundQueueName to set.
     */
    public void setInboundQueueName(String inboundQueueName) {
        this.inboundQueueName = inboundQueueName;
        if (this.localQueueName == null) {
            this.localQueueName = inboundQueueName;
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
