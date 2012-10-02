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
 * Create an Outbound Topic Bridge.  By default the bridge uses the same
 * name for both the inbound and outbound topics, however this can be altered
 * by using the public setter methods to configure both inbound and outbound
 * topic names.
 *
 * @org.apache.xbean.XBean
 */
public class OutboundTopicBridge extends TopicBridge {

    String outboundTopicName;
    String localTopicName;

    /**
     * Constructor that takes a foreign destination as an argument
     *
     * @param outboundTopicName
     */
    public OutboundTopicBridge(String outboundTopicName) {
        this.outboundTopicName = outboundTopicName;
        this.localTopicName = outboundTopicName;
    }

    /**
     * Default Contructor
     */
    public OutboundTopicBridge() {
    }

    /**
     * @return Returns the outboundTopicName.
     */
    public String getOutboundTopicName() {
        return outboundTopicName;
    }

    /**
     * Sets the name of the outbound topic name.  If the inbound topic name
     * has not been set already then this method uses the provided topic name
     * to set the inbound topic name as well.
     *
     * @param outboundTopicName The outboundTopicName to set.
     */
    public void setOutboundTopicName(String outboundTopicName) {
        this.outboundTopicName = outboundTopicName;
        if (this.localTopicName == null) {
            this.localTopicName = outboundTopicName;
        }
    }

    /**
     * @return the localTopicName
     */
    public String getLocalTopicName() {
        return localTopicName;
    }

    /**
     * @param localTopicName the localTopicName to set
     */
    public void setLocalTopicName(String localTopicName) {
        this.localTopicName = localTopicName;
    }

}
