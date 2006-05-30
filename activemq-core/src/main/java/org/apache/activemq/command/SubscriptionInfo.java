/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.command;

import org.apache.activemq.util.IntrospectionSupport;


/**
 * 
 * @openwire:marshaller code="55"
 * @version $Revision: 1.6 $
 */
public class SubscriptionInfo implements DataStructure {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.DURABLE_SUBSCRIPTION_INFO;

    protected ActiveMQDestination destination;
    protected String clientId;
    protected String subcriptionName;
    protected String selector;
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1
     */
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
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
    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    /**
     * @openwire:property version=1
     */
    public String getSubcriptionName() {
        return subcriptionName;
    }

    public void setSubcriptionName(String subcriptionName) {
        this.subcriptionName = subcriptionName;
    }

    public boolean isMarshallAware() {
        return false;
    }
    
    public String toString() {
        return IntrospectionSupport.toString(this);
    }

}
