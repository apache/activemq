/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
    
    public int hashCode() {
        int h1 = clientId != null ? clientId.hashCode():-1;
        int h2 = subcriptionName != null ? subcriptionName.hashCode():-1;
        return h1 ^ h2;
    }
    
    public boolean equals(Object obj){
        boolean result=false;
        if(obj instanceof SubscriptionInfo){
            SubscriptionInfo other=(SubscriptionInfo)obj;
            result=(clientId==null&&other.clientId==null||clientId!=null&&other.clientId!=null
                    &&clientId.equals(other.clientId))
                    &&(subcriptionName==null&&other.subcriptionName==null||subcriptionName!=null
                            &&other.subcriptionName!=null&&subcriptionName.equals(other.subcriptionName));
        }
        return result;
    }

}
