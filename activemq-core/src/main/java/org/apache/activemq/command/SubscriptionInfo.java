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
 * Used to represent a durable subscription.
 * 
 * @openwire:marshaller code="55"
 * @version $Revision: 1.6 $
 */
public class SubscriptionInfo implements DataStructure {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.DURABLE_SUBSCRIPTION_INFO;

    protected ActiveMQDestination subscribedDestination;
    protected ActiveMQDestination destination;
    protected String clientId;
    protected String subscriptionName;
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
     * This is the a resolved destination that the subscription is receiving messages from.
     * This will never be a pattern or a composite destination.
     * 
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
     * @deprecated
     */
    public String getSubcriptionName() {
        return subscriptionName;
    }

    /**
     * @param subscriptionName
     *  * @deprecated
     */
    public void setSubcriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }
    
    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public boolean isMarshallAware() {
        return false;
    }
    
    public String toString() {
        return IntrospectionSupport.toString(this);
    }
    
    public int hashCode() {
        int h1 = clientId != null ? clientId.hashCode():-1;
        int h2 = subscriptionName != null ? subscriptionName.hashCode():-1;
        return h1 ^ h2;
    }
    
    public boolean equals(Object obj){
        boolean result=false;
        if(obj instanceof SubscriptionInfo){
            SubscriptionInfo other=(SubscriptionInfo)obj;
            result=(clientId==null&&other.clientId==null||clientId!=null&&other.clientId!=null
                    &&clientId.equals(other.clientId))
                    &&(subscriptionName==null&&other.subscriptionName==null||subscriptionName!=null
                            &&other.subscriptionName!=null&&subscriptionName.equals(other.subscriptionName));
        }
        return result;
    }

    /**
     * The destination the client originally subscribed to.. This may not match the {@see getDestination} method
     * if the subscribed destination uses patterns or composites.
     *  
     *  If the subscribed destinationis not set, this just ruturns the desitination.
     *  
     * @openwire:property version=3
     */
	public ActiveMQDestination getSubscribedDestination() {
		if( subscribedDestination == null ) {
			return getDestination();
		}
		return subscribedDestination;
	}

	public void setSubscribedDestination(ActiveMQDestination subscribedDestination) {
		this.subscribedDestination = subscribedDestination;
	}

}
