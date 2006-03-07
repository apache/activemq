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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;



/**
 * @version $Revision: 1.5 $
 */
public class SubscriptionView implements SubscriptionViewMBean {
    
    
    protected final Subscription subscription;
    protected final String clientId;
    
    
    /**
     * Constructor
     * @param subs
     */
    public SubscriptionView(String clientId,Subscription subs){
        this.clientId = clientId;
        this.subscription = subs;
    }
    
    /**
     * @return the clientId
     */
    public String getClientId(){
        return clientId;
    }
    
    /**
     * @return the id of the Connection the Subscription is on
     */
    public String getConnectionId(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            return info.getConsumerId().getConnectionId();
        }
        return "NOTSET";
    }

    /**
     * @return the id of the Session the subscription is on
     */
    public long getSessionId(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            return info.getConsumerId().getSessionId();
        }
        return 0;
    }

    /**
     * @return the id of the Subscription
     */
    public long getSubcriptionId(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            return info.getConsumerId().getValue();
        }
        return 0;
    }

    /**
     * @return the destination name
     */
    public String getDestinationName(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            ActiveMQDestination dest = info.getDestination();
            return dest.getPhysicalName();
        }
        return "NOTSET";
       
    }

    /**
     * @return true if the destination is a Queue
     */
    public boolean isDestinationQueue(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            ActiveMQDestination dest = info.getDestination();
            return dest.isQueue();
        }
        return false;
    }

    /**
     * @return true of the destination is a Topic
     */
    public boolean isDestinationTopic(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            ActiveMQDestination dest = info.getDestination();
            return dest.isTopic();
        }
        return false;
    }

    /**
     * @return true if the destination is temporary
     */
    public boolean isDestinationTemporary(){
        ConsumerInfo info = getConsumerInfo();
        if (info != null){
            ActiveMQDestination dest = info.getDestination();
            return dest.isTemporary();
        }
        return false;
    }
    
    /**
     * @return true if the subscriber is active
     */
    public boolean isActive(){
        return true;
    }

    /**
     * The subscription should release as may references as it can to help the garbage collector
     * reclaim memory.
     */
    public void gc(){
        if (subscription != null){
        subscription.gc();
        }
    }
    
    /**
     * @return number of messages pending delivery
     */
    public int getPendingQueueSize(){
        return subscription != null ? subscription.getPendingQueueSize() : 0;
    }
    
    /**
     * @return number of messages dispatched
     */
    public int getDispatchedQueueSize(){
        return subscription != null ? subscription.getDispatchedQueueSize() : 0;
    }
        
    /**
     * @return number of messages that matched the subscription
     */
    public long getDispachedCounter() {
        return subscription != null ? subscription.getDispatchedCounter() : 0;
    }

    /**
     * @return number of messages that matched the subscription
     */
    public long getEnqueueCounter() {
        return subscription != null ? subscription.getEnqueueCounter() : 0;
    }

    /**
     * @return number of messages queued by the client
     */
    public long getDequeueCounter() {
        return subscription != null ? subscription.getDequeueCounter() : 0;
    }

    protected ConsumerInfo getConsumerInfo(){
        return subscription != null ? subscription.getConsumerInfo() : null;
    }
    
    /**
     *@return pretty print
     */
    public String toString(){
        return "SubscriptionView: " + getClientId()  + ":" +  getConnectionId();
    }

}