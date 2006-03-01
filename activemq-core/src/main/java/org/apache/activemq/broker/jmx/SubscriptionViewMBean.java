/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.broker.jmx;

/**
 * @version $Revision: 1.5 $
 */
public interface SubscriptionViewMBean{
    /**
     * @return the id of the Connection the Subscription is on
     */
    public String getConnectionId();

    /**
     * @return the id of the Session the subscription is on
     */
    public long getSessionId();

    /**
     * @return the id of the Subscription
     */
    public long getSubcriptionId();

    /**
     * @return the destination name
     */
    public String getDestinationName();

    /**
     * @return true if the destination is a Queue
     */
    public boolean isDestinationQueue();

    /**
     * @return true of the destination is a Topic
     */
    public boolean isDestinationTopic();

    /**
     * @return true if the destination is temporary
     */
    public boolean isDestinationTemporary();

    /**
     * The subscription should release as may references as it can to help the garbage collector reclaim memory.
     */
    public void gc();

    /**
     * @return number of messages pending delivery
     */
    public int getPending();

    /**
     * @return number of messages dispatched
     */
    public int getDispatched();

    /**
     * @return number of messages delivered
     */
    public int getDelivered();
}