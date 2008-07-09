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
package org.apache.activemq.network;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;

/**
 * Represents a network bridge interface
 * 
 * @version $Revision: 1.1 $
 */
public class DemandSubscription {
    private final ConsumerInfo remoteInfo;
    private final ConsumerInfo localInfo;
    private Set<ConsumerId> remoteSubsIds = new CopyOnWriteArraySet<ConsumerId>();
    private AtomicInteger dispatched = new AtomicInteger(0);

    DemandSubscription(ConsumerInfo info) {
        remoteInfo = info;
        localInfo = info.copy();
        localInfo.setSelector(null);
        localInfo.setBrokerPath(info.getBrokerPath());
        localInfo.setNetworkSubscription(true);
        remoteSubsIds.add(info.getConsumerId());    
     }

    /**
     * Increment the consumers associated with this subscription
     * 
     * @param id
     * @return true if added
     */
    public boolean add(ConsumerId id) {   
        if (localInfo != null) {
            localInfo.addNetworkConsumerId(id);
        }
        return remoteSubsIds.add(id);
    }

    /**
     * Increment the consumers associated with this subscription
     * 
     * @param id
     * @return true if removed
     */
    public boolean remove(ConsumerId id) {
        if (localInfo != null) {
            localInfo.removeNetworkConsumerId(id);
        }
        return remoteSubsIds.remove(id);
    }

    /**
     * @return true if there are no interested consumers
     */
    public boolean isEmpty() {
        return remoteSubsIds.isEmpty();
    }

    /**
     * @return Returns the dispatched.
     */
    public int getDispatched() {
        return dispatched.get();
    }

    /**
     * @param dispatched The dispatched to set.
     */
    public void setDispatched(int dispatched) {
        this.dispatched.set(dispatched);
    }

    /**
     * @return dispatched count after incremented
     */
    public int incrementDispatched() {
        return dispatched.incrementAndGet();
    }

    /**
     * @return Returns the localInfo.
     */
    public ConsumerInfo getLocalInfo() {
        return localInfo;
    }

    
    /**
     * @return Returns the remoteInfo.
     */
    public ConsumerInfo getRemoteInfo() {
        return remoteInfo;
    }    
}
