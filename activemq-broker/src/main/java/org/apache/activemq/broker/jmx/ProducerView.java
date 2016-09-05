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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ProducerInfo;

public class ProducerView implements ProducerViewMBean {

    protected final ProducerInfo info;
    protected final String clientId;
    protected final String userName;
    protected final ManagedRegionBroker broker;

    protected ActiveMQDestination lastUsedDestination;

    public ProducerView(ProducerInfo info, String clientId, String userName, ManagedRegionBroker broker) {
        this.info = info;
        this.clientId = clientId;
        this.userName = userName;
        this.broker = broker;
    }

    @Override
    public String getClientId() {
        return this.clientId;
    }

    @Override
    public String getConnectionId() {
        if (info != null) {
            return info.getProducerId().getConnectionId();
        }
        return "NOTSET";
    }

    @Override
    public long getSessionId() {
        if (info != null) {
            return info.getProducerId().getSessionId();
        }
        return 0;
    }

    @Override
    public String getProducerId() {
        if (info != null) {
            return info.getProducerId().toString();
        }
        return "NOTSET";
    }

    @Override
    public String getDestinationName() {
        if (info != null && info.getDestination() != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.getPhysicalName();
        } else if (this.lastUsedDestination != null) {
            return this.lastUsedDestination.getPhysicalName();
        }
        return "NOTSET";
    }

    @Override
    public boolean isDestinationQueue() {
        if (info != null) {
            if (info.getDestination() != null) {
                ActiveMQDestination dest = info.getDestination();
                return dest.isQueue();
            } else if(lastUsedDestination != null) {
                return lastUsedDestination.isQueue();
            }
        }
        return false;
    }

    @Override
    public boolean isDestinationTopic() {
        if (info != null) {
            if (info.getDestination() != null) {
                ActiveMQDestination dest = info.getDestination();
                return dest.isTopic();
            } else if(lastUsedDestination != null) {
                return lastUsedDestination.isTopic();
            }
        }
        return false;
    }

    @Override
    public boolean isDestinationTemporary() {
        if (info != null) {
            if (info.getDestination() != null) {
                ActiveMQDestination dest = info.getDestination();
                return dest.isTemporary();
            } else if(lastUsedDestination != null) {
                return lastUsedDestination.isTemporary();
            }
        }
        return false;
    }

    @Override
    public int getProducerWindowSize() {
        if (info != null) {
            return info.getWindowSize();
        }
        return 0;
    }

    @Override
    @Deprecated
    public boolean isDispatchAsync() {
        return false;
    }

    /**
     * @return pretty print
     */
    @Override
    public String toString() {
        return "ProducerView: " + getClientId() + ":" + getConnectionId();
    }

    /**
     * Set the last used Destination name for a Dynamic Destination Producer.
     */
    void setLastUsedDestinationName(ActiveMQDestination destinationName) {
        this.lastUsedDestination = destinationName;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public boolean isProducerBlocked() {
        ProducerBrokerExchange producerBrokerExchange = broker.getBrokerService().getProducerBrokerExchange(info);
        if (producerBrokerExchange != null){
            return producerBrokerExchange.isBlockedForFlowControl();
        }
        return false;
    }

    @Override
    public long getTotalTimeBlocked() {
        ProducerBrokerExchange producerBrokerExchange = broker.getBrokerService().getProducerBrokerExchange(info);
        if (producerBrokerExchange != null){
            return producerBrokerExchange.getTotalTimeBlocked();
        }
        return 0;
    }

    @Override
    public int getPercentageBlocked() {
        ProducerBrokerExchange producerBrokerExchange = broker.getBrokerService().getProducerBrokerExchange(info);
        if (producerBrokerExchange != null){
            return producerBrokerExchange.getPercentageBlocked();
        }
        return 0;
    }

    @Override
    public void resetFlowControlStats() {
        ProducerBrokerExchange producerBrokerExchange = broker.getBrokerService().getProducerBrokerExchange(info);
        if (producerBrokerExchange != null){
            producerBrokerExchange.resetFlowControl();
        }
    }

    @Override
    public void resetStatistics() {
       if (info != null){
           info.resetSentCount();
       }
    }

    @Override
    public long getSentCount() {
        return info != null ? info.getSentCount() :0;
    }
}
