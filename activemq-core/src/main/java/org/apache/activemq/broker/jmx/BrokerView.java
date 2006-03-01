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

import javax.management.ObjectName;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.memory.UsageManager;

public class BrokerView implements BrokerViewMBean {
    
    private final ManagedRegionBroker broker;
    private final UsageManager usageManager;

    public BrokerView(ManagedRegionBroker broker, UsageManager usageManager) {
        this.broker = broker;
        this.usageManager = usageManager;        
    }
    
    public String getBrokerId() {
        return broker.getBrokerId().toString();
    }
    
    public void gc() {
        broker.gc();
    }

    public void start() throws Exception {
        broker.start();
    }
    
    public void stop() throws Exception {
        broker.stop();
    }
    
    public long getTotalEnqueueCount() {
        return broker.getDestinationStatistics().getEnqueues().getCount();    
    }
    public long getTotalDequeueCount() {
        return broker.getDestinationStatistics().getDequeues().getCount();
    }
    public long getTotalConsumerCount() {
        return broker.getDestinationStatistics().getConsumers().getCount();
    }
    public long getTotalMessages() {
        return broker.getDestinationStatistics().getMessages().getCount();
    }    
    public long getTotalMessagesCached() {
        return broker.getDestinationStatistics().getMessagesCached().getCount();
    }

    public int getMemoryPercentageUsed() {
        return usageManager.getPercentUsage();
    }
    public long getMemoryLimit() {
        return usageManager.getLimit();
    }
    public void setMemoryLimit(long limit) {
        usageManager.setLimit(limit);
    }
    
    public void resetStatistics() {
        broker.getDestinationStatistics().reset();
    }

    public void terminateJVM(int exitCode) {
        System.exit(exitCode);
    }

    public ObjectName[] getTopics(){
        return broker.getTopics();
    }

    public ObjectName[] getQueues(){
        return broker.getQueues();
    }

    public ObjectName[] getTemporaryTopics(){
        return broker.getTemporaryTopics();
    }

    public ObjectName[] getTemporaryQueues(){
        return broker.getTemporaryQueues();
    }

    public ObjectName[] getTopicSubscribers(){
      return broker.getTemporaryTopicSubscribers();
    }

    public ObjectName[] getDurableTopicSubscribers(){
        return broker.getDurableTopicSubscribers();
    }

    public ObjectName[] getQueueSubscribers(){
       return broker.getQueueSubscribers();
    }

    public ObjectName[] getTemporaryTopicSubscribers(){
        return broker.getTemporaryTopicSubscribers();
    }

    public ObjectName[] getTemporaryQueueSubscribers(){
        return broker.getTemporaryQueueSubscribers();
    }
    
}
