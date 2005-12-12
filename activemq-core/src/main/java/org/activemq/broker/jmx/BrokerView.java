/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
 *
 **/
package org.activemq.broker.jmx;

import org.activemq.broker.Broker;
import org.activemq.broker.region.DestinationStatistics;
import org.activemq.memory.UsageManager;

public class BrokerView implements BrokerViewMBean {
    
    private final Broker broker;
    private final DestinationStatistics destinationStatistics;
    private final UsageManager usageManager;

    public BrokerView(Broker broker, DestinationStatistics destinationStatistics, UsageManager usageManager) {
        this.broker = broker;
        this.destinationStatistics = destinationStatistics;
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
        return destinationStatistics.getEnqueues().getCount();    
    }
    public long getTotalDequeueCount() {
        return destinationStatistics.getDequeues().getCount();
    }
    public long getTotalConsumerCount() {
        return destinationStatistics.getConsumers().getCount();
    }
    public long getTotalMessages() {
        return destinationStatistics.getMessages().getCount();
    }    
    public long getTotalMessagesCached() {
        return destinationStatistics.getMessagesCached().getCount();
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
        destinationStatistics.reset();
    }

    public void terminateJVM(int exitCode) {
        System.exit(exitCode);
    }
    
}
