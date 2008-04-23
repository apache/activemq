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
package org.apache.activemq.broker.region;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;


/**
 * @version $Revision: 1.12 $
 */
public abstract class BaseDestination implements Destination {
    public static final int DEFAULT_PAGE_SIZE=100;
    protected final ActiveMQDestination destination;
    protected final Broker broker;
    protected final MessageStore store;
    protected SystemUsage systemUsage;
    protected MemoryUsage memoryUsage;
    private boolean producerFlowControl = true;
    private int maxProducersToAudit=1024;
    private int maxAuditDepth=2048;
    private boolean enableAudit=true;
    private int maxPageSize=DEFAULT_PAGE_SIZE;
    private boolean useCache=true;
    private int minimumMessageSize=1024;
    private boolean lazyDispatch=false;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();
    protected final BrokerService brokerService;
    protected final Broker regionBroker;
    
    /**
     * @param broker 
     * @param store 
     * @param destination
     * @param parentStats
     * @throws Exception 
     */
    public BaseDestination(BrokerService brokerService,MessageStore store, ActiveMQDestination destination, DestinationStatistics parentStats) throws Exception {
        this.brokerService = brokerService;
        this.broker=brokerService.getBroker();
        this.store=store;
        this.destination=destination;
        // let's copy the enabled property from the parent DestinationStatistics
        this.destinationStatistics.setEnabled(parentStats.isEnabled());
        this.destinationStatistics.setParent(parentStats);        

        this.systemUsage = brokerService.getProducerSystemUsage();
        this.memoryUsage = new MemoryUsage(systemUsage.getMemoryUsage(), destination.toString());
        this.memoryUsage.setUsagePortion(1.0f);
        this.regionBroker = brokerService.getRegionBroker();
    }
    
    /**
     * initialize the destination
     * @throws Exception
     */
    public void initialize() throws Exception {
        // Let the store know what usage manager we are using so that he can
        // flush messages to disk when usage gets high.
        if (store != null) {
            store.setMemoryUsage(this.memoryUsage);
        } 
    }
    
    /**
     * @return the producerFlowControl
     */
    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }
    /**
     * @param producerFlowControl the producerFlowControl to set
     */
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }
    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }
    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }
    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }
    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }
    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return enableAudit;
    }
    /**
     * @param enableAudit the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }
    
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception{
        destinationStatistics.getProducers().increment();
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception{
        destinationStatistics.getProducers().decrement();
    }
    
    public final MemoryUsage getMemoryUsage() {
        return memoryUsage;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

        
    public final String getName() {
        return getActiveMQDestination().getPhysicalName();
    }
    
    public final MessageStore getMessageStore() {
        return store;
    }
    
    public final boolean isActive() {
        return destinationStatistics.getConsumers().getCount() != 0 ||
            destinationStatistics.getProducers().getCount() != 0;
    }
    
    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public int getMinimumMessageSize() {
        return minimumMessageSize;
    }

    public void setMinimumMessageSize(int minimumMessageSize) {
        this.minimumMessageSize = minimumMessageSize;
    }

    public boolean isLazyDispatch() {
        return lazyDispatch;
    }

    public void setLazyDispatch(boolean lazyDispatch) {
        this.lazyDispatch = lazyDispatch;
    } 
    
    protected long getDestinationSequenceId() {
        return regionBroker.getBrokerSequenceId();
    }
}
