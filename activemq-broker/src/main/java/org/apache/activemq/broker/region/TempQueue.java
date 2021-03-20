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

import java.io.IOException;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Queue is a List of MessageEntry objects that are dispatched to matching
 * subscriptions.
 * 
 * 
 */
public class TempQueue extends Queue{
    private static final Logger LOG = LoggerFactory.getLogger(TempQueue.class);
    private final ActiveMQTempDestination tempDest;
   
    
    /**
     * @param brokerService
     * @param destination
     * @param store
     * @param parentStats
     * @param taskFactory
     * @throws Exception
     */
    public TempQueue(BrokerService brokerService,
            ActiveMQDestination destination, MessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory)
            throws Exception {
        super(brokerService, destination, store, parentStats, taskFactory);
        this.tempDest = (ActiveMQTempDestination) destination;
    }
    
    @Override
    public void initialize() throws Exception {
        this.messages=new VMPendingMessageCursor(false);
        this.messages.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
        this.systemUsage = brokerService.getSystemUsage();
        memoryUsage.setParent(systemUsage.getMemoryUsage());           
        this.taskRunner = taskFactory.createTaskRunner(this, "TempQueue:  " + destination.getPhysicalName());
    }
    
    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        // Only consumers on the same connection can consume from
        // the temporary destination
        // However, we could have failed over - and we do this
        // check client side anyways ....
        if (!context.isFaultTolerant()
                && (!context.isNetworkConnection() && !tempDest
                        .getConnectionId().equals(
                                sub.getConsumerInfo().getConsumerId()
                                        .getConnectionId()))) {

            tempDest.setConnectionId(sub.getConsumerInfo().getConsumerId().getConnectionId());
            LOG.debug("changed ownership of {} to {}", this, tempDest.getConnectionId());
        }
        super.addSubscription(context, sub);
    }
    
    @Override
    public void dispose(ConnectionContext context) throws IOException {
        if (this.destinationStatistics.getMessages().getCount() > 0) {
            LOG.info("{} on dispose, purge of {} pending messages: {}",
                    getActiveMQDestination().getQualifiedName(),
                    this.destinationStatistics.getMessages().getCount(),
                    messages);
            // we may want to capture these message ids in an advisory
        }
        try {
            purge();
        } catch (Exception e) {
            LOG.warn("Caught an exception purging Queue: {}", destination, e);
        }
        super.dispose(context);
    }
}
