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

import java.util.Iterator;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;

/**
 * 
 * 
 */
public class QueueRegion extends AbstractRegion {

    public QueueRegion(RegionBroker broker, DestinationStatistics destinationStatistics,
                       SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
                       DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    public String toString() {
        return "QueueRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size()
               + ", memory=" + usageManager.getMemoryUsage().getPercentUsage() + "%";
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info)
        throws JMSException {
        ActiveMQDestination destination = info.getDestination();
        PolicyEntry entry = null;
        if (destination != null && broker.getDestinationPolicy() != null) {
            entry = broker.getDestinationPolicy().getEntryFor(destination);
            
        }
        if (info.isBrowser()) {
            QueueBrowserSubscription sub = new QueueBrowserSubscription(broker,usageManager, context, info);
            if (entry != null) {
                entry.configure(broker, usageManager, sub);
            }
            return sub;
        } else {
            QueueSubscription sub =   new QueueSubscription(broker, usageManager,context, info);
            if (entry != null) {
                entry.configure(broker, usageManager, sub);
            }
            return sub;
        }
    }

    protected Set<ActiveMQDestination> getInactiveDestinations() {
        Set<ActiveMQDestination> inactiveDestinations = super.getInactiveDestinations();
        for (Iterator<ActiveMQDestination> iter = inactiveDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination dest = iter.next();
            if (!dest.isQueue()) {
                iter.remove();
            }
        }
        return inactiveDestinations;
    }
    
    /*
     * For a Queue, dispatch order is imperative to match acks, so the dispatch is deferred till 
     * the notification to ensure that the subscription chosen by the master is used.
     * 
     * (non-Javadoc)
     * @see org.apache.activemq.broker.region.AbstractRegion#processDispatchNotification(org.apache.activemq.command.MessageDispatchNotification)
     */
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        processDispatchNotificationViaDestination(messageDispatchNotification);
    }
}
