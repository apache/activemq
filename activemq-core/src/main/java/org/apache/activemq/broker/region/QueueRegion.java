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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;

/**
 * 
 * @version $Revision: 1.9 $
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
        Destination dest = null;
        try {
            dest = lookup(context, info.getDestination());
        } catch (Exception e) {
            JMSException jmsEx = new JMSException("Failed to retrieve destination from region "+ e);
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
        if (info.isBrowser()) {
            return new QueueBrowserSubscription(broker,dest,usageManager, context, info);
        } else {
            return new QueueSubscription(broker, dest,usageManager,context, info);
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
}
