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

import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.7 $
 */
public class TempQueueRegion extends AbstractTempRegion {
    private static final Log LOG = LogFactory.getLog(TempQueueRegion.class);
    private final BrokerService brokerService;
    
    public TempQueueRegion(RegionBroker broker, BrokerService brokerService, DestinationStatistics destinationStatistics, SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
                           DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
        // We should allow the following to be configurable via a Destination
        // Policy
        // setAutoCreateDestinations(false);
        this.brokerService = brokerService;
    }

    protected Destination doCreateDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {  
        TempQueue result = new TempQueue(brokerService, destination, null, destinationStatistics, taskRunnerFactory);
        result.initialize();
        return result;
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws JMSException {
        if (info.isBrowser()) {
            return new QueueBrowserSubscription(broker,usageManager,context, info);
        } else {
            return new QueueSubscription(broker,usageManager,context, info);
        }
    }

    public String toString() {
        return "TempQueueRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size() + ", memory=" + usageManager.getMemoryUsage().getPercentUsage() + "%";
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {

        // Force a timeout value so that we don't get an error that
        // there is still an active sub. Temp destination may be removed
        // while a network sub is still active which is valid.
        if (timeout == 0) {
            timeout = 1;
        }

        super.removeDestination(context, destination, timeout);
    }
}
