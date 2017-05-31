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

import javax.jms.JMSException;
import javax.management.ObjectName;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TempQueueRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;

public class ManagedTempQueueRegion extends TempQueueRegion {

    private final ManagedRegionBroker regionBroker;

    public ManagedTempQueueRegion(ManagedRegionBroker broker, DestinationStatistics destinationStatistics, SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
                                  DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
        this.regionBroker = broker;
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws JMSException {
        Subscription sub = super.createSubscription(context, info);
        ObjectName name = regionBroker.registerSubscription(context, sub);
        sub.setObjectName(name);
        return sub;
    }

    protected void destroySubscription(Subscription sub) {
        regionBroker.unregisterSubscription(sub);
        super.destroySubscription(sub);
    }

    protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        Destination rc = super.createDestination(context, destination);
        regionBroker.register(destination, rc);
        return rc;
    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        regionBroker.unregister(destination);
    }

}
