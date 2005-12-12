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

import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.Destination;
import org.activemq.broker.region.DestinationStatistics;
import org.activemq.broker.region.TempTopicRegion;
import org.activemq.command.ActiveMQDestination;
import org.activemq.memory.UsageManager;
import org.activemq.thread.TaskRunnerFactory;

public class ManagedTempTopicRegion extends TempTopicRegion {

    private final ManagedRegionBroker regionBroker;

    public ManagedTempTopicRegion(ManagedRegionBroker regionBroker, DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory) {
        super(destinationStatistics, memoryManager, taskRunnerFactory);
        this.regionBroker = regionBroker;
    }

    protected Destination createDestination(ActiveMQDestination destination) throws Throwable {
        Destination rc = super.createDestination(destination);
        regionBroker.register(destination, rc);
        return rc;
    }
    
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable {
        super.removeDestination(context, destination, timeout);
        regionBroker.unregister(destination);
    }

}
