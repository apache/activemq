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

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.JMXSupport;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.util.Hashtable;

public class ManagedRegionBroker extends RegionBroker {

    private final MBeanServer mbeanServer;
    private final ObjectName brokerObjectName;

    public ManagedRegionBroker(MBeanServer mbeanServer, ObjectName brokerObjectName, TaskRunnerFactory taskRunnerFactory, UsageManager memoryManager, PersistenceAdapter adapter, PolicyMap policyMap) throws IOException {
        super(taskRunnerFactory, memoryManager, adapter, policyMap);
        this.mbeanServer = mbeanServer;
        this.brokerObjectName = brokerObjectName;
    }

    protected Region createQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter adapter, PolicyMap policyMap) {
        return new ManagedQueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, adapter, policyMap);
    }
    
    protected Region createTempQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory) {
        return new ManagedTempQueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory);
    }
    
    protected Region createTempTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory) {
        return new ManagedTempTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory);
    }
    
    protected Region createTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter adapter, PolicyMap policyMap) {
        return new ManagedTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, adapter, policyMap);
    }

    public void register(ActiveMQDestination destName, Destination destination) throws Throwable {
        
        // Build the object name for the destination
        Hashtable map = new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString()));
        map.put("Destination", JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        ObjectName destObjectName= new ObjectName(brokerObjectName.getDomain(), map);
        
        DestinationViewMBean view = new DestinationView(destination);
        
        mbeanServer.registerMBean(view, destObjectName);        
    }

    public void unregister(ActiveMQDestination destName) throws Throwable {
        // Build the object name for the destination
        Hashtable map = new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString()));
        map.put("Destination", JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        ObjectName destObjectName= new ObjectName(brokerObjectName.getDomain(), map);
        
        mbeanServer.unregisterMBean(destObjectName);        
    }
}
