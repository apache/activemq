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
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.broker.jmx.TopicView;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.replica.jmx.ReplicationJmxHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaJmxBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaJmxBroker.class);
    private final Set<ObjectName> registeredMBeans = ConcurrentHashMap.newKeySet();
    private final ReplicaPolicy replicaPolicy;
    private final BrokerService brokerService;

    public ReplicaJmxBroker(Broker next, ReplicaPolicy replicaPolicy) {
        super(next);
        this.replicaPolicy = replicaPolicy;
        brokerService = getBrokerService();
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
        Destination answer = super.addDestination(context, destination, createIfTemporary);
        if (ReplicaSupport.isReplicationDestination(destination)) {
            reregisterReplicationDestination(destination, answer);
        }
        return answer;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        if (ReplicaSupport.isReplicationDestination(destination)) {
            unregisterReplicationDestination(destination);
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription subscription = super.addConsumer(context, info);

        if (ReplicaSupport.isReplicationDestination(info.getDestination()) && brokerService.isUseJmx() &&
                replicaPolicy.isHideReplicationDestination()) {
            ManagedRegionBroker managedRegionBroker = (ManagedRegionBroker) getAdaptor(ManagedRegionBroker.class);
            if (managedRegionBroker != null) {
                managedRegionBroker.unregisterSubscription(subscription);
                ObjectName objectName = subscription.getObjectName();
                if (objectName != null) {
                    brokerService.getManagementContext().unregisterMBean(objectName);
                }
            }
            subscription.setObjectName(null);
        }
        return subscription;
    }

    private void reregisterReplicationDestination(ActiveMQDestination replicationDestination, Destination destination) {
        try {
            if (!brokerService.isUseJmx()) {
                return;
            }
            ObjectName destinationName = createCrdrDestinationName(replicationDestination);
            if (registeredMBeans.contains(destinationName)) {
                return;
            }

            ManagedRegionBroker managedRegionBroker = (ManagedRegionBroker) getAdaptor(ManagedRegionBroker.class);
            if (managedRegionBroker == null) {
                return;
            }
            if (replicaPolicy.isHideReplicationDestination()) {
                managedRegionBroker.unregister(replicationDestination);
            }

            DestinationView view = null;
            if (replicationDestination.isQueue()) {
                view = new QueueView(managedRegionBroker, DestinationExtractor.extractQueue(destination));
            } else if (replicationDestination.isTopic()) {
                view = new TopicView(managedRegionBroker, DestinationExtractor.extractTopic(destination));
            }

            if (view != null) {
                AnnotatedMBean.registerMBean(brokerService.getManagementContext(), view, destinationName);
                registeredMBeans.add(destinationName);
            }
        } catch (Exception e) {
            logger.warn("Failed to reregister MBean for {}", replicationDestination);
            logger.debug("Failure reason: ", e);
        }
    }

    private void unregisterReplicationDestination(ActiveMQDestination replicationDestination) {
        try {
            if (!brokerService.isUseJmx()) {
                return;
            }
            ObjectName destinationName = createCrdrDestinationName(replicationDestination);
            if (registeredMBeans.remove(destinationName)) {
                brokerService.getManagementContext().unregisterMBean(destinationName);
            }
        } catch (Exception e) {
            logger.warn("Failed to unregister MBean for {}", replicationDestination);
            logger.debug("Failure reason: ", e);
        }
    }

    private ObjectName createCrdrDestinationName(ActiveMQDestination replicationDestination) throws MalformedObjectNameException {
        return BrokerMBeanSupport.createDestinationName(ReplicationJmxHelper.createJmxName(brokerService), replicationDestination);
    }
}
