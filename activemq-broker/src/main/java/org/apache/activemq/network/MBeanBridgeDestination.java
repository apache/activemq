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
package org.apache.activemq.network;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.broker.jmx.NetworkDestinationView;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.thread.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MBeanBridgeDestination {
    private static final Logger LOG = LoggerFactory.getLogger(MBeanBridgeDestination.class);
    private final BrokerService brokerService;
    private final NetworkBridge bridge;
    private final NetworkBridgeView networkBridgeView;
    private final NetworkBridgeConfiguration networkBridgeConfiguration;
    private final Scheduler scheduler;
    private final Runnable purgeInactiveDestinationViewTask;
    private final Map<ActiveMQDestination, NetworkDestinationContainer> outboundDestinationViewMap = new ConcurrentHashMap<>();
    private final Map<ActiveMQDestination, NetworkDestinationContainer> inboundDestinationViewMap = new ConcurrentHashMap<>();

    public MBeanBridgeDestination(BrokerService brokerService, NetworkBridgeConfiguration networkBridgeConfiguration, NetworkBridge bridge, NetworkBridgeView networkBridgeView) {
        this.brokerService = brokerService;
        this.networkBridgeConfiguration = networkBridgeConfiguration;
        this.bridge = bridge;
        this.networkBridgeView = networkBridgeView;
        this.scheduler = brokerService.getScheduler();
        purgeInactiveDestinationViewTask = new Runnable() {
            public void run() {
                purgeInactiveDestinationViews();
            }
        };
    }


    public void onOutboundMessage(Message message) {
        ActiveMQDestination destination = message.getDestination();
        NetworkDestinationContainer networkDestinationContainer;

        if ((networkDestinationContainer = outboundDestinationViewMap.get(destination)) == null) {
            ObjectName bridgeObjectName = bridge.getMbeanObjectName();
            try {
                ObjectName objectName = BrokerMBeanSupport.createNetworkOutBoundDestinationObjectName(bridgeObjectName, destination);
                NetworkDestinationView networkDestinationView = new NetworkDestinationView(networkBridgeView, destination.getPhysicalName());
                AnnotatedMBean.registerMBean(brokerService.getManagementContext(), networkDestinationView, objectName);

                networkDestinationContainer = new NetworkDestinationContainer(networkDestinationView, objectName);
                outboundDestinationViewMap.put(destination, networkDestinationContainer);
                networkDestinationView.messageSent();
            } catch (Exception e) {
                LOG.warn("Failed to register " + destination, e);
            }
        } else {
            networkDestinationContainer.view.messageSent();
        }
    }


    public void onInboundMessage(Message message) {
        ActiveMQDestination destination = message.getDestination();
        NetworkDestinationContainer networkDestinationContainer;

        if ((networkDestinationContainer = inboundDestinationViewMap.get(destination)) == null) {
            ObjectName bridgeObjectName = bridge.getMbeanObjectName();
            try {
                ObjectName objectName = BrokerMBeanSupport.createNetworkInBoundDestinationObjectName(bridgeObjectName, destination);
                NetworkDestinationView networkDestinationView = new NetworkDestinationView(networkBridgeView, destination.getPhysicalName());
                AnnotatedMBean.registerMBean(brokerService.getManagementContext(), networkDestinationView, objectName);

                networkBridgeView.addNetworkDestinationView(networkDestinationView);
                networkDestinationContainer = new NetworkDestinationContainer(networkDestinationView, objectName);
                inboundDestinationViewMap.put(destination, networkDestinationContainer);
                networkDestinationView.messageSent();
            } catch (Exception e) {
                LOG.warn("Failed to register " + destination, e);
            }
        } else {
            networkDestinationContainer.view.messageSent();
        }
    }

    public void start() {
        if (networkBridgeConfiguration.isGcDestinationViews()) {
            long period = networkBridgeConfiguration.getGcSweepTime();
            if (period > 0) {
                scheduler.executePeriodically(purgeInactiveDestinationViewTask, period);
            }
        }
    }

    public void stop() {
        if (!brokerService.isUseJmx()) {
            return;
        }

        scheduler.cancel(purgeInactiveDestinationViewTask);
        for (NetworkDestinationContainer networkDestinationContainer : inboundDestinationViewMap.values()) {
            try {
                brokerService.getManagementContext().unregisterMBean(networkDestinationContainer.objectName);
            } catch (Exception e) {
                LOG.error("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
            }
        }
        for (NetworkDestinationContainer networkDestinationContainer : outboundDestinationViewMap.values()) {
            try {
                brokerService.getManagementContext().unregisterMBean(networkDestinationContainer.objectName);
            } catch (Exception e) {
                LOG.debug("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
            }
        }
        inboundDestinationViewMap.clear();
        outboundDestinationViewMap.clear();
    }

    private void purgeInactiveDestinationViews() {
        if (!brokerService.isUseJmx()) {
            return;
        }
        purgeInactiveDestinationView(inboundDestinationViewMap);
        purgeInactiveDestinationView(outboundDestinationViewMap);
    }

    private void purgeInactiveDestinationView(Map<ActiveMQDestination, NetworkDestinationContainer> map) {
        long time = System.currentTimeMillis() - networkBridgeConfiguration.getGcSweepTime();
        for (Iterator<Map.Entry<ActiveMQDestination, NetworkDestinationContainer>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<ActiveMQDestination, NetworkDestinationContainer> entry = it.next();
            if (entry.getValue().view.getLastAccessTime() <= time) {
                ObjectName objectName = entry.getValue().objectName;
                if (objectName != null) {
                    try {
                        brokerService.getManagementContext().unregisterMBean(entry.getValue().objectName);
                    } catch (Throwable e) {
                        LOG.debug("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
                    }
                }
                entry.getValue().view.close();
                it.remove();
            }
        }
    }

    private static class NetworkDestinationContainer {
        private final NetworkDestinationView view;
        private final ObjectName objectName;

        private NetworkDestinationContainer(NetworkDestinationView view, ObjectName objectName) {
            this.view = view;
            this.objectName = objectName;
        }
    }
}
