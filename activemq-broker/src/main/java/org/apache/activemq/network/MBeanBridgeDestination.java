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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.broker.jmx.NetworkDestinationView;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MBeanBridgeDestination {
    private static final Logger LOG = LoggerFactory.getLogger(MBeanBridgeDestination.class);
    private final BrokerService brokerService;
    private final NetworkBridge bridge;
    private final NetworkBridgeView networkBridgeView;
    private Map<ActiveMQDestination, ObjectName> destinationObjectNameMap = new ConcurrentHashMap<ActiveMQDestination, ObjectName>();
    private Map<ActiveMQDestination, NetworkDestinationView> outboundDestinationViewMap = new ConcurrentHashMap<ActiveMQDestination, NetworkDestinationView>();
    private Map<ActiveMQDestination, NetworkDestinationView> inboundDestinationViewMap = new ConcurrentHashMap<ActiveMQDestination, NetworkDestinationView>();

    public MBeanBridgeDestination(BrokerService brokerService, NetworkBridge bridge, NetworkBridgeView networkBridgeView) {
        this.brokerService = brokerService;
        this.bridge = bridge;
        this.networkBridgeView = networkBridgeView;
    }


    public void onOutboundMessage(Message message) {
        ActiveMQDestination destination = message.getDestination();
        NetworkDestinationView networkDestinationView = outboundDestinationViewMap.get(destination);
        if (networkDestinationView == null) {
            synchronized (destinationObjectNameMap) {
                if ((networkDestinationView = outboundDestinationViewMap.get(destination)) == null) {
                    ObjectName bridgeObjectName = bridge.getMbeanObjectName();
                    try {
                        ObjectName objectName = BrokerMBeanSupport.createNetworkOutBoundDestinationObjectName(bridgeObjectName, destination);
                        networkDestinationView = new NetworkDestinationView(networkBridgeView,destination.getPhysicalName());
                        AnnotatedMBean.registerMBean(brokerService.getManagementContext(), networkDestinationView, objectName);
                        destinationObjectNameMap.put(destination, objectName);
                        outboundDestinationViewMap.put(destination, networkDestinationView);

                    } catch (Exception e) {
                        LOG.warn("Failed to register " + destination, e);
                    }
                }
            }
        }
        networkDestinationView.messageSent();
    }


    public void onInboundMessage(Message message) {
        ActiveMQDestination destination = message.getDestination();
        NetworkDestinationView networkDestinationView = inboundDestinationViewMap.get(destination);
        if (networkDestinationView == null) {
            synchronized (destinationObjectNameMap) {
                if ((networkDestinationView = inboundDestinationViewMap.get(destination)) == null) {
                    ObjectName bridgeObjectName = bridge.getMbeanObjectName();
                    try {
                        ObjectName objectName = BrokerMBeanSupport.createNetworkInBoundDestinationObjectName(bridgeObjectName, destination);
                        networkDestinationView= new NetworkDestinationView(networkBridgeView,destination.getPhysicalName());
                        networkBridgeView.addNetworkDestinationView(networkDestinationView);
                        AnnotatedMBean.registerMBean(brokerService.getManagementContext(), networkDestinationView, objectName);
                        destinationObjectNameMap.put(destination, objectName);
                        inboundDestinationViewMap.put(destination, networkDestinationView);
                    } catch (Exception e) {
                        LOG.warn("Failed to register " + destination, e);
                    }
                }
            }
        }
        networkDestinationView.messageSent();
    }

    public void close() {
        if (!brokerService.isUseJmx()) {
            return;
        }

        for (ObjectName objectName : destinationObjectNameMap.values()) {
            try {
                if (objectName != null) {
                    brokerService.getManagementContext().unregisterMBean(objectName);
                }
            } catch (Throwable e) {
                LOG.debug("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
            }
        }
        destinationObjectNameMap.clear();
        outboundDestinationViewMap.clear();
        inboundDestinationViewMap.clear();
    }

}
