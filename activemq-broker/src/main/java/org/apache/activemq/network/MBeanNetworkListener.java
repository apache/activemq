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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MBeanNetworkListener implements NetworkBridgeListener {

    private static final Logger LOG = LoggerFactory.getLogger(MBeanNetworkListener.class);

    private final BrokerService brokerService;
    private final ObjectName connectorName;
    private final NetworkBridgeConfiguration networkBridgeConfiguration;
    private boolean createdByDuplex = false;
    private Map<NetworkBridge,MBeanBridgeDestination> destinationObjectNameMap = new ConcurrentHashMap<NetworkBridge,MBeanBridgeDestination>();

    public MBeanNetworkListener(BrokerService brokerService, NetworkBridgeConfiguration networkBridgeConfiguration, ObjectName connectorName) {
        this.brokerService = brokerService;
        this.networkBridgeConfiguration = networkBridgeConfiguration;
        this.connectorName = connectorName;
    }

    @Override
    public void bridgeFailed() {
    }

    @Override
    public void onStart(NetworkBridge bridge) {
        if (!brokerService.isUseJmx()) {
            return;
        }
        NetworkBridgeView view = new NetworkBridgeView(bridge);
        view.setCreateByDuplex(createdByDuplex);
        try {
            ObjectName objectName = createNetworkBridgeObjectName(bridge);
            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), view, objectName);
            bridge.setMbeanObjectName(objectName);
            MBeanBridgeDestination mBeanBridgeDestination = new MBeanBridgeDestination(brokerService,networkBridgeConfiguration,bridge,view);
            destinationObjectNameMap.put(bridge,mBeanBridgeDestination);
            mBeanBridgeDestination.start();
            LOG.debug("registered: {} as: {}", bridge, objectName);
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be registered in JMX: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onStop(NetworkBridge bridge) {
        if (!brokerService.isUseJmx()) {
            return;
        }
        try {
            ObjectName objectName = bridge.getMbeanObjectName();
            if (objectName != null) {
                brokerService.getManagementContext().unregisterMBean(objectName);
            }
            MBeanBridgeDestination mBeanBridgeDestination = destinationObjectNameMap.remove(bridge);
            if (mBeanBridgeDestination != null){
                mBeanBridgeDestination.stop();
            }
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
        }
    }



    protected ObjectName createNetworkBridgeObjectName(NetworkBridge bridge) throws MalformedObjectNameException {
        return BrokerMBeanSupport.createNetworkBridgeObjectName(connectorName, bridge.getRemoteAddress());
    }

    public void setCreatedByDuplex(boolean createdByDuplex) {
        this.createdByDuplex = createdByDuplex;
    }



    @Override
    public void onOutboundMessage(NetworkBridge bridge,Message message) {
        MBeanBridgeDestination mBeanBridgeDestination = destinationObjectNameMap.get(bridge);
        if (mBeanBridgeDestination != null){
            mBeanBridgeDestination.onOutboundMessage(message);
        }
    }

    @Override
    public void onInboundMessage(NetworkBridge bridge,Message message) {
        MBeanBridgeDestination mBeanBridgeDestination = destinationObjectNameMap.get(bridge);
        if (mBeanBridgeDestination != null){
            mBeanBridgeDestination.onInboundMessage(message);
        }
    }

}
