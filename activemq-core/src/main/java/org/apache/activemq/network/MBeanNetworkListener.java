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
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;

public class MBeanNetworkListener implements NetworkBridgeListener {

    private static final Logger LOG = LoggerFactory.getLogger(MBeanNetworkListener.class);

    BrokerService brokerService;
    ObjectName connectorName;
    boolean createdByDuplex = false;

    public MBeanNetworkListener(BrokerService brokerService, ObjectName connectorName) {
        this.brokerService = brokerService;
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
        NetworkBridgeViewMBean view = new NetworkBridgeView(bridge);
        ((NetworkBridgeView)view).setCreateByDuplex(createdByDuplex);
        try {
            ObjectName objectName = createNetworkBridgeObjectName(bridge);
            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), view, objectName);
            bridge.setMbeanObjectName(objectName);
            if (LOG.isDebugEnabled()) {
                LOG.debug("registered: " + bridge + " as: " + objectName);
            }
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be registered in JMX: " + e.getMessage(), e);
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
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be unregistered in JMX: " + e.getMessage(), e);
        }
    }


    protected ObjectName createNetworkBridgeObjectName(NetworkBridge bridge) throws MalformedObjectNameException {
        Map<String, String> map = new HashMap<String, String>(connectorName.getKeyPropertyList());
        return new ObjectName(connectorName.getDomain() + ":" + "BrokerName=" + JMXSupport.encodeObjectNamePart((String) map.get("BrokerName")) + "," + "Type=NetworkBridge,"
                              + "NetworkConnectorName=" + JMXSupport.encodeObjectNamePart((String)map.get("NetworkConnectorName")) + "," + "Name="
                              + JMXSupport.encodeObjectNamePart(JMXSupport.encodeObjectNamePart(bridge.getRemoteAddress())));
    }

    public void setCreatedByDuplex(boolean createdByDuplex) {
        this.createdByDuplex = createdByDuplex;
    }
}
