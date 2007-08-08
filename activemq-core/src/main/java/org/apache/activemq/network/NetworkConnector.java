/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.network;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public abstract class NetworkConnector extends NetworkBridgeConfiguration implements Service {

    protected static final Log log = LogFactory.getLog(NetworkConnector.class);
    protected URI localURI;
    private Set durableDestinations;
    private List excludedDestinations = new CopyOnWriteArrayList();
    private List dynamicallyIncludedDestinations = new CopyOnWriteArrayList();
    private List staticallyIncludedDestinations = new CopyOnWriteArrayList();
    protected ConnectionFilter connectionFilter;
    private BrokerService brokerService;
    private ObjectName objectName;

    protected ServiceSupport serviceSupport = new ServiceSupport() {

        protected void doStart() throws Exception {
            handleStart();
        }

        protected void doStop(ServiceStopper stopper) throws Exception {
            handleStop(stopper);
        }
    };

    public NetworkConnector() {
    }

    public NetworkConnector(URI localURI) {
        this.localURI = localURI;
    }

    public URI getLocalUri() throws URISyntaxException {
        return localURI;
    }

    public void setLocalUri(URI localURI) {
        this.localURI = localURI;
    }

    /**
     * @return Returns the durableDestinations.
     */
    public Set getDurableDestinations() {
        return durableDestinations;
    }

    /**
     * @param durableDestinations The durableDestinations to set.
     */
    public void setDurableDestinations(Set durableDestinations) {
        this.durableDestinations = durableDestinations;
    }

    /**
     * @return Returns the excludedDestinations.
     */
    public List getExcludedDestinations() {
        return excludedDestinations;
    }

    /**
     * @param excludedDestinations The excludedDestinations to set.
     */
    public void setExcludedDestinations(List excludedDestinations) {
        this.excludedDestinations = excludedDestinations;
    }

    public void addExcludedDestination(ActiveMQDestination destiantion) {
        this.excludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the staticallyIncludedDestinations.
     */
    public List getStaticallyIncludedDestinations() {
        return staticallyIncludedDestinations;
    }

    /**
     * @param staticallyIncludedDestinations The staticallyIncludedDestinations
     *                to set.
     */
    public void setStaticallyIncludedDestinations(List staticallyIncludedDestinations) {
        this.staticallyIncludedDestinations = staticallyIncludedDestinations;
    }

    public void addStaticallyIncludedDestination(ActiveMQDestination destiantion) {
        this.staticallyIncludedDestinations.add(destiantion);
    }

    /**
     * @return Returns the dynamicallyIncludedDestinations.
     */
    public List getDynamicallyIncludedDestinations() {
        return dynamicallyIncludedDestinations;
    }

    /**
     * @param dynamicallyIncludedDestinations The
     *                dynamicallyIncludedDestinations to set.
     */
    public void setDynamicallyIncludedDestinations(List dynamicallyIncludedDestinations) {
        this.dynamicallyIncludedDestinations = dynamicallyIncludedDestinations;
    }

    public void addDynamicallyIncludedDestination(ActiveMQDestination destiantion) {
        this.dynamicallyIncludedDestinations.add(destiantion);
    }

    public ConnectionFilter getConnectionFilter() {
        return connectionFilter;
    }

    public void setConnectionFilter(ConnectionFilter connectionFilter) {
        this.connectionFilter = connectionFilter;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected NetworkBridge configureBridge(DemandForwardingBridgeSupport result) {
        List destsList = getDynamicallyIncludedDestinations();
        ActiveMQDestination dests[] = (ActiveMQDestination[])destsList
            .toArray(new ActiveMQDestination[destsList.size()]);
        result.setDynamicallyIncludedDestinations(dests);
        destsList = getExcludedDestinations();
        dests = (ActiveMQDestination[])destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setExcludedDestinations(dests);
        destsList = getStaticallyIncludedDestinations();
        dests = (ActiveMQDestination[])destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setStaticallyIncludedDestinations(dests);
        if (durableDestinations != null) {
            ActiveMQDestination[] dest = new ActiveMQDestination[durableDestinations.size()];
            dest = (ActiveMQDestination[])durableDestinations.toArray(dest);
            result.setDurableDestinations(dest);
        }
        return result;
    }

    protected Transport createLocalTransport() throws Exception {
        return TransportFactory.connect(localURI);
    }

    public void start() throws Exception {
        serviceSupport.start();
    }

    public void stop() throws Exception {
        serviceSupport.stop();
    }

    public abstract String getName();

    protected void handleStart() throws Exception {
        if (localURI == null) {
            throw new IllegalStateException("You must configure the 'localURI' property");
        }
        log.info("Network Connector " + getName() + " Started");
    }

    protected void handleStop(ServiceStopper stopper) throws Exception {
        log.info("Network Connector " + getName() + " Stopped");
    }

    public ObjectName getObjectName() {
        return objectName;
    }

    public void setObjectName(ObjectName objectName) {
        this.objectName = objectName;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    protected void registerNetworkBridgeMBean(NetworkBridge bridge) {
        if (!getBrokerService().isUseJmx())
            return;

        MBeanServer mbeanServer = getBrokerService().getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            NetworkBridgeViewMBean view = new NetworkBridgeView(bridge);
            try {
                ObjectName objectName = createNetworkBridgeObjectName(bridge);
                mbeanServer.registerMBean(view, objectName);
            } catch (Throwable e) {
                log.debug("Network bridge could not be registered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected void unregisterNetworkBridgeMBean(NetworkBridge bridge) {
        if (!getBrokerService().isUseJmx())
            return;

        MBeanServer mbeanServer = getBrokerService().getManagementContext().getMBeanServer();
        if (mbeanServer != null) {
            try {
                ObjectName objectName = createNetworkBridgeObjectName(bridge);
                mbeanServer.unregisterMBean(objectName);
            } catch (Throwable e) {
                log.debug("Network bridge could not be unregistered in JMX: " + e.getMessage(), e);
            }
        }
    }

    protected ObjectName createNetworkBridgeObjectName(NetworkBridge bridge)
        throws MalformedObjectNameException {
        ObjectName connectorName = getObjectName();
        Hashtable map = connectorName.getKeyPropertyList();
        return new ObjectName(connectorName.getDomain()
                              + ":"
                              + "BrokerName="
                              + JMXSupport.encodeObjectNamePart((String)map.get("BrokerName"))
                              + ","
                              + "Type=NetworkBridge,"
                              + "NetworkConnectorName="
                              + JMXSupport.encodeObjectNamePart((String)map.get("NetworkConnectorName"))
                              + ","
                              + "Name="
                              + JMXSupport.encodeObjectNamePart(JMXSupport.encodeObjectNamePart(bridge
                                  .getRemoteAddress())));
    }

}
