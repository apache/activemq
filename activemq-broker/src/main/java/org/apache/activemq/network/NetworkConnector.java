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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.NetworkBridgeView;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector class for bridging broker networks.
 */
public abstract class NetworkConnector extends NetworkBridgeConfiguration implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkConnector.class);
    protected URI localURI;
    protected ConnectionFilter connectionFilter;
    protected ConcurrentMap<URI, NetworkBridge> bridges = new ConcurrentHashMap<URI, NetworkBridge>();

    protected ServiceSupport serviceSupport = new ServiceSupport() {

        @Override
        protected void doStart() throws Exception {
            handleStart();
        }

        @Override
        protected void doStop(ServiceStopper stopper) throws Exception {
            handleStop(stopper);
        }
    };

    private Set<ActiveMQDestination> durableDestinations;

    private BrokerService brokerService;
    private ObjectName objectName;

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
    public Set<ActiveMQDestination> getDurableDestinations() {
        return durableDestinations;
    }

    /**
     * @param durableDestinations The durableDestinations to set.
     */
    public void setDurableDestinations(Set<ActiveMQDestination> durableDestinations) {
        this.durableDestinations = durableDestinations;
    }


    public void addExcludedDestination(ActiveMQDestination destination) {
        this.excludedDestinations.add(destination);
    }


    public void addStaticallyIncludedDestination(ActiveMQDestination destination) {
        this.staticallyIncludedDestinations.add(destination);
    }


    public void addDynamicallyIncludedDestination(ActiveMQDestination destination) {
        this.dynamicallyIncludedDestinations.add(destination);
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
        List<ActiveMQDestination> destsList = getDynamicallyIncludedDestinations();
        ActiveMQDestination dests[] = destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setDynamicallyIncludedDestinations(dests);
        destsList = getExcludedDestinations();
        dests = destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setExcludedDestinations(dests);
        destsList = getStaticallyIncludedDestinations();
        dests = destsList.toArray(new ActiveMQDestination[destsList.size()]);
        result.setStaticallyIncludedDestinations(dests);
        result.setDurableDestinations(getDurableTopicDestinations(durableDestinations));
        return result;
    }

    protected Transport createLocalTransport() throws Exception {
        return NetworkBridgeFactory.createLocalTransport(this, localURI);
    }

    public static ActiveMQDestination[] getDurableTopicDestinations(final Set<ActiveMQDestination> durableDestinations) {
        if (durableDestinations != null) {

            HashSet<ActiveMQDestination> topics = new HashSet<ActiveMQDestination>();
            for (ActiveMQDestination d : durableDestinations) {
                if( d.isTopic() ) {
                    topics.add(d);
                }
            }

            ActiveMQDestination[] dest = new ActiveMQDestination[topics.size()];
            dest = topics.toArray(dest);
            return dest;
        }
        return null;
    }

    @Override
    public void start() throws Exception {
        serviceSupport.start();
    }

    @Override
    public void stop() throws Exception {
        serviceSupport.stop();
    }

    protected void handleStart() throws Exception {
        if (localURI == null) {
            throw new IllegalStateException("You must configure the 'localURI' property");
        }
        LOG.info("Network Connector {} started", this);
    }

    protected void handleStop(ServiceStopper stopper) throws Exception {
        LOG.info("Network Connector {} stopped", this);
    }

    public boolean isStarted() {
        return serviceSupport.isStarted();
    }

    public boolean isStopped() {
        return serviceSupport.isStopped();
    }

    public boolean isStopping() {
        return serviceSupport.isStopping();
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
        if (!getBrokerService().isUseJmx()) {
            return;
        }
        NetworkBridgeViewMBean view = new NetworkBridgeView(bridge);
        try {
            ObjectName objectName = createNetworkBridgeObjectName(bridge);
            AnnotatedMBean.registerMBean(getBrokerService().getManagementContext(), view, objectName);
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be registered in JMX: {}", e.getMessage(), e);
        }
    }

    protected void unregisterNetworkBridgeMBean(NetworkBridge bridge) {
        if (!getBrokerService().isUseJmx()) {
            return;
        }
        try {
            ObjectName objectName = createNetworkBridgeObjectName(bridge);
            getBrokerService().getManagementContext().unregisterMBean(objectName);
        } catch (Throwable e) {
            LOG.debug("Network bridge could not be unregistered in JMX: {}", e.getMessage(), e);
        }
    }

    protected ObjectName createNetworkBridgeObjectName(NetworkBridge bridge) throws MalformedObjectNameException {
        return BrokerMBeanSupport.createNetworkBridgeObjectName(getObjectName(), bridge.getRemoteAddress());
    }

    // ask all the bridges as we can't know to which this consumer is tied
    public boolean removeDemandSubscription(ConsumerId consumerId) {
        boolean removeSucceeded = false;
        for (NetworkBridge bridge : bridges.values()) {
            if (bridge instanceof DemandForwardingBridgeSupport) {
                DemandForwardingBridgeSupport demandBridge = (DemandForwardingBridgeSupport) bridge;
                if (demandBridge.removeDemandSubscriptionByLocalId(consumerId)) {
                    removeSucceeded = true;
                    break;
                }
            }
        }
        return removeSucceeded;
    }

    public Collection<NetworkBridge> activeBridges() {
        return bridges.values();
    }
}
