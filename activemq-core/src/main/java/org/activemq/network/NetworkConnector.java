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
package org.activemq.network;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.activemq.Service;
import org.activemq.command.DiscoveryEvent;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportFactory;
import org.activemq.transport.discovery.DiscoveryAgent;
import org.activemq.transport.discovery.DiscoveryAgentFactory;
import org.activemq.transport.discovery.DiscoveryListener;
import org.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class NetworkConnector implements Service, DiscoveryListener {

    private static final Log log = LogFactory.getLog(NetworkConnector.class);
    private DiscoveryAgent discoveryAgent;
    private URI localURI;

    private ConcurrentHashMap bridges = new ConcurrentHashMap();
    private String brokerName;
    
    public NetworkConnector() {
    }

    public NetworkConnector(URI localURI, DiscoveryAgent discoveryAgent) throws IOException {
        this.localURI = localURI;
        setDiscoveryAgent(discoveryAgent);
    }

    public void start() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("You must configure the 'discoveryAgent' property");
        }
        if (localURI == null) {
            throw new IllegalStateException("You must configure the 'localURI' property");
        }
        this.discoveryAgent.start();
    }

    public void stop() throws Exception {
        this.discoveryAgent.stop();
    }

    public void onServiceAdd(DiscoveryEvent event) {
        String url = event.getServiceName();
        if (url != null) {

            URI uri;
            try {
                uri = new URI(url);
            }
            catch (URISyntaxException e) {
                log.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }

            // Has it allready been added?
            if (bridges.containsKey(uri) || localURI.equals(uri))
                return;

            log.info("Establishing network connection between " + localURI + " and " + event.getBrokerName() + " at " + uri);

            Transport localTransport;
            try {
                localTransport = TransportFactory.connect(localURI);
            }
            catch (Exception e) {
                log.warn("Could not connect to local URI: " + localURI + ": " + e, e);
                return;
            }

            Transport remoteTransport;
            try {
                remoteTransport = TransportFactory.connect(uri);
            }
            catch (Exception e) {
                ServiceSupport.dispose(localTransport);
                log.warn("Could not connect to remote URI: " + uri + ": " + e, e);
                return;
            }

            Bridge bridge = createBridge(localTransport, remoteTransport);
            bridges.put(uri, bridge);
            try {
                bridge.start();
            }
            catch (Exception e) {
                ServiceSupport.dispose(localTransport);
                ServiceSupport.dispose(remoteTransport);
                log.warn("Could not start network bridge between: " + localURI + " and: " + uri + " due to: " + e, e);
                return;
            }
        }
    }

    public void onServiceRemove(DiscoveryEvent event) {
        String url = event.getServiceName();
        if (url != null) {
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                log.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }

            Bridge bridge = (Bridge) bridges.get(uri);
            if (bridge == null)
                return;

            ServiceSupport.dispose(bridge);
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    public DiscoveryAgent getDiscoveryAgent() {
        return discoveryAgent;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
        if (discoveryAgent != null) {
            this.discoveryAgent.setDiscoveryListener(this);
            this.discoveryAgent.setBrokerName(brokerName);
        }
    }

    public URI getLocalUri() throws URISyntaxException {
        return localURI;
    }

    public void setLocalUri(URI localURI) {
        this.localURI = localURI;
    }
    
    public void setUri(URI discoveryURI) throws IOException {
        setDiscoveryAgent(DiscoveryAgentFactory.createDiscoveryAgent(discoveryURI));
    }    

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Bridge createBridge(Transport localTransport, Transport remoteTransport) {
        return new DemandForwardingBridge(localTransport, remoteTransport);
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
        if( discoveryAgent!=null ) {
            discoveryAgent.setBrokerName(brokerName);
        }
    }

}
