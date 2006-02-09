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
package org.apache.activemq.network;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class NetworkConnector implements Service, DiscoveryListener {

    private static final Log log = LogFactory.getLog(NetworkConnector.class);
    private String brokerName = "localhost";
    private DiscoveryAgent discoveryAgent;
    private URI localURI;

    private ConcurrentHashMap bridges = new ConcurrentHashMap();
    boolean failover=true;
    
    
    public NetworkConnector(){
        
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

            URI connectUri = uri;
            if( failover ) {
                try {
                    connectUri = new URI("failover:"+connectUri);
                } catch (URISyntaxException e) {
                    log.warn("Could not create failover URI: "+connectUri);
                    return;
                }
            }
            
            log.info("Establishing network connection between " + localURI + " and " + event.getBrokerName() + " at " + connectUri);

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
                remoteTransport = TransportFactory.connect(connectUri);
            }
            catch (Exception e) {
                ServiceSupport.dispose(localTransport);
                log.warn("Could not connect to remote URI: " + connectUri + ": " + e, e);
                return;
            }

            Bridge bridge = createBridge(localTransport, remoteTransport, event);
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
    protected Bridge createBridge(Transport localTransport, Transport remoteTransport, final DiscoveryEvent event) {
        DemandForwardingBridge result =  new DemandForwardingBridge(localTransport, remoteTransport) {
            protected void serviceRemoteException(IOException error) {
                super.serviceRemoteException(error);
                try {
                    // Notify the discovery agent that the remote broker failed.
                    discoveryAgent.serviceFailed(event);
                } catch (IOException e) {
                }
            }
        };
        result.setLocalBrokerName(brokerName);
        return result;
    }

    

    public boolean isFailover() {
        return failover;
    }

    public void setFailover(boolean reliable) {
        this.failover = reliable;
    }


    /**
     * @return Returns the brokerName.
     */
    public String getBrokerName(){
        return brokerName;
    }


    /**
     * @param brokerName The brokerName to set.
     */
    public void setBrokerName(String brokerName){
        this.brokerName=brokerName;
    }

}
