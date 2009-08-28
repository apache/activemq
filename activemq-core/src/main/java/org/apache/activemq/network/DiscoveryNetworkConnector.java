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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A network connector which uses a discovery agent to detect the remote brokers
 * available and setup a connection to each available remote broker
 * 
 * @org.apache.xbean.XBean element="networkConnector"
 * @version $Revision$
 */
public class DiscoveryNetworkConnector extends NetworkConnector implements DiscoveryListener {
    private static final Log LOG = LogFactory.getLog(DiscoveryNetworkConnector.class);

    private DiscoveryAgent discoveryAgent;
    private ConcurrentHashMap<URI, NetworkBridge> bridges = new ConcurrentHashMap<URI, NetworkBridge>();
    private Map<String, String> parameters;
    
    public DiscoveryNetworkConnector() {
    }

    public DiscoveryNetworkConnector(URI discoveryURI) throws IOException {
        setUri(discoveryURI);
    }

    public void setUri(URI discoveryURI) throws IOException {
        setDiscoveryAgent(DiscoveryAgentFactory.createDiscoveryAgent(discoveryURI));
        try {
            parameters = URISupport.parseParamters(discoveryURI);
            // allow discovery agent to grab it's parameters
            IntrospectionSupport.setProperties(getDiscoveryAgent(), parameters);
        } catch (URISyntaxException e) {
            LOG.warn("failed to parse query parameters from discoveryURI: " + discoveryURI, e);
        }  
        
    }

    public void onServiceAdd(DiscoveryEvent event) {
        String localURIName = localURI.getScheme() + "://" + localURI.getHost();
        // Ignore events once we start stopping.
        if (serviceSupport.isStopped() || serviceSupport.isStopping()) {
            return;
        }
        String url = event.getServiceName();
        if (url != null) {
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                LOG.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }
            // Should we try to connect to that URI?
            if( bridges.containsKey(uri) ) {
                LOG.debug("Discovery agent generated a duplicate onServiceAdd event for: "+uri );
                return;
            }
            if ( localURI.equals(uri) || (connectionFilter != null && !connectionFilter.connectTo(uri))) {
                LOG.debug("not connecting loopback: " + uri);
                return;
            }
            URI connectUri = uri;
            try {
                connectUri = URISupport.applyParameters(connectUri, parameters);
            } catch (URISyntaxException e) {
                LOG.warn("could not apply query parameters: " + parameters + " to: " + connectUri, e);
            }
            LOG.info("Establishing network connection from " + localURIName + " to " + connectUri);

            Transport remoteTransport;
            Transport localTransport;
            try {
                // Allows the transport to access the broker's ssl configuration.
                SslContext.setCurrentSslContext(getBrokerService().getSslContext());
                try {
                    remoteTransport = TransportFactory.connect(connectUri);
                } catch (Exception e) {
                    LOG.warn("Could not connect to remote URI: " + connectUri + ": " + e.getMessage());
                    LOG.debug("Connection failure exception: " + e, e);
                    return;
                }
                try {
                    localTransport = createLocalTransport();
                } catch (Exception e) {
                    ServiceSupport.dispose(remoteTransport);
                    LOG.warn("Could not connect to local URI: " + localURIName + ": " + e.getMessage());
                    LOG.debug("Connection failure exception: " + e, e);
                    return;
                }
            } finally {
                SslContext.setCurrentSslContext(null);
            }
            NetworkBridge bridge = createBridge(localTransport, remoteTransport, event);
            try {
                bridge.start();
                bridges.put(uri, bridge);
            } catch (Exception e) {
                ServiceSupport.dispose(localTransport);
                ServiceSupport.dispose(remoteTransport);
                LOG.warn("Could not start network bridge between: " + localURIName + " and: " + uri + " due to: " + e);
                LOG.debug("Start failure exception: " + e, e);
                try {
                    discoveryAgent.serviceFailed(event);
                } catch (IOException e1) {
                    LOG.debug("Discovery agent failure while handling failure event: " + e1.getMessage(), e1);
                }
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
                LOG.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }

            NetworkBridge bridge = bridges.remove(uri);
            if (bridge == null) {
                return;
            }

            ServiceSupport.dispose(bridge);
        }
    }

    public DiscoveryAgent getDiscoveryAgent() {
        return discoveryAgent;
    }

    public void setDiscoveryAgent(DiscoveryAgent discoveryAgent) {
        this.discoveryAgent = discoveryAgent;
        if (discoveryAgent != null) {
            this.discoveryAgent.setDiscoveryListener(this);
        }
    }

    protected void handleStart() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("You must configure the 'discoveryAgent' property");
        }
        this.discoveryAgent.start();
        super.handleStart();
    }

    protected void handleStop(ServiceStopper stopper) throws Exception {
        for (Iterator<NetworkBridge> i = bridges.values().iterator(); i.hasNext();) {
            NetworkBridge bridge = i.next();
            try {
                bridge.stop();
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        try {
            this.discoveryAgent.stop();
        } catch (Exception e) {
            stopper.onException(this, e);
        }

        super.handleStop(stopper);
    }

    protected NetworkBridge createBridge(Transport localTransport, Transport remoteTransport, final DiscoveryEvent event) {
        NetworkBridgeListener listener = new NetworkBridgeListener() {

            public void bridgeFailed() {
                if (!serviceSupport.isStopped()) {
                    try {
                        discoveryAgent.serviceFailed(event);
                    } catch (IOException e) {
                    }
                }

            }

            public void onStart(NetworkBridge bridge) {
                registerNetworkBridgeMBean(bridge);
            }

            public void onStop(NetworkBridge bridge) {
                unregisterNetworkBridgeMBean(bridge);
            }

        };
        DemandForwardingBridge result = NetworkBridgeFactory.createBridge(this, localTransport, remoteTransport, listener);
        result.setBrokerService(getBrokerService());
        return configureBridge(result);
    }

    public String getName() {
        String name = super.getName();
        if (name == null) {
            name = discoveryAgent.toString();
            super.setName(name);
        }
        return name;
    }

    @Override
    public String toString() {
        return "DiscoveryNetworkConnector:" + getName() + ":" + getBrokerService();
    }
}
