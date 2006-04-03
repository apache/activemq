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

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * A network connector which uses a discovery agent to detect the remote brokers
 * available and setup a connection to each available remote broker
 * 
 * @org.apache.xbean.XBean element="networkConnector"
 * 
 * @version $Revision$
 */
public class DiscoveryNetworkConnector extends NetworkConnector implements DiscoveryListener {

    private DiscoveryAgent discoveryAgent;
    private ConcurrentHashMap bridges = new ConcurrentHashMap();
    
    public DiscoveryNetworkConnector() {
    }

    public DiscoveryNetworkConnector(URI discoveryURI) throws IOException {
        setUri(discoveryURI);
    }

    public void setUri(URI discoveryURI) throws IOException {
        setDiscoveryAgent(DiscoveryAgentFactory.createDiscoveryAgent(discoveryURI));
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

            // Should we try to connect to that URI?
            if (    bridges.containsKey(uri) 
                    || localURI.equals(uri) 
                    || (connectionFilter!=null && !connectionFilter.connectTo(uri))
                    )
                return;

            URI connectUri = uri;
            if (failover) {
                try {
                    connectUri = new URI("failover:" + connectUri);
                }
                catch (URISyntaxException e) {
                    log.warn("Could not create failover URI: " + connectUri);
                    return;
                }
            }

            log.info("Establishing network connection between from " + localURI + " to " + connectUri);

            Transport localTransport;
            try {
                localTransport = createLocalTransport();
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
            }
            catch (URISyntaxException e) {
                log.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }

            Bridge bridge = (Bridge) bridges.remove(uri);
            if (bridge == null)
                return;

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
            this.discoveryAgent.setBrokerName(getBrokerName());
        }
    }

    public boolean isFailover() {
        return failover;
    }

    public void setFailover(boolean reliable) {
        this.failover = reliable;
    }

    protected void doStart() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("You must configure the 'discoveryAgent' property");
        }
        this.discoveryAgent.start();
        super.doStart();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        for (Iterator i = bridges.values().iterator(); i.hasNext();) {
            Bridge bridge = (Bridge) i.next();
            try {
                bridge.stop();
            }
            catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        try {
            this.discoveryAgent.stop();
        }
        catch (Exception e) {
            stopper.onException(this, e);
        }

        super.doStop(stopper);
    }

    protected Bridge createBridge(Transport localTransport, Transport remoteTransport, final DiscoveryEvent event) {
        DemandForwardingBridge result = null;
        if (conduitSubscriptions) {
            if (dynamicOnly) {
                result = new ConduitBridge(localTransport, remoteTransport) {
                    protected void serviceRemoteException(Exception error) {
                        super.serviceRemoteException(error);
                        try {
                            // Notify the discovery agent that the remote broker
                            // failed.
                            discoveryAgent.serviceFailed(event);
                        }
                        catch (IOException e) {
                        }
                    }
                };
            }
            else {
                result = new DurableConduitBridge(localTransport, remoteTransport) {
                    protected void serviceRemoteException(Exception error) {
                        super.serviceRemoteException(error);
                        try {
                            // Notify the discovery agent that the remote broker
                            // failed.
                            discoveryAgent.serviceFailed(event);
                        }
                        catch (IOException e) {
                        }
                    }
                };
            }
        }
        else {
            result = new DemandForwardingBridge(localTransport, remoteTransport) {
                protected void serviceRemoteException(Exception error) {
                    super.serviceRemoteException(error);
                    try {
                        // Notify the discovery agent that the remote broker
                        // failed.
                        discoveryAgent.serviceFailed(event);
                    }
                    catch (IOException e) {
                    }
                }
            };
        }
        return configureBridge(result);
    }

    protected String createName() {
        return discoveryAgent.toString();
    }

}
