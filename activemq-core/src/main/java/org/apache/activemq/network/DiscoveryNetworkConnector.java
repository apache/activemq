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

import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;

import java.util.concurrent.ConcurrentHashMap;

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

    public void onServiceAdd(DiscoveryEvent event){
        String localURIName=localURI.getScheme() + "://" + localURI.getHost();
        // Ignore events once we start stopping.
        if(serviceSupport.isStopped()||serviceSupport.isStopping())
            return;
        String url=event.getServiceName();
        if(url!=null){
            URI uri;
            try{
                uri=new URI(url);
            }catch(URISyntaxException e){
                log.warn("Could not connect to remote URI: "+url+" due to bad URI syntax: "+e,e);
                return;
            }
            // Should we try to connect to that URI?
            if(bridges.containsKey(uri)||localURI.equals(uri)
                    ||(connectionFilter!=null&&!connectionFilter.connectTo(uri)))
                return;
            URI connectUri=uri;
            log.info("Establishing network connection between from "+localURIName+" to "+connectUri);
            Transport remoteTransport;
            try{
                remoteTransport=TransportFactory.connect(connectUri);
            }catch(Exception e){
                log.warn("Could not connect to remote URI: "+localURIName+": "+e.getMessage());
                log.debug("Connection failure exception: "+e,e);
                return;
            }
            Transport localTransport;
            try{
                localTransport=createLocalTransport();
            }catch(Exception e){
                ServiceSupport.dispose(remoteTransport);
                log.warn("Could not connect to local URI: "+localURIName+": "+e.getMessage());
                log.debug("Connection failure exception: "+e,e);
                return;
            }
            NetworkBridge bridge=createBridge(localTransport,remoteTransport,event);
            bridges.put(uri,bridge);
            try{
                bridge.start();
            }catch(Exception e){
                ServiceSupport.dispose(localTransport);
                ServiceSupport.dispose(remoteTransport);
                log.warn("Could not start network bridge between: "+localURIName+" and: "+uri+" due to: "+e);
                log.debug("Start failure exception: "+e,e);
                try{
                    discoveryAgent.serviceFailed(event);
                }catch(IOException e1){
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
            }
            catch (URISyntaxException e) {
                log.warn("Could not connect to remote URI: " + url + " due to bad URI syntax: " + e, e);
                return;
            }

            NetworkBridge bridge = (NetworkBridge) bridges.remove(uri);
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

    protected void handleStart() throws Exception {
        if (discoveryAgent == null) {
            throw new IllegalStateException("You must configure the 'discoveryAgent' property");
        }
        this.discoveryAgent.start();
        super.handleStart();
    }

    protected void handleStop(ServiceStopper stopper) throws Exception {
        for (Iterator i = bridges.values().iterator(); i.hasNext();) {
            NetworkBridge bridge = (NetworkBridge) i.next();
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

        super.handleStop(stopper);
    }

    protected NetworkBridge createBridge(Transport localTransport, Transport remoteTransport, final DiscoveryEvent event) {
        NetworkBridgeListener listener = new NetworkBridgeListener() {

            public void bridgeFailed(){
                if( !serviceSupport.isStopped() ) {
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
        DemandForwardingBridge result = NetworkBridgeFactory.createBridge(this,localTransport,remoteTransport,listener);
        return configureBridge(result);
    }

    public String getName() {
        return discoveryAgent.toString();
    }

   

}
