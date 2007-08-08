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

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.ServiceStopper;

import java.net.URI;

/**
 * A network connector which uses some kind of multicast-like transport that
 * communicates with potentially many remote brokers over a single logical
 * {@link Transport} instance such as when using multicast.
 * 
 * This implementation does not depend on multicast at all; any other group
 * based transport could be used.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class MulticastNetworkConnector extends NetworkConnector {

    private Transport localTransport;
    private Transport remoteTransport;
    private URI remoteURI;
    private DemandForwardingBridgeSupport bridge;

    public MulticastNetworkConnector() {
    }

    public MulticastNetworkConnector(URI remoteURI) {
        this.remoteURI = remoteURI;
    }

    // Properties
    // -------------------------------------------------------------------------

    public DemandForwardingBridgeSupport getBridge() {
        return bridge;
    }

    public void setBridge(DemandForwardingBridgeSupport bridge) {
        this.bridge = bridge;
    }

    public Transport getLocalTransport() {
        return localTransport;
    }

    public void setLocalTransport(Transport localTransport) {
        this.localTransport = localTransport;
    }

    public Transport getRemoteTransport() {
        return remoteTransport;
    }

    /**
     * Sets the remote transport implementation
     */
    public void setRemoteTransport(Transport remoteTransport) {
        this.remoteTransport = remoteTransport;
    }

    public URI getRemoteURI() {
        return remoteURI;
    }

    /**
     * Sets the remote transport URI to some group transport like
     * <code>multicast://address:port</code>
     */
    public void setRemoteURI(URI remoteURI) {
        this.remoteURI = remoteURI;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected void handleStart() throws Exception {
        if (remoteTransport == null) {
            if (remoteURI == null) {
                throw new IllegalArgumentException("You must specify the remoteURI property");
            }
            remoteTransport = TransportFactory.connect(remoteURI);
        }

        if (localTransport == null) {
            localTransport = createLocalTransport();
        }

        bridge = createBridge(localTransport, remoteTransport);
        configureBridge(bridge);
        bridge.start();

        // we need to start the transports after we've created the bridge
        remoteTransport.start();
        localTransport.start();

        super.handleStart();
    }

    protected void handleStop(ServiceStopper stopper) throws Exception {
        super.handleStop(stopper);
        if (bridge != null) {
            try {
                bridge.stop();
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        if (remoteTransport != null) {
            try {
                remoteTransport.stop();
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        if (localTransport != null) {
            try {
                localTransport.stop();
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
    }

    public String getName() {
        return remoteTransport.toString();
    }

    protected DemandForwardingBridgeSupport createBridge(Transport local, Transport remote) {
        return new CompositeDemandForwardingBridge(this, local, remote);
    }

}
