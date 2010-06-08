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
package org.apache.activemq.proxy;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.activemq.Service;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class ProxyConnector implements Service {

    private static final Log LOG = LogFactory.getLog(ProxyConnector.class);
    private TransportServer server;
    private URI bind;
    private URI remote;
    private URI localUri;
    private String name;
    /**
     * Should we proxy commands to the local broker using VM transport as well?
     */
    private boolean proxyToLocalBroker = true;
    
    private final CopyOnWriteArrayList<ProxyConnection> connections = new CopyOnWriteArrayList<ProxyConnection>();

    public void start() throws Exception {

        this.getServer().setAcceptListener(new TransportAcceptListener() {
            public void onAccept(Transport localTransport) {
                try {
                    Transport remoteTransport = createRemoteTransport();
                    ProxyConnection connection = new ProxyConnection(localTransport, remoteTransport);
                    connections.add(connection);
                    connection.start();
                } catch (Exception e) {
                    onAcceptError(e);
                }
            }

            public void onAcceptError(Exception error) {
                LOG.error("Could not accept connection: " + error, error);
            }
        });
        getServer().start();
        LOG.info("Proxy Connector " + getName() + " Started");

    }

    public void stop() throws Exception {
        ServiceStopper ss = new ServiceStopper();
        if (this.server != null) {
            ss.stop(this.server);
        }
        for (Iterator<ProxyConnection> iter = connections.iterator(); iter.hasNext();) {
            LOG.info("Connector stopped: Stopping proxy.");
            ss.stop(iter.next());
        }
        ss.throwFirstException();
        LOG.info("Proxy Connector " + getName() + " Stopped");
    }

    // Properties
    // -------------------------------------------------------------------------

    public URI getLocalUri() {
        return localUri;
    }

    public void setLocalUri(URI localURI) {
        this.localUri = localURI;
    }

    public URI getBind() {
        return bind;
    }

    public void setBind(URI bind) {
        this.bind = bind;
    }

    public URI getRemote() {
        return remote;
    }

    public void setRemote(URI remote) {
        this.remote = remote;
    }

    public TransportServer getServer() throws IOException, URISyntaxException {
        if (server == null) {
            server = createServer();
        }
        return server;
    }

    public void setServer(TransportServer server) {
        this.server = server;
    }

    protected TransportServer createServer() throws IOException, URISyntaxException {
        if (bind == null) {
            throw new IllegalArgumentException("You must specify either a server or the bind property");
        }
        return TransportFactory.bind(bind);
    }

    private Transport createRemoteTransport() throws Exception {
        Transport transport = TransportFactory.compositeConnect(remote);
        CompositeTransport ct = transport.narrow(CompositeTransport.class);
        if (ct != null && localUri != null && proxyToLocalBroker) {
            ct.add(false,new URI[] {localUri});
        }

        // Add a transport filter so that we can track the transport life cycle
        transport = new TransportFilter(transport) {
            @Override
            public void stop() throws Exception {
                LOG.info("Stopping proxy.");
                super.stop();
                connections.remove(this);
            }
        };
        return transport;
    }

    public String getName() {
        if (name == null) {
            if (server != null) {
                name = server.getConnectURI().toString();
            } else {
                name = "proxy";
            }
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isProxyToLocalBroker() {
        return proxyToLocalBroker;
    }

    public void setProxyToLocalBroker(boolean proxyToLocalBroker) {
        this.proxyToLocalBroker = proxyToLocalBroker;
    }

}
