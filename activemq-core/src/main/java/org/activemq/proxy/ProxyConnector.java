/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.proxy;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.activemq.Service;
import org.activemq.transport.CompositeTransport;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportAcceptListener;
import org.activemq.transport.TransportFactory;
import org.activemq.transport.TransportServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class ProxyConnector implements Service {

    private static final Log log = LogFactory.getLog(ProxyConnector.class);
    private TransportServer server;
    private URI bind;
    private URI remote;
    private URI localUri;
       
    public void start() throws Exception {
        
        this.getServer().setAcceptListener(new TransportAcceptListener() {
            public void onAccept(Transport localTransport) {
                try {
                    Transport remoteTransport = createRemoteTransport();
                    ProxyConnection connection = new ProxyConnection(localTransport, remoteTransport);
                    connection.start();
                }
                catch (Exception e) {
                    onAcceptError(e);
                }
            }

            public void onAcceptError(Exception error) {
                log.error("Could not accept connection: " + error, error);
            }
        });
        getServer().start();

    }

    public void stop() throws Exception {
        if( this.server!=null ) {
            this.server.stop();
        }
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
        return TransportFactory.bind(null, bind);
    }

    private Transport createRemoteTransport() throws Exception {
        Transport transport = TransportFactory.compositeConnect(remote);
        CompositeTransport ct = (CompositeTransport) transport.narrow(CompositeTransport.class);
        if( ct !=null && localUri!=null ) {
            ct.add(new URI[]{localUri});
        }
        return transport;
    }

}
