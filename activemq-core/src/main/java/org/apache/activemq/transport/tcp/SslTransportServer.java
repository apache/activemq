/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.transport.tcp;

import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.transport.Transport;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;

/**
 *  An SSL TransportServer.
 * 
 *  Allows for client certificate authentication (refer to setNeedClientAuth for
 *      details).
 *  NOTE: Client certificate authentication is disabled by default. 
 *
 */
public class SslTransportServer extends TcpTransportServer {
    
    // Specifies if sockets created from this server should needClientAuth.
    private boolean needClientAuth = false;
    
    // Specifies if sockets created from this server should wantClientAuth.
    private boolean wantClientAuth = false;
    
    
    /**
     * Constructor.
     * 
     * @param transportFactory The factory used to create transports when connections arrive.
     * @param location The location of the broker to bind to.
     * @param serverSocketFactory The factory used to create this server.
     * @param needClientAuth States if this server should needClientAuth.
     * @throws IOException passed up from TcpTransportFactory.
     * @throws URISyntaxException passed up from TcpTransportFactory.
     */
    public SslTransportServer(
            SslTransportFactory transportFactory,
            URI location,
            SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory);
    }
    
    /**
     * Setter for needClientAuth.
     * 
     * When set to true, needClientAuth will set SSLSockets' needClientAuth to true forcing clients to provide
     *      client certificates.
     */
    public void setNeedClientAuth(boolean needAuth) {
        this.needClientAuth = needAuth;
    }
    
    /**
     * Getter for needClientAuth.
     */
    public boolean getNeedClientAuth() {
        return this.needClientAuth;
    }
    
    /**
     * Getter for wantClientAuth.
     */
    public boolean getWantClientAuth() {
        return this.wantClientAuth;
    }
    
    /**
     * Setter for wantClientAuth.
     * 
     * When set to true, wantClientAuth will set SSLSockets' wantClientAuth to true forcing clients to provide
     *      client certificates.
     */
    public void setWantClientAuth(boolean wantAuth) {
        this.wantClientAuth = wantAuth;
    }
    
    /**
     * Binds this socket to the previously specified URI.
     * 
     * Overridden to allow for proper handling of needClientAuth.
     * 
     * @throws IOException passed up from TcpTransportServer. 
     */
    public void bind() throws IOException {
        super.bind();
        ((SSLServerSocket)this.serverSocket).setWantClientAuth(wantClientAuth);
        ((SSLServerSocket)this.serverSocket).setNeedClientAuth(needClientAuth);
    }
    
    /**
     * Used to create Transports for this server.
     * 
     * Overridden to allow the use of SslTransports (instead of TcpTransports).
     * 
     * @param socket The incoming socket that will be wrapped into the new Transport. 
     * @param format The WireFormat being used.
     * @return The newly return (SSL) Transport.
     * @throws IOException
     */
    protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
        return new SslTransport(format, (SSLSocket)socket);
    }
}
