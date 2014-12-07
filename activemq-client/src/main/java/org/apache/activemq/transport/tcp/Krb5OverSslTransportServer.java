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

package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 *  An SSL TransportServer.
 *
 *  Allows for client certificate authentication (refer to setNeedClientAuth for
 *      details).
 *  NOTE: Client certificate authentication is disabled by default.
 *
 */
public class Krb5OverSslTransportServer extends SslTransportServer {
    private URI location;

    /**
     * Creates a ssl transport server for the specified url using the provided
     * serverSocketFactory
     *
     * @param transportFactory The factory used to create transports when connections arrive.
     * @param location The location of the broker to bind to.
     * @param serverSocketFactory The factory used to create this server.
     * @throws IOException passed up from TcpTransportFactory.
     * @throws URISyntaxException passed up from TcpTransportFactory.
     */
    public Krb5OverSslTransportServer(SslTransportFactory transportFactory, URI location, SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory);
        this.location = location;
    }

    @Override
    public void run() {
        if (AccessController.getContext() == null || Subject.getSubject(AccessController.getContext()) == null) {
            String krb5ConfigName = null;
            try {
                Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
                krb5ConfigName = options.get(Krb5OverSslTransportFactory.KRB5_CONFIG_NAME);
                if (krb5ConfigName == null) {
                    throw new IllegalStateException("No security context established and no '" + Krb5OverSslTransportFactory.KRB5_CONFIG_NAME + "' URL parameter is set!");
                }
        
                LoginContext loginCtx = new LoginContext(krb5ConfigName);
                loginCtx.login();
    
                Subject subject = loginCtx.getSubject();
    
                Subject.doAs(subject, new PrivilegedAction<Void>() {
                    public Void run() {
                        try {
                            Krb5OverSslTransportServer.super.run();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                });
            } catch (LoginException e) {
                throw new RuntimeException("Cannot authenticate using Login configuration: " + krb5ConfigName, e);
            } catch (URISyntaxException e1) {
                throw new RuntimeException(e1);
            }
        } else {
            super.run();
        }
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
        return new Krb5OverSslTransport(format, (SSLSocket)socket, location);
    }
}
