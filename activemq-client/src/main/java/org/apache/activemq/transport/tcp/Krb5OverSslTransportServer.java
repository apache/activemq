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

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.security.auth.Subject;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.HashMap;

/**
 *  An KRB5 TransportServer.
 *
 *  Implementation based on SSL TransportServer and
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
        Subject subject = Krb5OverSslTransport.getSecuritySubject(location);

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
    }

    /**
     * Used to create Transports for this server.
     *
     * Overridden to allow the use of Krb5OverSslTransports (instead of SslTransports).
     *
     * @param socket The incoming socket that will be wrapped into the new Transport.
     * @param format The WireFormat being used.
     * @return The newly return (SSL) Transport.
     * @throws IOException
     */
    protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
        try {
            return new Krb5OverSslTransport(format, (SSLSocket)socket);
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Adds enabledCipherSuites default values to default Transport Options map.
     *
     * @return default transport options map for this transport
     */
    @Override
    protected HashMap<String, Object> getDefaultTransportOptions() {
        HashMap<String, Object> defaultOptions = super.getDefaultTransportOptions();

        defaultOptions.put("enabledCipherSuites", Krb5OverSslTransport.KRB5_CIPHERS);

        return defaultOptions;
    }
}
