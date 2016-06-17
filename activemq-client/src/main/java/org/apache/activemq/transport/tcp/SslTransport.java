/*
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
import java.net.URI;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.HashMap;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A Transport class that uses SSL and client-side certificate authentication.
 * Client-side certificate authentication must be enabled through the
 * constructor. By default, this class will have the same client authentication
 * behavior as the socket it is passed. This class will set ConnectionInfo's
 * transportContext to the SSL certificates of the client. NOTE: Accessor method
 * for needClientAuth was not provided on purpose. This is because
 * needClientAuth's value must be set before the socket is connected. Otherwise,
 * unexpected situations may occur.
 */
public class SslTransport extends TcpTransport {

    /**
     * Connect to a remote node such as a Broker.
     *
     * @param wireFormat The WireFormat to be used.
     * @param socketFactory The socket factory to be used. Forcing SSLSockets
     *                for obvious reasons.
     * @param remoteLocation The remote location.
     * @param localLocation The local location.
     * @param needClientAuth If set to true, the underlying socket will need
     *                client certificate authentication.
     * @throws UnknownHostException If TcpTransport throws.
     * @throws IOException If TcpTransport throws.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public SslTransport(WireFormat wireFormat, SSLSocketFactory socketFactory, URI remoteLocation, URI localLocation, boolean needClientAuth) throws IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
        if (this.socket != null) {
            ((SSLSocket)this.socket).setNeedClientAuth(needClientAuth);

            // Lets try to configure the SSL SNI field.  Handy in case your using
            // a single proxy to route to different messaging apps.

            // On java 1.7 it seems like it can only be configured via reflection.
            // TODO: find out if this will work on java 1.8
            HashMap props = new HashMap();
            props.put("host", remoteLocation.getHost());
            IntrospectionSupport.setProperties(this.socket, props);
        }
    }

    /**
     * Initialize from a ServerSocket. No access to needClientAuth is given
     * since it is already set within the provided socket.
     *
     * @param wireFormat The WireFormat to be used.
     * @param socket The Socket to be used. Forcing SSL.
     * @throws IOException If TcpTransport throws.
     */
    public SslTransport(WireFormat wireFormat, SSLSocket socket) throws IOException {
        super(wireFormat, socket);
    }

    public SslTransport(WireFormat format, SSLSocket socket,
            InitBuffer initBuffer) throws IOException {
        super(format, socket, initBuffer);
    }

    /**
     * Overriding in order to add the client's certificates to ConnectionInfo
     * Commmands.
     *
     * @param command The Command coming in.
     */
    @Override
    public void doConsume(Object command) {
        // The instanceof can be avoided, but that would require modifying the
        // Command clas tree and that would require too much effort right
        // now.
        if (command instanceof ConnectionInfo) {
            ConnectionInfo connectionInfo = (ConnectionInfo)command;
            connectionInfo.setTransportContext(getPeerCertificates());
        }
        super.doConsume(command);
    }

    /**
     * @return peer certificate chain associated with the ssl socket
     */
    @Override
    public X509Certificate[] getPeerCertificates() {

        SSLSocket sslSocket = (SSLSocket)this.socket;

        SSLSession sslSession = sslSocket.getSession();

        X509Certificate[] clientCertChain;
        try {
            clientCertChain = (X509Certificate[])sslSession.getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            clientCertChain = null;
        }

        return clientCertChain;
    }

    /**
     * @return pretty print of 'this'
     */
    @Override
    public String toString() {
        return "ssl://" + socket.getInetAddress() + ":" + socket.getPort();
    }
}
