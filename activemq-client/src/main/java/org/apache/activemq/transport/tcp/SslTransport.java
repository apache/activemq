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
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.Collections;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.command.ConnectionInfo;
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
     * Default to null as there are different defaults between server and client, initialiseSocket
     * for more details
     */
    private Boolean verifyHostName = null;

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
        }
    }

    @Override
    protected void initialiseSocket(Socket sock) throws SocketException, IllegalArgumentException {
        /**
         * This needs to default to null because this transport class is used for both a server transport
         * and a client connection and we have different defaults for both.
         * If we default it to a value it might override the transport server setting
         * that was configured inside TcpTransportServer (which sets a default to false for server side)
         *
         * The idea here is that if this is a server transport then verifyHostName will be set by the setter
         * and not be null as TcpTransportServer will set a default value of false (or a user will set it
         * using transport.verifyHostName) but if this is a client connection the value will be null by default
         * and will stay null if the user uses socket.verifyHostName to set the value or doesn't use the setter
         * If it is null then we can check socketOptions for the value and if not set there then we can
         * just set a default of true as this will be a client
         *
         * Unfortunately we have to do this to stay consistent because every other SSL option on the client
         * side can be configured using socket. but this particular option isn't actually part of the socket
         * so it makes it tricky from a user standpoint. For consistency sake I think it makes sense to allow
         * using the socket. prefix that has been established so users do not get confused (as well as
         * allow using no prefix which just calls the setter directly)
         *
         * Because of this there are actually two ways a client can configure this value, the client can either use
         * socket.verifyHostName=<value> as mentioned or just simply use verifyHostName=<value> without using the socket.
         * prefix and that will also work as the value will be set using the setter on the transport
         *
         * example server transport config:
         *  ssl://localhost:61616?transport.verifyHostName=true
         *
         * example from client:
         *  ssl://localhost:61616?verifyHostName=true
         *                  OR
         *  ssl://localhost:61616?socket.verifyHostName=true
         *
         */
        if (verifyHostName == null) {
            //Check to see if the user included the value as part of socket options and if so then use that value
            if (socketOptions != null && socketOptions.containsKey("verifyHostName")) {
                verifyHostName = Boolean.parseBoolean(socketOptions.get("verifyHostName").toString());
                socketOptions.remove("verifyHostName");
            } else {
                //If null and not set then this is a client so default to true
                verifyHostName = true;
            }
        }

        // Lets try to configure the SSL SNI field.  Handy in case your using
        // a single proxy to route to different messaging apps.
        final SSLParameters sslParams = new SSLParameters();
        if (remoteLocation != null) {
            sslParams.setServerNames(Collections.singletonList(new SNIHostName(remoteLocation.getHost())));
        }

        if (verifyHostName) {
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
        }

        if (remoteLocation != null || verifyHostName) {
            // AMQ-8445 only set SSLParameters if it has been populated before
            ((SSLSocket) this.socket).setSSLParameters(sslParams);
        }

        super.initialiseSocket(sock);
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

    public void setVerifyHostName(Boolean verifyHostName) {
        this.verifyHostName = verifyHostName;
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
