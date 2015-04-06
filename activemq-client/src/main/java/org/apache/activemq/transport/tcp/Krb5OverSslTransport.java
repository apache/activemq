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

import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

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
public class Krb5OverSslTransport extends SslTransport {
    public static final String KRB5_CIPHER = "TLS_KRB5_WITH_3DES_EDE_CBC_SHA";

    /**
     * Connect to a remote node such as a Broker.
     * 
     * @param wireFormat The WireFormat to be used.
     * @param socketFactory The socket factory to be used. Forcing SSLSockets
     *                for obvious reasons.
     * @param remoteLocation The remote location.
     * @param localLocation The local location.
     * @throws UnknownHostException If TcpTransport throws.
     * @throws IOException If TcpTransport throws.
     */
    public Krb5OverSslTransport(WireFormat wireFormat, SSLSocketFactory socketFactory, URI remoteLocation, URI localLocation) throws IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation, false);
        configureTransport(remoteLocation);
    }
    
    /**
     * Initialize from a ServerSocket. No access to needClientAuth is given
     * since it is already set within the provided socket.
     * 
     * @param wireFormat The WireFormat to be used.
     * @param socket The Socket to be used. Forcing SSL.
     * @throws IOException If TcpTransport throws.
     */
    public Krb5OverSslTransport(WireFormat wireFormat, SSLSocket socket, URI remoteLocation) throws IOException {
        super(wireFormat, socket);
        configureTransport(remoteLocation);
    }

    private void configureTransport(URI remoteLocation) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(remoteLocation));
            IntrospectionSupport.setProperties(this, options);
            String enabledSuites[] = { Krb5OverSslTransport.KRB5_CIPHER };
            ((SSLSocket)socket).setEnabledCipherSuites(enabledSuites);
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * @return pretty print of 'this'
     */
    public String toString() {
        return "krb5://" + socket.getInetAddress() + ":" + socket.getPort();
    }

    public void setKrb5ConfigName(String krb5ConfigName) {
        //no-op just a workaround
    }

}
