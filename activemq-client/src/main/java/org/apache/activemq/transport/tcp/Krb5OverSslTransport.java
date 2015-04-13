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

import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Doc?
 */
public class Krb5OverSslTransport extends SslTransport {
    public static final String KRB5_CONFIG_NAME = "krb5ConfigName";

    // Cipher Suites as in RFC2712 excluding those which were failing:
    // TLS_KRB5_WITH_IDEA_CBC_SHA, TLS_KRB5_WITH_IDEA_CBC_MD5
    public static final String KRB5_CIPHERS = "TLS_KRB5_WITH_DES_CBC_SHA,TLS_KRB5_WITH_3DES_EDE_CBC_SHA,TLS_KRB5_WITH_RC4_128_SHA,TLS_KRB5_WITH_DES_CBC_MD5,TLS_KRB5_WITH_3DES_EDE_CBC_MD5,TLS_KRB5_WITH_RC4_128_MD5";


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
    public Krb5OverSslTransport(WireFormat wireFormat, SSLSocketFactory socketFactory, URI remoteLocation, URI localLocation) throws IOException, URISyntaxException {
        super(wireFormat, socketFactory, remoteLocation, localLocation, false);
    }

    /**
     * Initialize from a ServerSocket. No access to needClientAuth is given
     * since it is already set within the provided socket.
     *
     * @param wireFormat The WireFormat to be used.
     * @param socket The Socket to be used. Forcing SSL.
     * @throws IOException If TcpTransport throws.
     */
    public Krb5OverSslTransport(WireFormat wireFormat, SSLSocket socket) throws IOException, URISyntaxException {
        super(wireFormat, socket);
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

    public static Subject getSecuritySubject(URI location) {
        String krb5ConfigName = null;

        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            krb5ConfigName = options.get(KRB5_CONFIG_NAME);
            if (krb5ConfigName == null) {
                throw new IllegalStateException("No security context established and no '" + KRB5_CONFIG_NAME + "' URL parameter is set!");
            }

            LoginContext loginCtx = new LoginContext(krb5ConfigName);
            loginCtx.login();

            return loginCtx.getSubject();
        } catch (LoginException e) {
            throw new RuntimeException("Cannot authenticate using Login configuration: " + krb5ConfigName, e);
        } catch (URISyntaxException e1) {
            throw new RuntimeException(e1);
        }
    }
}
