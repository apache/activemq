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
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of the TcpTransportFactory using SSL and Kerberos V5. 
 * The major contribution from this class is that it is aware of 
 * SslTransportServer and SslTransport classes. All Transports and 
 * TransportServers created from this factory will working in Security Subject
 * context thus being able to authenticate user with Kerberos.
 */
public class Krb5OverSslTransportFactory extends SslTransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(Krb5OverSslTransportFactory.class);

    @Override
    protected TcpTransportServer createTransportServer(final URI location) throws IOException, URISyntaxException {
        SSLServerSocketFactory sslServerSocketFactory = createServerSocketFactory();
        return new Krb5OverSslTransportServer(this, location, sslServerSocketFactory);
    }
    /**
     * Overriding to use {@link Krb5OverSslTransport} additionally performing
     * login if necesseray.
     */
    protected Transport createTransport(final URI location, final WireFormat wf) throws IOException {
        String path = location.getPath();
        final URI localLocation = getLocalLocation(location, path);
        final SocketFactory socketFactory = createSocketFactory();

        Subject subject = Krb5OverSslTransport.getSecuritySubject(location);

        return Subject.doAs(subject, new PrivilegedAction<Transport>() {
            public Transport run() {
                try {
                    return new Krb5OverSslTransport(wf, (SSLSocketFactory)socketFactory, location, localLocation);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     *
     * Defines default list of socket.* options for this transport
     *
     * @return Default list of socket.* options for this transport
     */
    @Override
    protected Map<String,Object> getDefaultSocketOptions() {
        Map<String,Object> defaultSocketOptions =  super.getDefaultSocketOptions();

        defaultSocketOptions.put("enabledCipherSuites", Krb5OverSslTransport.KRB5_CIPHERS);

        return defaultSocketOptions;
    }
}
