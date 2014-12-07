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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the TcpTransportFactory using SSL and Kerberos V5. 
 * The major contribution from this class is that it is aware of 
 * SslTransportServer and SslTransport classes. All Transports and 
 * TransportServers created from this factory will working in Security Subject
 * context thus being able to authenticate user with Kerberos.
 */
public class Krb5OverSslTransportFactory extends SslTransportFactory {
    public static final String KRB5_CONFIG_NAME = "krb5ConfigName";
    private static final Logger LOG = LoggerFactory.getLogger(Krb5OverSslTransportFactory.class);

    protected SslTransportServer createSslTransportServer(final URI location, SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new Krb5OverSslTransportServer(this, location, serverSocketFactory);
    }
    /**
     * Overriding to use {@link Krb5OverSslTransport} additionally performing
     * login if necesseray.
     */
    protected Transport createTransport(final URI location, final WireFormat wf) throws UnknownHostException, IOException {
        String path = location.getPath();
        final URI localLocation = getLocalLocation(location, path);
        final SocketFactory socketFactory = createSocketFactory();


        if (AccessController.getContext() == null || Subject.getSubject(AccessController.getContext()) == null) {
            String krb5ConfigName = null;
            try {
                Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
                krb5ConfigName = options.get(KRB5_CONFIG_NAME);
                if (krb5ConfigName == null) {
                    throw new IllegalStateException("No security context established and no '" + KRB5_CONFIG_NAME + "' URL parameter is set!");
                }

                LoginContext loginCtx = new LoginContext(krb5ConfigName);
                loginCtx.login();

                Subject subject = loginCtx.getSubject();

                return Subject.doAs(subject, new PrivilegedAction<Transport>() {
                    public Transport run() {
                        try {
                            return new Krb5OverSslTransport(wf, (SSLSocketFactory)socketFactory, location, localLocation);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            } catch (LoginException e) {
                throw new RuntimeException("Cannot authenticate using Login configuration: " + krb5ConfigName, e);
            } catch (URISyntaxException e1) {
                throw new RuntimeException(e1);
            }
        }

        return new Krb5OverSslTransport(wf, (SSLSocketFactory)socketFactory, location, localLocation);
    }

    private URI getLocalLocation(final URI location, String path) {
        if (path != null && path.length() > 0) {
            int localPortIndex = path.indexOf(':');
            try {
                Integer.parseInt(path.substring(localPortIndex + 1, path.length()));
                String localString = location.getScheme() + ":/" + path;
                return new URI(localString);
            } catch (Exception e) {
                LOG.warn("path isn't a valid local location for SslTransport to use", e);
            }
        }
        return null;
    }
}
