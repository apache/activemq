/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.transport.nio;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.Krb5OverSslTransport;
import org.apache.activemq.transport.tcp.Krb5OverSslTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public class NIOSASLTransportFactory extends NIOTransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOSASLTransportFactory.class);

    @Override
    protected TcpTransportServer createTransportServer(URI location) throws IOException, URISyntaxException {
        return new NIOSASLTransportServer(this, location, createServerSocketFactory());
    }

    @Override
    public Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
        return super.configure(transport, wf, options);
    }

    /**
     * Overriding to use {@link Krb5OverSslTransport} additionally performing
     * login if necesseray.
     */
    @Override
    protected Transport createTransport(final URI location, final WireFormat wf) throws IOException {
        String path = location.getPath();
        final URI localLocation = getLocalLocation(location, path);
        final SocketFactory socketFactory = createSocketFactory();

        final Subject subject = Krb5OverSslTransport.getSecuritySubject(location); //FIXME this shouldn't be like that

        return Subject.doAs(subject, new PrivilegedAction<Transport>() {
            public Transport run() {
                try {
                    Map<String, Object> props = new HashMap<>();
                    props.put(Sasl.QOP, "auth-conf,auth-int,auth"); //FIXME PARAMS

                    //FIXME HOSTNAME
                    SaslClient saslClient = Sasl.createSaslClient(new String[] {"GSSAPI"}, null, "host", InetAddress.getLocalHost().getHostName(), props, new NIOSASLTransportServer.TestCallbackHandler());
                    return new NIOSASLTransport(subject, saslClient, wf, socketFactory, location, localLocation);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * See if the path is a local URI location
     *
     * @param location
     * @param path
     * @return
     */
    protected URI getLocalLocation(final URI location, String path) {
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
