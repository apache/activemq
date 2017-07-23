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
package org.apache.activemq.transport.nio;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.Krb5OverSslTransport;
import org.apache.activemq.transport.tcp.Krb5OverSslTransportServer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public class NIOSASLTransportServer extends TcpTransportServer {

    public NIOSASLTransportServer(TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory);
    }

    private boolean needClientAuth;
    private boolean wantClientAuth;


    @Override
    protected Transport createTransport(final Socket socket, final WireFormat format) throws IOException {
        final Subject subject = Krb5OverSslTransport.getSecuritySubject(getConnectURI()); //FIXME this shouldn't be like that

        return Subject.doAs(subject, new PrivilegedAction<NIOSASLTransport>() {
            public NIOSASLTransport run() {
                try {
                    Map<String, Object> props = new HashMap<>();
                    props.put(Sasl.QOP, "auth-conf,auth-int,auth"); //FIXME

                    //FIXME THIS HOSTNAME
                    SaslServer server = Sasl.createSaslServer("GSSAPI", "host", InetAddress.getLocalHost().getHostName(), props, new TestCallbackHandler());
                    return new NIOSASLTransport(subject, server, format, socket);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public boolean isSslServer() {
        return true;
    }

    public boolean isNeedClientAuth() {
        return this.needClientAuth;
    }

    public void setNeedClientAuth(boolean value) {
        this.needClientAuth = value;
    }

    public boolean isWantClientAuth() {
        return this.wantClientAuth;
    }

    public void setWantClientAuth(boolean value) {
        this.wantClientAuth = value;
    }

    public static class TestCallbackHandler implements CallbackHandler {

        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {

            AuthorizeCallback acb = null;

            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof AuthorizeCallback) {
                    acb = (AuthorizeCallback) callbacks[i];
                } else {
                    throw new UnsupportedCallbackException(callbacks[i]);
                }
            }

            if (acb != null) {
                String authid = acb.getAuthenticationID();
                String authzid = acb.getAuthorizationID();
                if (authid.equals(authzid)) {
                    // Self is always authorized
                    acb.setAuthorized(true);

                } else {
                    // Should check some database for mapping and decide.
                    // Current simplified policy is to reject authzids that
                    // don't match authid

                    acb.setAuthorized(false);
                }

                if (acb.isAuthorized()) {
                    // Set canonicalized name.
                    // Should look up database for canonical names

                    acb.setAuthorizedID(authzid);
                }
            }
        }
    }
}
