/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.https;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.http.HttpClientTransport;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

public class HttpsClientTransport extends HttpClientTransport {

    public HttpsClientTransport(TextWireFormat wireFormat, URI remoteUrl) {
        super(wireFormat, remoteUrl);
    }

    @Override
    protected ClientConnectionManager createClientConnectionManager() {
        PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager(createSchemeRegistry());
        return connectionManager;
    }

    private SchemeRegistry createSchemeRegistry() {

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        try {
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(createSocketFactory(),
                    SSLSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
            schemeRegistry.register(new Scheme("https", getRemoteUrl().getPort(), sslSocketFactory));
            return schemeRegistry;
        } catch (Exception e) {
            throw new IllegalStateException("Failure trying to create scheme registry", e);
        }
    }

    /**
     * Creates a new SSL SocketFactory. The given factory will use user-provided
     * key and trust managers (if the user provided them).
     *
     * @return Newly created (Ssl)SocketFactory.
     * @throws IOException
     */
    protected javax.net.ssl.SSLSocketFactory createSocketFactory() throws IOException {
        if (SslContext.getCurrentSslContext() != null) {
            SslContext ctx = SslContext.getCurrentSslContext();
            try {
                return ctx.getSSLContext().getSocketFactory();
            } catch (Exception e) {
                throw IOExceptionSupport.create(e);
            }
        } else {
            return (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();
        }

    }
}
