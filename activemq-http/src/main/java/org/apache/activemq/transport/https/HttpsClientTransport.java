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
import org.apache.activemq.transport.http.marshallers.HttpTransportMarshaller;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpsClientTransport extends HttpClientTransport {

    public HttpsClientTransport(final HttpTransportMarshaller marshaller, URI remoteUrl) {
        super(marshaller, remoteUrl);
    }

    @Override
    protected HttpClientConnectionManager createClientConnectionManager() {
        return new PoolingHttpClientConnectionManager(createRegistry());
    }

    private Registry<ConnectionSocketFactory> createRegistry() {

        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.<ConnectionSocketFactory>create();
        try {
            SSLConnectionSocketFactory sslConnectionFactory = new SSLConnectionSocketFactory(createSocketFactory(), new DefaultHostnameVerifier());
            registryBuilder.register("https", sslConnectionFactory);
            return registryBuilder.build();
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
