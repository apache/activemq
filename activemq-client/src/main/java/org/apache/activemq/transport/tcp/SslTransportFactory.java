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
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * An implementation of the TcpTransportFactory using SSL. The major
 * contribution from this class is that it is aware of SslTransportServer and
 * SslTransport classes. All Transports and TransportServers created from this
 * factory will have their needClientAuth option set to false.
 */
public class SslTransportFactory extends TcpTransportFactory {

    /**
     * Overriding to derive the SSL server socket factory from the given
     * SslContext, falling back to the JVM default when none is supplied.
     */
    @Override
    protected ServerSocketFactory createServerSocketFactory(SslContext sslContext) throws IOException {
        SSLContext context = toSSLContext(sslContext);
        return context != null ? context.getServerSocketFactory() : createServerSocketFactory();
    }

    /**
     * Overriding to use SslTransportServer and allow for proper reflection.
     */
    @Override
    protected TcpTransportServer createTcpTransportServer(final URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return createSslTransportServer(location, (SSLServerSocketFactory) serverSocketFactory);
    }

    /**
     * Allows subclasses of SslTransportFactory to create custom instances of
     * SslTransportServer.
     *
     * @param location
     * @param serverSocketFactory
     * @return a new SslTransportServer initialized from the given location and socket factory.
     * @throws IOException
     * @throws URISyntaxException
     */
    protected SslTransportServer createSslTransportServer(final URI location, SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new SslTransportServer(this, location, serverSocketFactory);
    }

    /**
     * Overriding to allow for proper configuration through reflection but delegate to get common
     * configuration
     */
    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        SslTransport sslTransport = transport.narrow(SslTransport.class);
        IntrospectionSupport.setProperties(sslTransport, options);

        return super.compositeConfigure(transport, format, options);
    }

    /**
     * Overriding to derive the SSL socket factory from the given SslContext,
     * falling back to the JVM default when none is supplied.
     */
    @Override
    protected SocketFactory createSocketFactory(SslContext sslContext) throws IOException {
        SSLContext context = toSSLContext(sslContext);
        return context != null ? context.getSocketFactory() : createSocketFactory();
    }

    /**
     * Overriding to use SslTransports.
     */
    @Override
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new SslTransport(wf, (SSLSocketFactory) socketFactory, location, localLocation, false);
    }

    /**
     * Creates a new SSL ServerSocketFactory. The given factory will use
     * user-provided key and trust managers (if the user provided them).
     *
     * @return Newly created (Ssl)ServerSocketFactory.
     * @throws IOException
     */
    @Override
    protected ServerSocketFactory createServerSocketFactory() throws IOException {
        return SSLServerSocketFactory.getDefault();
    }

    @Override
    protected SocketFactory createSocketFactory() throws IOException {
        return SSLSocketFactory.getDefault();
    }

    @Override
    public SslTransport createTransport(WireFormat wireFormat, Socket socket, InitBuffer initBuffer)
            throws IOException {

        return new SslTransport(wireFormat, (SSLSocket)socket, initBuffer);
    }
}
