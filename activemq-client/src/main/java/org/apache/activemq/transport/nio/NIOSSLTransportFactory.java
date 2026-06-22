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

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

public class NIOSSLTransportFactory extends NIOTransportFactory {
    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return createTcpTransportServer(location, serverSocketFactory, null);
    }

    /**
     * Overriding to create an NIO SSL transport server that uses the given
     * SslContext for accepted connections.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory, SslContext sslContext) throws IOException, URISyntaxException {
        return new NIOSSLTransportServer(toSSLContext(sslContext), this, location, serverSocketFactory);
    }

    /**
     * Overriding to allow for proper configuration through reflection but
     * delegate to get common configuration
     */
    @Override
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        if (transport instanceof SslTransport) {
            SslTransport sslTransport = (SslTransport) transport.narrow(SslTransport.class);
            IntrospectionSupport.setProperties(sslTransport, options);
        } else if (transport instanceof NIOSSLTransport) {
            NIOSSLTransport sslTransport = (NIOSSLTransport) transport.narrow(NIOSSLTransport.class);
            IntrospectionSupport.setProperties(sslTransport, options);
        }

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

    @Override
    public TcpTransport createTransport(WireFormat wireFormat, Socket socket,
            SSLEngine engine, InitBuffer initBuffer, ByteBuffer inputBuffer)
            throws IOException {
        return new NIOSSLTransport(wireFormat, socket, engine, initBuffer, inputBuffer);
    }

    @Override
    protected SocketFactory createSocketFactory() throws IOException {
        return SSLSocketFactory.getDefault();
    }

}
