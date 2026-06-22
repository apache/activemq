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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.nio.NIOSSLTransportServer;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

public class StompNIOSSLTransportFactory extends StompNIOTransportFactory {

    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return createTcpTransportServer(location, serverSocketFactory, null);
    }

    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory, final SSLContext context) throws IOException, URISyntaxException {
        return new NIOSSLTransportServer(context, this, location, serverSocketFactory) {

            @Override
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                StompNIOSSLTransport transport = new StompNIOSSLTransport(format, socket);
                if (context != null) {
                    transport.setSslContext(context);
                }

                transport.setNeedClientAuth(isNeedClientAuth());
                transport.setWantClientAuth(isWantClientAuth());

                return transport;
            }
        };
    }

    @Override
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new StompNIOSSLTransport(wf, socketFactory, location, localLocation);
    }

    @Override
    public TcpTransport createTransport(WireFormat wireFormat, Socket socket,
            SSLEngine engine, InitBuffer initBuffer, ByteBuffer inputBuffer)
            throws IOException {
        return new StompNIOSSLTransport(wireFormat, socket, engine, initBuffer, inputBuffer);
    }

    @Override
    public TransportServer doBind(URI location) throws IOException {
        return doBind(location, null);
    }

    @Override
    public TransportServer doBind(URI location, SslContext sslContext) throws IOException {
        SSLContext context = null;
        if (sslContext != null) {
            try {
                context = sslContext.getSSLContext();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            ServerSocketFactory serverSocketFactory = createServerSocketFactory();
            TcpTransportServer server = createTcpTransportServer(location, serverSocketFactory, context);
            server.setWireFormatFactory(createWireFormatFactory(options));
            IntrospectionSupport.setProperties(server, options);
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            server.setTransportOption(transportOptions);
            server.bind();

            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }
}
