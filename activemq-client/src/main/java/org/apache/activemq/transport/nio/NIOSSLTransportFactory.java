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
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOSSLTransportFactory extends NIOTransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOSSLTransportFactory.class);

    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return createTcpTransportServer(location, serverSocketFactory, null);
    }

    /**
     * Allows subclasses to create custom instances of the transport server
     * using the given SSLContext.
     *
     * @param location
     * @param serverSocketFactory
     * @param context the SSLContext to use for accepted connections, or null to use the JVM default.
     * @return a new TcpTransportServer initialized from the given location, socket factory and SSLContext.
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory, SSLContext context) throws IOException, URISyntaxException {
        return new NIOSSLTransportServer(context, this, location, serverSocketFactory);
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

    @Override
    public Transport doConnect(URI location, SslContext sslContext) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            if (!options.containsKey("wireFormat.host")) {
                options.put("wireFormat.host", location.getHost());
            }
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf, sslContext);
            Transport rc = configure(transport, wf, options);
            IntrospectionSupport.extractProperties(options, "auto.");
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public Transport doCompositeConnect(URI location, SslContext sslContext) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf, sslContext);
            Transport rc = compositeConfigure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
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
     * Overriding to use SslTransports.
     */
    @Override
    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        return createTransport(location, wf, null);
    }

    protected Transport createTransport(URI location, WireFormat wf, SslContext sslContext) throws UnknownHostException, IOException {
        URI localLocation = null;
        String path = location.getPath();
        // see if the path is a local URI location
        if (path != null && path.length() > 0) {
            int localPortIndex = path.indexOf(':');
            try {
                Integer.parseInt(path.substring(localPortIndex + 1, path.length()));
                String localString = location.getScheme() + ":/" + path;
                localLocation = new URI(localString);
            } catch (Exception e) {
                LOG.warn("path isn't a valid local location for SslTransport to use", e);
            }
        }
        SSLSocketFactory socketFactory = createSslSocketFactory(sslContext);
        return new SslTransport(wf, socketFactory, location, localLocation, false);
    }

    protected SSLSocketFactory createSslSocketFactory(SslContext sslContext) throws IOException {
        if (sslContext != null) {
            try {
                return sslContext.getSSLContext().getSocketFactory();
            } catch (Exception e) {
                throw IOExceptionSupport.create(e);
            }
        }
        return (SSLSocketFactory) SSLSocketFactory.getDefault();
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
