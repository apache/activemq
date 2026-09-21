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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.activemq.TransportLoggerSupport;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpTransportFactory extends TransportFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportFactory.class);

    @Override
    public TransportServer doBind(final URI location) throws IOException {
        return doBind(location, null);
    }

    /**
     * Binds a TCP based transport server. The given {@link SslContext} is
     * handed to {@link #createServerSocketFactory(SslContext)} and
     * {@link #createTcpTransportServer(URI, ServerSocketFactory, SslContext)}
     * so SSL capable subclasses can derive their socket factory and server
     * from it; plain TCP ignores it. The broker always binds through this
     * method, so subclasses customizing the bind must override it rather
     * than {@link #doBind(URI)}.
     */
    @Override
    public TransportServer doBind(final URI location, SslContext sslContext) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            ServerSocketFactory serverSocketFactory = createServerSocketFactory(sslContext);
            TcpTransportServer server = createTcpTransportServer(location, serverSocketFactory, sslContext);
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

    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransportServer.
     *
     * @param location
     * @param serverSocketFactory
     * @return a new TcpTransportServer instance.
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TcpTransportServer createTcpTransportServer(final URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory);
    }

    /**
     * Allows SSL capable subclasses to create a TcpTransportServer that uses
     * the given SslContext for accepted connections. The default ignores the
     * context and delegates to
     * {@link #createTcpTransportServer(URI, ServerSocketFactory)}.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    protected TcpTransportServer createTcpTransportServer(final URI location, ServerSocketFactory serverSocketFactory, SslContext sslContext) throws IOException, URISyntaxException {
        return createTcpTransportServer(location, serverSocketFactory);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {

        TcpTransport tcpTransport = transport.narrow(TcpTransport.class);
        IntrospectionSupport.setProperties(tcpTransport, options);

        Map<String, Object> socketOptions = IntrospectionSupport.extractProperties(options, "socket.");
        tcpTransport.setSocketOptions(socketOptions);

        if (tcpTransport.isTrace()) {
            try {
                transport = TransportLoggerSupport.createTransportLogger(transport, tcpTransport.getLogWriterName(), tcpTransport.isDynamicManagement(), tcpTransport.isStartLogging(), tcpTransport.getJmxPort());
            } catch (Throwable e) {
                LOG.error("Could not create TransportLogger object for: " + tcpTransport.getLogWriterName() + ", reason: " + e, e);
            }
        }

        boolean useInactivityMonitor = "true".equals(getOption(options, "useInactivityMonitor", "true"));
        if (useInactivityMonitor && isUseInactivityMonitor(transport)) {
            transport = createInactivityMonitor(transport, format);
            IntrospectionSupport.setProperties(transport, options);
        }

        // Only need the WireFormatNegotiator if using openwire
        if (format instanceof OpenWireFormat) {
            transport = new WireFormatNegotiator(transport, (OpenWireFormat)format, tcpTransport.getMinmumWireFormatVersion());
        }

        return super.compositeConfigure(transport, format, options);
    }


    /**
     * @return true if the inactivity monitor should be used on the transport
     */
    protected boolean isUseInactivityMonitor(Transport transport) {
        return true;
    }

    /**
     * Connects a TCP based transport. The given {@link SslContext} is threaded
     * through {@link #doConnectInternal} to {@link #createTransport(URI, WireFormat, SslContext)}
     * (and thence {@link #createSocketFactory(SslContext)}) so SSL capable
     * subclasses can derive their socket factory from it; plain TCP ignores it.
     * The plain {@code doConnect(URI)}/{@code doCompositeConnect(URI)} are
     * inherited from {@link TransportFactory}, which routes through the same
     * createTransport(URI, WireFormat, SslContext) override with a null context.
     */
    @Override
    public Transport doConnect(URI location, SslContext sslContext) throws IOException {
        return doConnectInternal(location, sslContext, false);
    }

    @Override
    public Transport doCompositeConnect(URI location, SslContext sslContext) throws IOException {
        return doConnectInternal(location, sslContext, true);
    }

    @Override
    protected Transport createTransport(URI location, WireFormat wf) throws IOException {
        return createTransport(location, wf, null);
    }

    /**
     * Creates the client side transport for the given location, deriving the
     * socket factory from the given SslContext via
     * {@link #createSocketFactory(SslContext)}.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    protected Transport createTransport(URI location, WireFormat wf, SslContext sslContext) throws IOException {
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
                LOG.warn("path isn't a valid local location for TcpTransport to use: {}", e.getMessage());
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Failure detail", e);
                }
            }
        }
        SocketFactory socketFactory = createSocketFactory(sslContext);
        return createTcpTransport(wf, socketFactory, location, localLocation);
    }

    public TcpTransport createTransport(WireFormat wireFormat, Socket socket, InitBuffer initBuffer) throws IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    public TcpTransport createTransport(WireFormat wireFormat, Socket socket,
            SSLEngine engine, InitBuffer initBuffer, ByteBuffer inputBuffer) throws IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    /**
     * Allows subclasses of TcpTransportFactory to provide a create custom
     * TcpTransport instances.
     *
     * @param wf
     * @param socketFactory
     * @param location
     * @param localLocation
     *
     * @return a new TcpTransport instance connected to the given location.
     *
     * @throws IOException
     */
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws IOException {
        return new TcpTransport(wf, socketFactory, location, localLocation);
    }

    protected ServerSocketFactory createServerSocketFactory() throws IOException {
        return ServerSocketFactory.getDefault();
    }

    /**
     * Allows SSL capable subclasses to derive the ServerSocketFactory from the
     * given SslContext. The default ignores the context and delegates to
     * {@link #createServerSocketFactory()}.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    protected ServerSocketFactory createServerSocketFactory(SslContext sslContext) throws IOException {
        return createServerSocketFactory();
    }

    /**
     * Resolves the SSLContext held by the given SslContext.
     *
     * @return the SSLContext, or null when no SslContext was supplied.
     * @throws IOException if the SslContext cannot produce an SSLContext.
     */
    protected static SSLContext toSSLContext(SslContext sslContext) throws IOException {
        if (sslContext == null) {
            return null;
        }
        try {
            return sslContext.getSSLContext();
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    protected SocketFactory createSocketFactory() throws IOException {
        return SocketFactory.getDefault();
    }

    /**
     * Allows SSL capable subclasses to derive the SocketFactory from the given
     * SslContext. The default ignores the context and delegates to
     * {@link #createSocketFactory()}.
     *
     * @param sslContext the SslContext to use, or null for the JVM default.
     */
    protected SocketFactory createSocketFactory(SslContext sslContext) throws IOException {
        return createSocketFactory();
    }

    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
        return new InactivityMonitor(transport, format);
    }
}
