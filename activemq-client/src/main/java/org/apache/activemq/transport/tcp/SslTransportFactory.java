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
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the TcpTransportFactory using SSL. The major
 * contribution from this class is that it is aware of SslTransportServer and
 * SslTransport classes. All Transports and TransportServers created from this
 * factory will have their needClientAuth option set to false.
 */
public class SslTransportFactory extends TcpTransportFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SslTransportFactory.class);

    @Override
    @SuppressWarnings("deprecation")
    public TransportServer doBind(final URI location) throws IOException {
        return doBind(location, SslContext.getCurrentSslContext());
    }

    @Override
    public TransportServer doBind(final URI location, SslContext sslContext) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            SSLServerSocketFactory serverSocketFactory;
            if (sslContext != null) {
                try {
                    serverSocketFactory = sslContext.getSSLContext().getServerSocketFactory();
                } catch (Exception e) {
                    throw IOExceptionSupport.create(e);
                }
            } else {
                serverSocketFactory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
            }
            SslTransportServer server = createSslTransportServer(location, serverSocketFactory);
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
