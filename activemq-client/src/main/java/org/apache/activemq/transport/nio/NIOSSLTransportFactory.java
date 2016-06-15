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
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOSSLTransportFactory extends NIOTransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOSSLTransportFactory.class);

    protected SSLContext context;

    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new NIOSSLTransportServer(context, this, location, serverSocketFactory);
    }

    @Override
    public TransportServer doBind(URI location) throws IOException {
        if (SslContext.getCurrentSslContext() != null) {
            try {
                context = SslContext.getCurrentSslContext().getSSLContext();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return super.doBind(location);
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
        SocketFactory socketFactory = createSocketFactory();
        return new SslTransport(wf, (SSLSocketFactory) socketFactory, location, localLocation, false);
    }

    @Override
    public TcpTransport createTransport(WireFormat wireFormat, Socket socket,
            SSLEngine engine, InitBuffer initBuffer, ByteBuffer inputBuffer)
            throws IOException {
        return new NIOSSLTransport(wireFormat, socket, engine, initBuffer, inputBuffer);
    }

    /**
     * Creates a new SSL SocketFactory. The given factory will use user-provided
     * key and trust managers (if the user provided them).
     *
     * @return Newly created (Ssl)SocketFactory.
     * @throws IOException
     */
    @Override
    protected SocketFactory createSocketFactory() throws IOException {
        if (SslContext.getCurrentSslContext() != null) {
            SslContext ctx = SslContext.getCurrentSslContext();
            try {
                return ctx.getSSLContext().getSocketFactory();
            } catch (Exception e) {
                throw IOExceptionSupport.create(e);
            }
        } else {
            return SSLSocketFactory.getDefault();
        }

    }

}
