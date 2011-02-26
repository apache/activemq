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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 *
 */
public class TcpTransportFactory extends TransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportFactory.class);

    public TransportServer doBind(final URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            ServerSocketFactory serverSocketFactory = createServerSocketFactory();
            TcpTransportServer server = createTcpTransportServer(location, serverSocketFactory);
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
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TcpTransportServer createTcpTransportServer(final URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory);
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {

        TcpTransport tcpTransport = (TcpTransport)transport.narrow(TcpTransport.class);
        IntrospectionSupport.setProperties(tcpTransport, options);

        Map<String, Object> socketOptions = IntrospectionSupport.extractProperties(options, "socket.");
        tcpTransport.setSocketOptions(socketOptions);

        if (tcpTransport.isTrace()) {
            try {
                transport = TransportLoggerFactory.getInstance().createTransportLogger(transport, tcpTransport.getLogWriterName(),
                        tcpTransport.isDynamicManagement(), tcpTransport.isStartLogging(), tcpTransport.getJmxPort());
            } catch (Throwable e) {
                LOG.error("Could not create TransportLogger object for: " + tcpTransport.getLogWriterName() + ", reason: " + e, e);
            }
        }

        boolean useInactivityMonitor = "true".equals(getOption(options, "useInactivityMonitor", "true"));
        if (useInactivityMonitor && isUseInactivityMonitor(transport)) {
            transport = new InactivityMonitor(transport, format);
            IntrospectionSupport.setProperties(transport, options);
        }


        // Only need the WireFormatNegotiator if using openwire
        if (format instanceof OpenWireFormat) {
            transport = new WireFormatNegotiator(transport, (OpenWireFormat)format, tcpTransport.getMinmumWireFormatVersion());
        }

        return super.compositeConfigure(transport, format, options);
    }

    /**
     * Returns true if the inactivity monitor should be used on the transport
     */
    protected boolean isUseInactivityMonitor(Transport transport) {
        return true;
    }

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
                LOG.warn("path isn't a valid local location for TcpTransport to use", e.getMessage());
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Failure detail", e);
                }
            }
        }
        SocketFactory socketFactory = createSocketFactory();
        return createTcpTransport(wf, socketFactory, location, localLocation);
    }

    /**
     * Allows subclasses of TcpTransportFactory to provide a create custom
     * TcpTransport intances.
     *
     * @param location
     * @param wf
     * @param socketFactory
     * @param localLocation
     * @return
     * @throws UnknownHostException
     * @throws IOException
     */
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new TcpTransport(wf, socketFactory, location, localLocation);
    }

    protected ServerSocketFactory createServerSocketFactory() throws IOException {
        return ServerSocketFactory.getDefault();
    }

    protected SocketFactory createSocketFactory() throws IOException {
        return SocketFactory.getDefault();
    }
}
