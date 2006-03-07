/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.udp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import org.activeio.command.WireFormat;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLogger;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UdpTransportFactory extends TransportFactory {
    private static final Log log = LogFactory.getLog(UdpTransportFactory.class);

    public TransportServer doBind(String brokerId, final URI location) throws IOException {
        throw new IOException("TransportServer not supported for UDP");
        /*
        try {
            Map options = new HashMap(URISupport.parseParamters(location));

            return null;
            UdpTransportServer server = new UdpTransportServer(location);
            server.setWireFormatFactory(createWireFormatFactory(options));
            IntrospectionSupport.setProperties(server, options);

            return server;
        }
        catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
        */
    }

    public Transport configure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        UdpTransport tcpTransport = (UdpTransport) transport;
        if (tcpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        if (tcpTransport.getMaxInactivityDuration() > 0) {
            transport = new InactivityMonitor(transport, tcpTransport.getMaxInactivityDuration());
        }

        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        UdpTransport tcpTransport = (UdpTransport) transport;
        if (tcpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        if (tcpTransport.getMaxInactivityDuration() > 0) {
            transport = new InactivityMonitor(transport, tcpTransport.getMaxInactivityDuration());
        }
        return transport;
    }

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        /*
        URI localLocation = null;
        String path = location.getPath();
        // see if the path is a local URI location
        if (path != null && path.length() > 0) {
            int localPortIndex = path.indexOf(':');
            try {
                Integer.parseInt(path.substring((localPortIndex + 1), path.length()));
                String localString = location.getScheme() + ":/" + path;
                localLocation = new URI(localString);
            }
            catch (Exception e) {
                log.warn("path isn't a valid local location for TcpTransport to use", e);
            }
        }
        if (localLocation != null) {
            return new UdpTransport(wf, location, localLocation);
        }
        */
        return new UdpTransport((OpenWireFormat) wf, location);
    }

    protected ServerSocketFactory createServerSocketFactory() {
        return ServerSocketFactory.getDefault();
    }

    protected SocketFactory createSocketFactory() {
        return SocketFactory.getDefault();
    }
}
