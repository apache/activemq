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

import org.activeio.command.WireFormat;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLogger;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;

public class UdpTransportFactory extends TransportFactory {

    public TransportServer doBind(String brokerId, final URI location) throws IOException {
        try {
            UdpTransport transport = (UdpTransport) doConnect(location);
            UdpTransportServer server = new UdpTransportServer(transport);
            return server;
        }
        catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
        catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
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
        OpenWireFormat wireFormat = (OpenWireFormat) wf;
        wireFormat.setPrefixPacketSize(false);
        return new UdpTransport(wireFormat, location);
    }

    protected ServerSocketFactory createServerSocketFactory() {
        return ServerSocketFactory.getDefault();
    }

    protected SocketFactory createSocketFactory() {
        return SocketFactory.getDefault();
    }
}
