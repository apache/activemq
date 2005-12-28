/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.transport.tcp;

import org.activeio.command.WireFormat;
import org.apache.activemq.openwire.OpenWireFormat;
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

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class TcpTransportFactory extends TransportFactory {

    public TransportServer doBind(String brokerId, final URI location) throws IOException {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));

            TcpTransportServer server = new TcpTransportServer(location);
            server.setWireFormatFactory(createWireFormatFactory(options));
            IntrospectionSupport.setProperties(server, options);

            return server;
        }
        catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Transport configure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        TcpTransport tcpTransport = (TcpTransport) transport;
        if (tcpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        // TODO: missing inactivity monitor
        // transport = new InactivityMonitor(transport,
        // temp.getMaxInactivityDuration(), activityMonitor.getReadCounter(),
        // activityMonitor.getWriteCounter());
        if( format instanceof OpenWireFormat )
            transport = new WireFormatNegotiator(transport, format, tcpTransport.getMinmumWireFormatVersion());
        
        transport = new MutexTransport(transport);
        transport = new ResponseCorrelator(transport);
        return transport;
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        TcpTransport tcpTransport = (TcpTransport) transport;
        if (tcpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        // TODO: missing inactivity monitor
        // transport = new InactivityMonitor(transport,
        // temp.getMaxInactivityDuration(), activityMonitor.getReadCounter(),
        // activityMonitor.getWriteCounter());
        transport = new WireFormatNegotiator(transport, format, tcpTransport.getMinmumWireFormatVersion());
        return transport;
    }

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        return new TcpTransport(wf, location);
    }

    protected ServerSocketFactory createServerSocketFactory() {
        return ServerSocketFactory.getDefault();
    }

    protected SocketFactory createSocketFactory() {
        return SocketFactory.getDefault();
    }
}
