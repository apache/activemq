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
package org.apache.activemq.transport.udp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.reliable.DefaultReplayStrategy;
import org.apache.activemq.transport.reliable.ExceptionIfDroppedReplayStrategy;
import org.apache.activemq.transport.reliable.ReliableTransport;
import org.apache.activemq.transport.reliable.ReplayStrategy;
import org.apache.activemq.transport.reliable.Replayer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 * @version $Revision$
 */
public class UdpTransportFactory extends TransportFactory {
    
    private static final Log log = LogFactory.getLog(TcpTransportFactory.class);
    
    public TransportServer doBind(final URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(location));
            if (options.containsKey("port")) {
                throw new IllegalArgumentException("The port property cannot be specified on a UDP server transport - please use the port in the URI syntax");
            }
            WireFormat wf = createWireFormat(options);
            int port = location.getPort();
            OpenWireFormat openWireFormat = asOpenWireFormat(wf);
            UdpTransport transport = (UdpTransport) createTransport(location, wf);

            Transport configuredTransport = configure(transport, wf, options, true);
            UdpTransportServer server = new UdpTransportServer(location, transport, configuredTransport, createReplayStrategy());
            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Transport configure(Transport transport, WireFormat format, Map options) throws Exception {
        return configure(transport, format, options, false);
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        final UdpTransport udpTransport = (UdpTransport)transport;

        // deal with fragmentation
        transport = new CommandJoiner(transport, asOpenWireFormat(format));

        if (udpTransport.isTrace()) {
            try {
                transport = TransportLoggerFactory.getInstance().createTransportLogger(transport);
            } catch (Throwable e) {
                log.error("Could not create TransportLogger object for: " + TransportLoggerFactory.defaultLogWriterName + ", reason: " + e, e);
            }
        }

        transport = new InactivityMonitor(transport, format);

        if (format instanceof OpenWireFormat) {
            transport = configureClientSideNegotiator(transport, format, udpTransport);
        }

        return transport;
    }

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        OpenWireFormat wireFormat = asOpenWireFormat(wf);
        return new UdpTransport(wireFormat, location);
    }

    /**
     * Configures the transport
     * 
     * @param acceptServer true if this transport is used purely as an 'accept'
     *                transport for new connections which work like TCP
     *                SocketServers where new connections spin up a new separate
     *                UDP transport
     */
    protected Transport configure(Transport transport, WireFormat format, Map options, boolean acceptServer) throws Exception {
        IntrospectionSupport.setProperties(transport, options);
        UdpTransport udpTransport = (UdpTransport)transport;

        OpenWireFormat openWireFormat = asOpenWireFormat(format);

        if (udpTransport.isTrace()) {
            transport = TransportLoggerFactory.getInstance().createTransportLogger(transport);
        }

        transport = new InactivityMonitor(transport, format);

        if (!acceptServer && format instanceof OpenWireFormat) {
            transport = configureClientSideNegotiator(transport, format, udpTransport);
        }

        // deal with fragmentation

        if (acceptServer) {
            // lets not support a buffer of messages to enable reliable
            // messaging on the 'accept server' transport
            udpTransport.setReplayEnabled(false);

            // we don't want to do reliable checks on this transport as we
            // delegate to one that does
            transport = new CommandJoiner(transport, openWireFormat);
            return transport;
        } else {
            ReliableTransport reliableTransport = new ReliableTransport(transport, udpTransport);
            Replayer replayer = reliableTransport.getReplayer();
            reliableTransport.setReplayStrategy(createReplayStrategy(replayer));

            // Joiner must be on outside as the inbound messages must be
            // processed by the reliable transport first
            return new CommandJoiner(reliableTransport, openWireFormat);
        }
    }

    protected ReplayStrategy createReplayStrategy(Replayer replayer) {
        if (replayer != null) {
            return new DefaultReplayStrategy(5);
        }
        return new ExceptionIfDroppedReplayStrategy(1);
    }

    protected ReplayStrategy createReplayStrategy() {
        return new DefaultReplayStrategy(5);
    }

    protected Transport configureClientSideNegotiator(Transport transport, WireFormat format, final UdpTransport udpTransport) {
        return new ResponseRedirectInterceptor(transport, udpTransport);
    }

    protected OpenWireFormat asOpenWireFormat(WireFormat wf) {
        OpenWireFormat answer = (OpenWireFormat)wf;
        return answer;
    }
}
