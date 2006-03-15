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
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportLogger;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.reliable.DefaultReplayStrategy;
import org.apache.activemq.transport.reliable.ExceptionIfDroppedReplayStrategy;
import org.apache.activemq.transport.reliable.ReliableTransport;
import org.apache.activemq.transport.reliable.ReplayStrategy;
import org.apache.activemq.transport.reliable.Replayer;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntSequenceGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class UdpTransportFactory extends TransportFactory {

    public TransportServer doBind(String brokerId, final URI location) throws IOException {
        try {
            Map options = new HashMap(URISupport.parseParamters(location));
            if (options.containsKey("port")) {
                throw new IllegalArgumentException("The port property cannot be specified on a UDP server transport - please use the port in the URI syntax");
            }
            WireFormat wf = createWireFormat(options);
            int port = location.getPort();
            OpenWireFormat openWireFormat = asOpenWireFormat(wf);
            UdpTransport transport = new UdpTransport(openWireFormat, port);

            Transport configuredTransport = configure(transport, wf, options, true);
            UdpTransportServer server = new UdpTransportServer(location, transport, configuredTransport, createReplayStrategy());
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
        return configure(transport, format, options, false);
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        IntrospectionSupport.setProperties(transport, options);
        final UdpTransport udpTransport = (UdpTransport) transport;

        // deal with fragmentation
        transport = new CommandJoiner(transport, asOpenWireFormat(format));

        if (udpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        if (format instanceof OpenWireFormat) {
            transport = configureClientSideNegotiator(transport, format, udpTransport);
        }

        if (udpTransport.getMaxInactivityDuration() > 0) {
            transport = new InactivityMonitor(transport, udpTransport.getMaxInactivityDuration());
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
     * @param acceptServer
     *            true if this transport is used purely as an 'accept' transport
     *            for new connections which work like TCP SocketServers where
     *            new connections spin up a new separate UDP transport
     */
    protected Transport configure(Transport transport, WireFormat format, Map options, boolean acceptServer) {
        IntrospectionSupport.setProperties(transport, options);
        UdpTransport udpTransport = (UdpTransport) transport;

        OpenWireFormat openWireFormat = asOpenWireFormat(format);

        if (udpTransport.isTrace()) {
            transport = new TransportLogger(transport);
        }

        if (!acceptServer && format instanceof OpenWireFormat) {
            transport = configureClientSideNegotiator(transport, format, udpTransport);
        }

        if (udpTransport.getMaxInactivityDuration() > 0) {
            transport = new InactivityMonitor(transport, udpTransport.getMaxInactivityDuration());
        }

        // deal with fragmentation

        if (acceptServer) {
            // lets not support a buffer of messages to enable reliable
            // messaging on the 'accept server' transport
            udpTransport.setReplayEnabled(true);

            // we don't want to do reliable checks on this transport as we
            // delegate to one that does
            transport = new CommandJoiner(transport, openWireFormat);
            udpTransport.setSequenceGenerator(new IntSequenceGenerator());
            return transport;
        }
        else {
            Replayer replayer = udpTransport.createReplayer();
            ReliableTransport reliableTransport = new ReliableTransport(transport, createReplayStrategy(replayer));
            udpTransport.setSequenceGenerator(reliableTransport.getSequenceGenerator());

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
        return new TransportFilter(transport) {

            public void onCommand(Command command) {
                // redirect to the endpoint that the last response came from
                Endpoint from = command.getFrom();
                udpTransport.setTargetEndpoint(from);

                super.onCommand(command);
            }

        };
        /*
         * transport = new WireFormatNegotiator(transport,
         * asOpenWireFormat(format), udpTransport.getMinmumWireFormatVersion()) {
         * protected void onWireFormatNegotiated(WireFormatInfo info) { // lets
         * switch to the target endpoint // based on the last packet that was
         * received // so that all future requests go to the newly created UDP
         * channel Endpoint from = info.getFrom();
         * System.out.println("####Êsetting the client side target to: " +
         * from); udpTransport.setTargetEndpoint(from); } }; return transport;
         */
    }

    protected OpenWireFormat asOpenWireFormat(WireFormat wf) {
        OpenWireFormat answer = (OpenWireFormat) wf;
        return answer;
    }
}
