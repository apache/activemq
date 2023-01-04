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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerSupport;
import org.apache.activemq.transport.reliable.ReliableTransport;
import org.apache.activemq.transport.reliable.ReplayStrategy;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A UDP based implementation of {@link TransportServer}
 *
 * @deprecated
 */
@Deprecated
public class UdpTransportServer extends TransportServerSupport {
    private static final Logger LOG = LoggerFactory.getLogger(UdpTransportServer.class);

    private final UdpTransport serverTransport;
    private final ReplayStrategy replayStrategy;
    private final Transport configuredTransport;
    private boolean usingWireFormatNegotiation;
    private final Map<DatagramEndpoint, Transport> transports = new HashMap<DatagramEndpoint, Transport>();
    private boolean allowLinkStealing;

    public UdpTransportServer(URI connectURI, UdpTransport serverTransport, Transport configuredTransport, ReplayStrategy replayStrategy) {
        super(connectURI);
        this.serverTransport = serverTransport;
        this.configuredTransport = configuredTransport;
        this.replayStrategy = replayStrategy;
    }

    @Override
    public String toString() {
        return "UdpTransportServer@" + serverTransport;
    }

    public void run() {
    }

    public UdpTransport getServerTransport() {
        return serverTransport;
    }

    @Override
    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    @Override
    protected void doStart() throws Exception {
        LOG.info("Starting " + this);

        configuredTransport.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object o) {
                final Command command = (Command)o;
                processInboundConnection(command);
            }

            @Override
            public void onException(IOException error) {
                LOG.error("Caught: " + error, error);
            }

            @Override
            public void transportInterupted() {
            }

            @Override
            public void transportResumed() {
            }
        });
        configuredTransport.start();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        configuredTransport.stop();
    }

    protected void processInboundConnection(Command command) {
        DatagramEndpoint endpoint = (DatagramEndpoint)command.getFrom();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received command on: " + this + " from address: " + endpoint + " command: " + command);
        }
        Transport transport = null;
        synchronized (transports) {
            transport = transports.get(endpoint);
            if (transport == null) {
                if (usingWireFormatNegotiation && !command.isWireFormatInfo()) {
                    LOG.error("Received inbound server communication from: " + command.getFrom() + " expecting WireFormatInfo but was command: " + command);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating a new UDP server connection");
                    }
                    try {
                        transport = createTransport(command, endpoint);
                        transport = configureTransport(transport);
                        transports.put(endpoint, transport);
                    } catch (IOException e) {
                        LOG.error("Caught: " + e, e);
                        getAcceptListener().onAcceptError(e);
                    }
                }
            } else {
                LOG.warn("Discarding duplicate command to server from: " + endpoint + " command: " + command);
            }
        }
    }

    protected Transport configureTransport(Transport transport) {
        transport = new InactivityMonitor(transport, serverTransport.getWireFormat());
        getAcceptListener().onAccept(transport);
        return transport;
    }

    protected Transport createTransport(final Command command, DatagramEndpoint endpoint) throws IOException {
        if (endpoint == null) {
            throw new IOException("No endpoint available for command: " + command);
        }
        final SocketAddress address = endpoint.getAddress();
        final OpenWireFormat connectionWireFormat = serverTransport.getWireFormat().copy();
        final UdpTransport transport = new UdpTransport(connectionWireFormat, address);

        final ReliableTransport reliableTransport = new ReliableTransport(transport, transport);
        reliableTransport.getReplayer();
        reliableTransport.setReplayStrategy(replayStrategy);

        // Joiner must be on outside as the inbound messages must be processed
        // by the reliable transport first
        return new CommandJoiner(reliableTransport, connectionWireFormat) {
            @Override
            public void start() throws Exception {
                super.start();
                reliableTransport.onCommand(command);
            }
        };

        /**
         * final WireFormatNegotiator wireFormatNegotiator = new
         * WireFormatNegotiator(configuredTransport, transport.getWireFormat(),
         * serverTransport .getMinmumWireFormatVersion()) { public void start()
         * throws Exception { super.start(); log.debug("Starting a new server
         * transport: " + this + " with command: " + command);
         * onCommand(command); } // lets use the specific addressing of wire
         * format protected void sendWireFormat(WireFormatInfo info) throws
         * IOException { log.debug("#### we have negotiated the wireformat;
         * sending a wireformat to: " + address); transport.oneway(info,
         * address); } }; return wireFormatNegotiator;
         */
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return serverTransport.getLocalSocketAddress();
    }

    @Override
    public boolean isSslServer() {
        return false;
    }

    @Override
    public boolean isAllowLinkStealing() {
        return allowLinkStealing;
    }

    public void setAllowLinkStealing(boolean allowLinkStealing) {
        this.allowLinkStealing = allowLinkStealing;
    }

    @Override
    public long getMaxConnectionExceededCount() {
        // UDP transport deprecated
        return -1l;
    }

    @Override
    public void resetStatistics() {
        // UDP transport deprecated
    }
}
