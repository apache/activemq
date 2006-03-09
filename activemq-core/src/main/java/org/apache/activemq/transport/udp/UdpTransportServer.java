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

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerSupport;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A UDP based implementation of {@link TransportServer}
 * 
 * @version $Revision$
 */

public class UdpTransportServer extends TransportServerSupport implements CommandProcessor {
    private static final Log log = LogFactory.getLog(UdpTransportServer.class);

    private UdpTransport serverTransport;
    private Transport configuredTransport;
    private Map transports = new HashMap();

    public UdpTransportServer(URI connectURI, UdpTransport serverTransport, Transport configuredTransport) {
        super(connectURI);
        this.serverTransport = serverTransport;
        this.configuredTransport = configuredTransport;

        // lets disable the incremental checking of the sequence numbers
        // as we are getting messages from many different clients
        serverTransport.setCheckSequenceNumbers(false);
    }

    public String toString() {
        return "UdpTransportServer@" + serverTransport;
    }

    public void run() {
    }

    public UdpTransport getServerTransport() {
        return serverTransport;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    protected void doStart() throws Exception {
        log.info("Starting " + this);

        configuredTransport.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
            }

            public void onException(IOException error) {
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });
        configuredTransport.start();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        configuredTransport.stop();
    }

    public void process(Command command, DatagramHeader header) throws IOException {
        SocketAddress address = header.getFromAddress();
        if (log.isDebugEnabled()) {
            log.debug("Received command on: " + this + " from address: " + address + " command: " + command);
        }
        Transport transport = null;
        synchronized (transports) {
            transport = (Transport) transports.get(address);
            if (transport == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Creating a new UDP server connection");
                }
                transport = createTransport(command, header);
                transport = configureTransport(transport);
                transports.put(address, transport);
            }
            else {
                log.warn("Discarding duplicate command to server: " + command + " from: " + address);
            }
        }
    }

    protected Transport configureTransport(Transport transport) {
        transport = new ResponseCorrelator(transport);

        if (serverTransport.getMaxInactivityDuration() > 0) {
            transport = new InactivityMonitor(transport, serverTransport.getMaxInactivityDuration());
        }
        
        getAcceptListener().onAccept(transport);
        return transport;
    }

    protected Transport createTransport(final Command command, DatagramHeader header) throws IOException {
        final SocketAddress address = header.getFromAddress();
        final OpenWireFormat connectionWireFormat = serverTransport.getWireFormat().copy();
        final UdpTransport transport = new UdpTransport(connectionWireFormat, address);

        transport.receivedHeader(header);

        return new WireFormatNegotiator(transport, transport.getWireFormat(), serverTransport.getMinmumWireFormatVersion()) {

            public void start() throws Exception {
                super.start();

                // process the inbound wireformat
                onCommand(command);
            }

            // lets use the specific addressing of wire format
            protected void sendWireFormat(WireFormatInfo info) throws IOException {
                transport.oneway(info, address);
            }
        };
    }

}
