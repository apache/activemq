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
import org.apache.activemq.transport.InactivityMonitor;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerSupport;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * A UDP based implementation of {@link TransportServer}
 * 
 * @version $Revision$
 */

public class UdpTransportServer extends TransportServerSupport {
    private static final Log log = LogFactory.getLog(UdpTransportServer.class);

    private UdpTransport serverTransport;
    private Map transports = new HashMap();

    public UdpTransportServer(UdpTransport serverTransport) {
        this.serverTransport = serverTransport;
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
        serverTransport.start();
        serverTransport.setCommandProcessor(new CommandProcessor() {
            public void process(Command command, SocketAddress address) {
                onInboundCommand(command, address);
            }
        });
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        serverTransport.stop();
    }

    protected void onInboundCommand(Command command, SocketAddress address) {
        Transport transport = null;
        synchronized (transports) {
            transport = (Transport) transports.get(address);
            if (transport == null) {
                transport = createTransport(address);
                transport = configureTransport(transport);
                transports.put(address, transport);
            }
        }
        processInboundCommand(command, transport);
    }

    public void sendOutboundCommand(Command command, SocketAddress address) {
        // TODO we should use an inbound buffer to make this async
        
    }

    protected void processInboundCommand(Command command, Transport transport) {
        // TODO - consider making this asynchronous
        TransportListener listener = transport.getTransportListener();
        if (listener != null) {
            listener.onCommand(command);
        }
        else {
            log.error("No transportListener available for transport: " + transport + " to process inbound command: " + command);
        }
    }

    protected Transport configureTransport(Transport transport) {
        transport = new ResponseCorrelator(transport);
        transport = new InactivityMonitor(transport, serverTransport.getMaxInactivityDuration());
        getAcceptListener().onAccept(transport);
        return transport;
    }

    protected TransportSupport createTransport(SocketAddress address) {
        return new UdpTransportServerClient(this, address);
    }

}
