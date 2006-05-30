/*
 * Copyright 2005-2006 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.reliable;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.udp.CommandChannel;
import org.apache.activemq.transport.udp.CommandDatagramChannel;
import org.apache.activemq.transport.udp.UdpTransport;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * An unreliable UDP transport that will randomly discard packets to simulate a
 * bad network (or UDP buffers being flooded).
 * 
 * @version $Revision: $
 */
public class UnreliableUdpTransport extends UdpTransport {

    private DropCommandStrategy dropCommandStrategy;

    public UnreliableUdpTransport(OpenWireFormat wireFormat, int port) throws UnknownHostException, IOException {
        super(wireFormat, port);
    }

    public UnreliableUdpTransport(OpenWireFormat wireFormat, SocketAddress socketAddress) throws IOException {
        super(wireFormat, socketAddress);
    }

    public UnreliableUdpTransport(OpenWireFormat wireFormat, URI remoteLocation) throws UnknownHostException,
            IOException {
        super(wireFormat, remoteLocation);
    }

    public UnreliableUdpTransport(OpenWireFormat wireFormat) throws IOException {
        super(wireFormat);
    }

    public DropCommandStrategy getDropCommandStrategy() {
        return dropCommandStrategy;
    }

    public void setDropCommandStrategy(DropCommandStrategy dropCommandStrategy) {
        this.dropCommandStrategy = dropCommandStrategy;
    }

    protected CommandChannel createCommandDatagramChannel() {
        return new UnreliableCommandDatagramChannel(this, getWireFormat(), getDatagramSize(), getTargetAddress(),
                createDatagramHeaderMarshaller(), getReplayBuffer(), getChannel(), getBufferPool(), dropCommandStrategy);
    }

}
