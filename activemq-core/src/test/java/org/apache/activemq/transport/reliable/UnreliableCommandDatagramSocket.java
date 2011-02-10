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
package org.apache.activemq.transport.reliable;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketAddress;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.udp.CommandDatagramSocket;
import org.apache.activemq.transport.udp.DatagramHeaderMarshaller;
import org.apache.activemq.transport.udp.UdpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: $
 */
public class UnreliableCommandDatagramSocket extends CommandDatagramSocket {
    private static final Logger LOG = LoggerFactory.getLogger(UnreliableCommandDatagramSocket.class);

    private DropCommandStrategy dropCommandStrategy;

    public UnreliableCommandDatagramSocket(UdpTransport transport, OpenWireFormat wireFormat, int datagramSize, SocketAddress targetAddress,
                                           DatagramHeaderMarshaller headerMarshaller, DatagramSocket channel, DropCommandStrategy strategy) {
        super(transport, wireFormat, datagramSize, targetAddress, headerMarshaller, channel);
        this.dropCommandStrategy = strategy;
    }

    protected void sendWriteBuffer(int commandId, SocketAddress address, byte[] data, boolean redelivery) throws IOException {
        if (dropCommandStrategy.shouldDropCommand(commandId, address, redelivery)) {
            LOG.info("Dropping datagram with command: " + commandId);

            // lets still add it to the replay buffer though!
            ReplayBuffer bufferCache = getReplayBuffer();
            if (bufferCache != null && !redelivery) {
                bufferCache.addBuffer(commandId, data);
            }
        } else {
            super.sendWriteBuffer(commandId, address, data, redelivery);
        }
    }
}
