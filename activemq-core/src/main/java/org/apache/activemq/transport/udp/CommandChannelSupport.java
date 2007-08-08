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

import org.apache.activemq.command.Command;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.reliable.ReplayBuffer;
import org.apache.activemq.util.IntSequenceGenerator;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * 
 * @version $Revision$
 */
public abstract class CommandChannelSupport implements CommandChannel {

    protected OpenWireFormat wireFormat;
    protected int datagramSize = 4 * 1024;
    protected SocketAddress targetAddress;
    protected SocketAddress replayAddress;
    protected final String name;
    protected final IntSequenceGenerator sequenceGenerator;
    protected DatagramHeaderMarshaller headerMarshaller;
    private ReplayBuffer replayBuffer;

    public CommandChannelSupport(UdpTransport transport, OpenWireFormat wireFormat, int datagramSize, SocketAddress targetAddress,
            DatagramHeaderMarshaller headerMarshaller) {
        this.wireFormat = wireFormat;
        this.datagramSize = datagramSize;
        this.targetAddress = targetAddress;
        this.headerMarshaller = headerMarshaller;
        this.name = transport.toString();
        this.sequenceGenerator = transport.getSequenceGenerator();
        this.replayAddress = targetAddress;
        if (sequenceGenerator == null) {
            throw new IllegalArgumentException("No sequenceGenerator on the given transport: " + transport);
        }
    }
    
    public void write(Command command) throws IOException {
        write(command, targetAddress);
    }


    // Properties
    // -------------------------------------------------------------------------

    public int getDatagramSize() {
        return datagramSize;
    }

    /**
     * Sets the default size of a datagram on the network.
     */
    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    public SocketAddress getTargetAddress() {
        return targetAddress;
    }

    public void setTargetAddress(SocketAddress targetAddress) {
        this.targetAddress = targetAddress;
    }

    public SocketAddress getReplayAddress() {
        return replayAddress;
    }

    public void setReplayAddress(SocketAddress replayAddress) {
        this.replayAddress = replayAddress;
    }

    public String toString() {
        return "CommandChannel#" + name;
    }

    public DatagramHeaderMarshaller getHeaderMarshaller() {
        return headerMarshaller;
    }

    public void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller) {
        this.headerMarshaller = headerMarshaller;
    }

    public ReplayBuffer getReplayBuffer() {
        return replayBuffer;
    }

    public void setReplayBuffer(ReplayBuffer replayBuffer) {
        this.replayBuffer = replayBuffer;
    }

}
