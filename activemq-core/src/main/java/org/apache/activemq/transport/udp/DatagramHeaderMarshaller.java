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


import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * 
 * @version $Revision$
 */
public class DatagramHeaderMarshaller {

    /**
     * Reads any header if applicable and then creates an endpoint object
     */
    public Endpoint createEndpoint(ByteBuffer readBuffer, SocketAddress address) {
        return new DatagramEndpoint(address.toString(), address);
    }

    public Endpoint createEndpoint(DatagramPacket datagram, DataInputStream dataIn) {
        SocketAddress address = datagram.getSocketAddress();
        return new DatagramEndpoint(address.toString(), address);
    }

    public void writeHeader(Command command, ByteBuffer writeBuffer) {
        /*
        writeBuffer.putLong(command.getCounter());
        writeBuffer.putInt(command.getDataSize());
        byte flags = command.getFlags();
        //System.out.println("Writing header with counter: " + header.getCounter() + " size: " + header.getDataSize() + " with flags: " + flags);
        writeBuffer.put(flags);
        */
    }

    public void writeHeader(Command command, DataOutputStream dataOut) {
    }

}
