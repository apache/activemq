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


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;

/**
 * 
 * 
 */
public class DatagramHeaderMarshaller {

    // TODO for large dynamic networks
    // we may want to evict endpoints that disconnect
    // from a transport - e.g. for multicast
    private Map<SocketAddress, Endpoint> endpoints = new HashMap<SocketAddress, Endpoint>();
    
    /**
     * Reads any header if applicable and then creates an endpoint object
     */
    public Endpoint createEndpoint(ByteBuffer readBuffer, SocketAddress address) {
        return getEndpoint(address);
    }

    public Endpoint createEndpoint(DatagramPacket datagram, DataInputStream dataIn) {
        return getEndpoint(datagram.getSocketAddress());
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

    /**
     * Gets the current endpoint object for this address or creates one if not available.
     * 
     * Note that this method does not need to be synchronized as its only ever going to be
     * used by the already-synchronized read() method of a CommandChannel 
     * 
     */
    protected Endpoint getEndpoint(SocketAddress address) {
        Endpoint endpoint = endpoints.get(address);
        if (endpoint == null) {
            endpoint = createEndpoint(address);
            endpoints.put(address, endpoint);
        }
        return endpoint;
    }

    protected Endpoint createEndpoint(SocketAddress address) {
        return new DatagramEndpoint(address.toString(), address);
    }
}
