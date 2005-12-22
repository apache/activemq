/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activeio.packet.sync.datagram;

import java.net.DatagramPacket;
import java.net.InetAddress;


final public class DatagramContext {

    public InetAddress address;
    public Integer port;

    public DatagramContext() {            
    }
    
    public DatagramContext(DatagramPacket datagramPacket) {
        this(datagramPacket.getAddress(), new Integer(datagramPacket.getPort()));
    }
    
    public DatagramContext(InetAddress address, Integer port) {
        this.address = address;
        this.port = port;
    }
    
    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
    
}