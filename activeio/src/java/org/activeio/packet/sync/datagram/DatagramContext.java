/*
 * Created on Dec 22, 2004
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.activeio.packet.sync.datagram;

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