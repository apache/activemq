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
package org.apache.activemq.transport.multicast;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.udp.CommandChannel;
import org.apache.activemq.transport.udp.CommandDatagramSocket;
import org.apache.activemq.transport.udp.DatagramHeaderMarshaller;
import org.apache.activemq.transport.udp.UdpTransport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A multicast based transport.
 * 
 * 
 */
public class MulticastTransport extends UdpTransport {

    private static final Logger LOG = LoggerFactory.getLogger(MulticastTransport.class);

    private static final int DEFAULT_IDLE_TIME = 5000;

    private MulticastSocket socket;
    private InetAddress mcastAddress;
    private int mcastPort;
    private int timeToLive = 1;
    private boolean loopBackMode;
    private long keepAliveInterval = DEFAULT_IDLE_TIME;

    public MulticastTransport(OpenWireFormat wireFormat, URI remoteLocation) throws UnknownHostException, IOException {
        super(wireFormat, remoteLocation);
    }

    public long getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public boolean isLoopBackMode() {
        return loopBackMode;
    }

    public void setLoopBackMode(boolean loopBackMode) {
        this.loopBackMode = loopBackMode;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    protected String getProtocolName() {
        return "Multicast";
    }

    protected String getProtocolUriScheme() {
        return "multicast://";
    }

    protected void bind(DatagramSocket socket, SocketAddress localAddress) throws SocketException {
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        super.doStop(stopper);
        if (socket != null) {
            try {
                socket.leaveGroup(getMulticastAddress());
            } catch (IOException e) {
                stopper.onException(this, e);
            }
            socket.close();
        }
    }

    protected CommandChannel createCommandChannel() throws IOException {
        socket = new MulticastSocket(mcastPort);
        socket.setLoopbackMode(loopBackMode);
        socket.setTimeToLive(timeToLive);

        LOG.debug("Joining multicast address: " + getMulticastAddress());
        socket.joinGroup(getMulticastAddress());
        socket.setSoTimeout((int)keepAliveInterval);

        return new CommandDatagramSocket(this, getWireFormat(), getDatagramSize(), getTargetAddress(), createDatagramHeaderMarshaller(), getSocket());
    }

    protected InetAddress getMulticastAddress() {
        return mcastAddress;
    }

    protected MulticastSocket getSocket() {
        return socket;
    }

    protected void setSocket(MulticastSocket socket) {
        this.socket = socket;
    }

    protected InetSocketAddress createAddress(URI remoteLocation) throws UnknownHostException, IOException {
        this.mcastAddress = InetAddress.getByName(remoteLocation.getHost());
        this.mcastPort = remoteLocation.getPort();
        return new InetSocketAddress(mcastAddress, mcastPort);
    }

    protected DatagramHeaderMarshaller createDatagramHeaderMarshaller() {
        return new MulticastDatagramHeaderMarshaller("udp://dummyHostName:" + getPort());
    }

}
