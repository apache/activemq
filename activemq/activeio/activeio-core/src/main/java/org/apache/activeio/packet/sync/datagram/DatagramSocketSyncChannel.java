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

package org.apache.activeio.packet.sync.datagram;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.ByteSequence;
import org.apache.activeio.packet.FilterPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;

/**
 * A {@see org.apache.activeio.SynchChannel}implementation that uses
 * TCP to talk to the network.
 * 
 * @version $Revision$
 */
public class DatagramSocketSyncChannel implements SyncChannel {

    private final class UDPFilterPacket extends FilterPacket {
        private final DatagramPacket packet;

        private UDPFilterPacket(Packet next, DatagramPacket packet) {
            super(next);
            this.packet = packet;
        }

        public Object getAdapter(Class target) {
            if( target == DatagramContext.class ) {
                return new DatagramContext(packet);
            }
            return super.getAdapter(target);
        }

        public Packet filter(Packet packet) {
            return new UDPFilterPacket(packet, this.packet);
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private final DatagramSocket socket;

    private boolean disposed;

    private int curentSoTimeout;

    /**
     * Construct basic helpers
     * 
     * @param wireFormat
     * @throws IOException
     */
    protected DatagramSocketSyncChannel(DatagramSocket socket) throws IOException {
        this.socket = socket;
        socket.setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        socket.setSendBufferSize(DEFAULT_BUFFER_SIZE);
    }

    protected DatagramSocket getSocket() {
        return socket;
    }

    /**
     * @see org.apache.activeio.packet.sync.SyncChannel#read(long)
     */
    public org.apache.activeio.packet.Packet read(long timeout) throws IOException {
        try {

            if (timeout == SyncChannelServer.WAIT_FOREVER_TIMEOUT)
                setSoTimeout(0);
            else if (timeout == SyncChannelServer.NO_WAIT_TIMEOUT)
                setSoTimeout(1);
            else
                setSoTimeout((int) timeout);

            // FYI: message data is truncated if biger than this buffer.
            final byte data[] = new byte[DEFAULT_BUFFER_SIZE];
            final DatagramPacket packet = new DatagramPacket(data, data.length);
            socket.receive(packet);
            
            // A FilterPacket is used to provide the UdpDatagramContext via narrow.
            return new UDPFilterPacket(new ByteArrayPacket(data, 0, packet.getLength()), packet);
            
        } catch (SocketTimeoutException e) {
            return null;
        }
    }

    private void setSoTimeout(int i) throws SocketException {
        if (curentSoTimeout != i) {
            socket.setSoTimeout(i);
            curentSoTimeout = i;
        }
    }

    /**
     * @see org.apache.activeio.Channel#write(org.apache.activeio.packet.Packet)
     */
    public void write(org.apache.activeio.packet.Packet packet) throws IOException {
        ByteSequence sequence = packet.asByteSequence();

        DatagramContext context = (DatagramContext) packet.getAdapter(DatagramContext.class);
        if( context!=null ) {
            socket.send(new DatagramPacket(sequence.getData(),sequence.getOffset(), sequence.getLength(), context.address, context.port.intValue()));
        } else {
            socket.send(new DatagramPacket(sequence.getData(),sequence.getOffset(), sequence.getLength()));
        }
    }

    /**
     * @see org.apache.activeio.Channel#flush()
     */
    public void flush() throws IOException {
    }

    /**
     * @see org.apache.activeio.Disposable#dispose()
     */
    public void dispose() {
        if (disposed)
            return;
        socket.close();
        disposed = true;
    }

    public void start() throws IOException {
    }

    public void stop() throws IOException {
    }

    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }

    public String toString() {
        return "Datagram Connection: "+socket.getLocalSocketAddress()+" -> "+socket.getRemoteSocketAddress();
    }
}