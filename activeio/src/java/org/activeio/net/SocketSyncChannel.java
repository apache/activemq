/** 
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/

package org.activeio.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.activeio.ByteSequence;
import org.activeio.Packet;
import org.activeio.SyncChannel;
import org.activeio.SyncChannelServer;
import org.activeio.adapter.OutputStreamChannelToOutputStream;
import org.activeio.packet.ByteArrayPacket;
import org.activeio.packet.EOSPacket;
import org.activeio.packet.EmptyPacket;

/**
 * A {@see org.activeio.SynchChannel} implementation that uses a {@see java.net.Socket}
 *  to talk to the network.
 * 
 * @version $Revision$
 */
public class SocketSyncChannel implements SyncChannel, SocketMetadata {

    protected static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private final SocketStreamChannel channel;
    private Packet inputPacket;
    private final OutputStreamChannelToOutputStream outputStream;
    
    protected SocketSyncChannel(Socket socket) throws IOException {
        this(new SocketStreamChannel(socket));
    }

    public SocketSyncChannel(SocketStreamChannel channel) throws IOException {
        this.channel = channel;
        outputStream = new OutputStreamChannelToOutputStream(channel);
        setReceiveBufferSize(DEFAULT_BUFFER_SIZE);
        setSendBufferSize(DEFAULT_BUFFER_SIZE);
    }

    /**
     * @see org.activeio.SynchChannel#read(long)
     */
    synchronized public org.activeio.Packet read(long timeout) throws IOException {
        try {
            
            if( timeout==SyncChannelServer.WAIT_FOREVER_TIMEOUT )
                setSoTimeout( 0 );
            else if( timeout==SyncChannelServer.NO_WAIT_TIMEOUT )
                setSoTimeout( 1 );
            else 
                setSoTimeout( (int)timeout );

            if( inputPacket==null || !inputPacket.hasRemaining() ) {
                inputPacket = allocatePacket();
            }
            
            ByteSequence sequence = inputPacket.asByteSequence();
            int size = channel.read(sequence.getData(), sequence.getOffset(), sequence.getLength());
            if( size == -1 )
                return EOSPacket.EOS_PACKET;
            if( size == 0 )
                return EmptyPacket.EMPTY_PACKET;
            inputPacket.position(size);
            
            Packet remaining = inputPacket.slice();
            
            inputPacket.flip();
            Packet data = inputPacket.slice();

            // Keep the remaining buffer around to fill with data.
            inputPacket = remaining;            
            return data;
            
        } catch (SocketTimeoutException e) {
            return null;
        }
    }

    private Packet allocatePacket() {
        byte[] data = new byte[DEFAULT_BUFFER_SIZE];
        return new ByteArrayPacket(data);
    }

    protected void setSoTimeout(int i) throws SocketException {
        channel.setSoTimeout(i);
    }
    
    /**
     * @see org.activeio.Channel#write(org.activeio.Packet)
     */
    public void write(Packet packet) throws IOException {
        packet.writeTo(outputStream);
    }

    /**
     * @see org.activeio.Channel#flush()
     */
    public void flush() throws IOException {
        channel.flush();
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        channel.dispose();
    }

    public void start() throws IOException {
        channel.start();
    }
    public void stop(long timeout) throws IOException {
        channel.stop(timeout);
    }
    
    public InetAddress getInetAddress() {
        return channel.getInetAddress();
    }
    public boolean getKeepAlive() throws SocketException {
        return channel.getKeepAlive();
    }
    public InetAddress getLocalAddress() {
        return channel.getLocalAddress();
    }
    public int getLocalPort() {
        return channel.getLocalPort();
    }
    public SocketAddress getLocalSocketAddress() {
        return channel.getLocalSocketAddress();
    }
    public boolean getOOBInline() throws SocketException {
        return channel.getOOBInline();
    }
    public int getPort() {
        return channel.getPort();
    }
    public int getReceiveBufferSize() throws SocketException {
        return channel.getReceiveBufferSize();
    }
    public SocketAddress getRemoteSocketAddress() {
        return channel.getRemoteSocketAddress();
    }
    public boolean getReuseAddress() throws SocketException {
        return channel.getReuseAddress();
    }
    public int getSendBufferSize() throws SocketException {
        return channel.getSendBufferSize();
    }
    public int getSoLinger() throws SocketException {
        return channel.getSoLinger();
    }
    public int getSoTimeout() throws SocketException {
        return channel.getSoTimeout();
    }
    public boolean getTcpNoDelay() throws SocketException {
        return channel.getTcpNoDelay();
    }
    public int getTrafficClass() throws SocketException {
        return channel.getTrafficClass();
    }
    public boolean isBound() {
        return channel.isBound();
    }
    public boolean isClosed() {
        return channel.isClosed();
    }
    public boolean isConnected() {
        return channel.isConnected();
    }
    public void setKeepAlive(boolean on) throws SocketException {
        channel.setKeepAlive(on);
    }
    public void setOOBInline(boolean on) throws SocketException {
        channel.setOOBInline(on);
    }
    public void setReceiveBufferSize(int size) throws SocketException {
        channel.setReceiveBufferSize(size);
    }
    public void setReuseAddress(boolean on) throws SocketException {
        channel.setReuseAddress(on);
    }
    public void setSendBufferSize(int size) throws SocketException {
        channel.setSendBufferSize(size);
    }
    public void setSoLinger(boolean on, int linger) throws SocketException {
        channel.setSoLinger(on, linger);
    }
    public void setTcpNoDelay(boolean on) throws SocketException {
        channel.setTcpNoDelay(on);
    }
    public void setTrafficClass(int tc) throws SocketException {
        channel.setTrafficClass(tc);
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return channel.getAdapter(target);
    }

    public String toString() {
        return channel.toString();
    }
}