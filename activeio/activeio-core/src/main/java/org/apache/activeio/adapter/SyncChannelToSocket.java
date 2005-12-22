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
package org.apache.activeio.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.stream.sync.socket.SocketMetadata;

/**
 * Provides a {@see java.net.Socket} interface to a {@see org.apache.activeio.SynchChannel}.
 * 
 * If the {@see org.apache.activeio.SynchChannel} being adapted can not be 
 * {@see org.apache.activeio.Channel#narrow(Class)}ed to a {@see org.apache.activeio.net.SocketMetadata} 
 * then all methods accessing socket metadata will throw a {@see java.net.SocketException}.
 *  
 */
public class SyncChannelToSocket extends Socket {
    
    private final SyncChannel channel;
    private final SyncChannelToInputStream inputStream;
    private final SyncChannelToOutputStream outputStream;
    private final SocketMetadata socketMetadata;
    private final Packet urgentPackget = new ByteArrayPacket(new byte[1]);
    boolean closed;

    public SyncChannelToSocket(SyncChannel channel) {
        this(channel, (SocketMetadata)channel.getAdapter(SocketMetadata.class));
    }
    
    public SyncChannelToSocket(SyncChannel channel, SocketMetadata socketMetadata) {
        this.channel = channel;
        this.socketMetadata = socketMetadata;
        this.inputStream = new SyncChannelToInputStream(channel);
        this.outputStream = new SyncChannelToOutputStream(channel);
    }

    public boolean isConnected() {
        return true;
    }
    
    public boolean isBound() {
        return true;
    }
    
    public boolean isClosed() {
        return closed;
    }

    public void bind(SocketAddress bindpoint) throws IOException {
        throw new IOException("Not supported");
    }

    public synchronized void close() throws IOException {
        if( closed )
            return;
        closed = true;
        inputStream.close();
        outputStream.close();
        channel.stop();
    }
    
    public void connect(SocketAddress endpoint) throws IOException {
        throw new IOException("Not supported");
    }
    
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        throw new IOException("Not supported");
    }

    public SocketChannel getChannel() {
        return null;
    }
    
    public InputStream getInputStream() throws IOException {
        return inputStream;
    }

    public OutputStream getOutputStream() throws IOException {
        return outputStream;
    }
    
    public boolean isInputShutdown() {
        return inputStream.isClosed();
    }

    public boolean isOutputShutdown() {
        return outputStream.isClosed();
    }
    
    public void sendUrgentData(int data) throws IOException {
        urgentPackget.clear();
        urgentPackget.write(data);
        urgentPackget.flip();
        channel.write(urgentPackget);
    }

    public int getSoTimeout() throws SocketException {
        return (int) inputStream.getTimeout();
    }
    
    public synchronized void setSoTimeout(int timeout) throws SocketException {
        inputStream.setTimeout(timeout);
    }    
    
    public void shutdownOutput() throws IOException {
        outputStream.close();
    }
    
    public void shutdownInput() throws IOException {
        inputStream.close();
    }
    
    protected SocketMetadata getSocketMetadata() throws SocketException {
        if( socketMetadata == null )
            throw new SocketException("No socket metadata available.");
        return socketMetadata;
    }
    
    public InetAddress getInetAddress() {
        if( socketMetadata ==null )
            return null;
        return socketMetadata.getInetAddress();
    }
    public boolean getKeepAlive() throws SocketException {
        return getSocketMetadata().getKeepAlive();
    }
    public InetAddress getLocalAddress() {
        if( socketMetadata ==null )
            return null;
        return socketMetadata.getLocalAddress();
    }
    public int getLocalPort() {
        if( socketMetadata ==null )
            return -1;
        return socketMetadata.getLocalPort();
    }
    public SocketAddress getLocalSocketAddress() {
        if( socketMetadata ==null )
            return null;
        return socketMetadata.getLocalSocketAddress();
    }
    public boolean getOOBInline() throws SocketException {
        return getSocketMetadata().getOOBInline();
    }
    public int getPort() {
        if( socketMetadata ==null )
            return -1;
        return socketMetadata.getPort();
    }
    public int getReceiveBufferSize() throws SocketException {
        return getSocketMetadata().getReceiveBufferSize();
    }
    public SocketAddress getRemoteSocketAddress() {
        if( socketMetadata ==null )
            return null;
        return socketMetadata.getRemoteSocketAddress();
    }
    public boolean getReuseAddress() throws SocketException {
        return getSocketMetadata().getReuseAddress();
    }
    public int getSendBufferSize() throws SocketException {
        return getSocketMetadata().getSendBufferSize();
    }
    public int getSoLinger() throws SocketException {
        return getSocketMetadata().getSoLinger();
    }
    public boolean getTcpNoDelay() throws SocketException {
        return getSocketMetadata().getTcpNoDelay();
    }
    public int getTrafficClass() throws SocketException {
        return getSocketMetadata().getTrafficClass();
    }
    public void setKeepAlive(boolean on) throws SocketException {
        getSocketMetadata().setKeepAlive(on);
    }
    public void setOOBInline(boolean on) throws SocketException {
        getSocketMetadata().setOOBInline(on);
    }
    public void setReceiveBufferSize(int size) throws SocketException {
        getSocketMetadata().setReceiveBufferSize(size);
    }
    public void setReuseAddress(boolean on) throws SocketException {
        getSocketMetadata().setReuseAddress(on);
    }
    public void setSendBufferSize(int size) throws SocketException {
        getSocketMetadata().setSendBufferSize(size);
    }
    public void setSoLinger(boolean on, int linger) throws SocketException {
        getSocketMetadata().setSoLinger(on, linger);
    }
    public void setTcpNoDelay(boolean on) throws SocketException {
        getSocketMetadata().setTcpNoDelay(on);
    }
    public void setTrafficClass(int tc) throws SocketException {
        getSocketMetadata().setTrafficClass(tc);
    }
}
