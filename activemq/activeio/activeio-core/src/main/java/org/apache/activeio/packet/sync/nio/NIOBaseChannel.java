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

package org.apache.activeio.packet.sync.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.activeio.packet.ByteBufferPacket;
import org.apache.activeio.stream.sync.socket.SocketMetadata;

/**
 * Base class for the Async and Sync implementations of NIO channels.
 * 
 * @version $Revision$
 */
public class NIOBaseChannel implements SocketMetadata {

	protected final SocketChannel socketChannel;
    protected final Socket socket;
	private final boolean useDirect;
    private int curentSoTimeout;
	private boolean disposed;
    private final String name;

    protected NIOBaseChannel(SocketChannel socketChannel, boolean useDirect) throws IOException {
        this.socketChannel = socketChannel;
		this.useDirect = useDirect;
		this.socket = this.socketChannel.socket();

        if( useDirect ) {
            socket.setSendBufferSize(ByteBufferPacket.DEFAULT_DIRECT_BUFFER_SIZE);
            socket.setReceiveBufferSize(ByteBufferPacket.DEFAULT_DIRECT_BUFFER_SIZE);
        } else {
            socket.setSendBufferSize(ByteBufferPacket.DEFAULT_BUFFER_SIZE);
            socket.setReceiveBufferSize(ByteBufferPacket.DEFAULT_BUFFER_SIZE);
        }		

        this.name = "NIO Socket Connection: "+getLocalSocketAddress()+" -> "+getRemoteSocketAddress();
    }
    
    protected ByteBuffer allocateBuffer() {
        if( useDirect ) {
            return ByteBuffer.allocateDirect(ByteBufferPacket.DEFAULT_DIRECT_BUFFER_SIZE);
        } else {
            return ByteBuffer.allocate(ByteBufferPacket.DEFAULT_BUFFER_SIZE);
        }
    }

    public void setSoTimeout(int i) throws SocketException {
        if( curentSoTimeout != i ) {
            socket.setSoTimeout(i);
            curentSoTimeout = i;
        }
    }
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }

    public String toString() {
        return name;
    }

	public void dispose() {
        if (disposed)
            return;

        try {
            socketChannel.close();
        } catch (IOException ignore) {
        }
        disposed = true;
	}

    /**
     * @see org.apache.activeio.Channel#flush()
     */
    public void flush() throws IOException {
    }
    
    public InetAddress getInetAddress() {
        return socket.getInetAddress();
    }
    public boolean getKeepAlive() throws SocketException {
        return socket.getKeepAlive();
    }
    public InetAddress getLocalAddress() {
        return socket.getLocalAddress();
    }
    public int getLocalPort() {
        return socket.getLocalPort();
    }
    public SocketAddress getLocalSocketAddress() {
        return socket.getLocalSocketAddress();
    }
    public boolean getOOBInline() throws SocketException {
        return socket.getOOBInline();
    }
    public int getPort() {
        return socket.getPort();
    }
    public int getReceiveBufferSize() throws SocketException {
        return socket.getReceiveBufferSize();
    }
    public SocketAddress getRemoteSocketAddress() {
        return socket.getRemoteSocketAddress();
    }
    public boolean getReuseAddress() throws SocketException {
        return socket.getReuseAddress();
    }
    public int getSendBufferSize() throws SocketException {
        return socket.getSendBufferSize();
    }
    public int getSoLinger() throws SocketException {
        return socket.getSoLinger();
    }
    public int getSoTimeout() throws SocketException {
        return socket.getSoTimeout();
    }
    public boolean getTcpNoDelay() throws SocketException {
        return socket.getTcpNoDelay();
    }
    public int getTrafficClass() throws SocketException {
        return socket.getTrafficClass();
    }
    public boolean isBound() {
        return socket.isBound();
    }
    public boolean isClosed() {
        return socket.isClosed();
    }
    public boolean isConnected() {
        return socket.isConnected();
    }
    public void setKeepAlive(boolean on) throws SocketException {
        socket.setKeepAlive(on);
    }
    public void setOOBInline(boolean on) throws SocketException {
        socket.setOOBInline(on);
    }
    public void setReceiveBufferSize(int size) throws SocketException {
        socket.setReceiveBufferSize(size);
    }
    public void setReuseAddress(boolean on) throws SocketException {
        socket.setReuseAddress(on);
    }
    public void setSendBufferSize(int size) throws SocketException {
        socket.setSendBufferSize(size);
    }
    public void setSoLinger(boolean on, int linger) throws SocketException {
        socket.setSoLinger(on, linger);
    }
    public void setTcpNoDelay(boolean on) throws SocketException {
        socket.setTcpNoDelay(on);
    }
    public void setTrafficClass(int tc) throws SocketException {
        socket.setTrafficClass(tc);
    }    
}