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
package org.apache.activeio.adapter;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;

import org.apache.activeio.Channel;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;

/**
 */
public class SyncChannelServerToServerSocket extends ServerSocket {

    private final SyncChannelServer channelServer;
    private long timeout = Channel.WAIT_FOREVER_TIMEOUT;
    boolean closed;
    private InetAddress inetAddress;
    private int localPort;
    private SocketAddress localSocketAddress;
    private int receiveBufferSize;
    private boolean reuseAddress;
    
    /**
     * @throws IOException
     */
    public SyncChannelServerToServerSocket(SyncChannelServer channelServer) throws IOException {
        this.channelServer = channelServer;
        URI connectURI = channelServer.getConnectURI();
        localPort = connectURI.getPort();
        inetAddress = InetAddress.getByName(connectURI.getHost());
        localSocketAddress = new InetSocketAddress(inetAddress, localPort);        
    }
    
    public synchronized void setSoTimeout(int timeout) throws SocketException {
        if( timeout <= 0 )
            this.timeout = Channel.WAIT_FOREVER_TIMEOUT;
        else 
            this.timeout = timeout;
    }
    
    public synchronized int getSoTimeout() throws IOException {
        if( timeout == Channel.WAIT_FOREVER_TIMEOUT )
            return 0;
        return (int) timeout;
    }

    public Socket accept() throws IOException {
        Channel channel = channelServer.accept(timeout);
        if( channel==null )
            throw new InterruptedIOException();
        
        SyncChannel syncChannel = AsyncToSyncChannel.adapt(channel);            
        syncChannel.start();
        return new SyncChannelToSocket(syncChannel);
                    
    }
    
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
    	if (isClosed())
    	    throw new SocketException("Socket is closed");
  	    throw new SocketException("Already bound");
    }
    
    public void bind(SocketAddress endpoint) throws IOException {
    	if (isClosed())
    	    throw new SocketException("Socket is closed");
  	    throw new SocketException("Already bound");
    }
    
    public ServerSocketChannel getChannel() {
        return null;
    }
    
    public InetAddress getInetAddress() {
        return inetAddress;
    }
    public int getLocalPort() {
        return localPort;
    }
    public SocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }    
    public synchronized int getReceiveBufferSize() throws SocketException {
        return receiveBufferSize;
    }
    
    public boolean getReuseAddress() throws SocketException {
        return reuseAddress;
    }
    
    public boolean isBound() {
        return true;
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
    }
    
    public synchronized void setReceiveBufferSize(int size) throws SocketException {
        this.receiveBufferSize = size;
    }
    
    public void setReuseAddress(boolean on) throws SocketException {
        reuseAddress = on;
    }    
}
