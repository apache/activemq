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

package org.activeio.packet.async.aio;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;

import org.activeio.Channel;
import org.activeio.packet.ByteBufferPacket;
import org.activeio.packet.async.AsyncChannel;
import org.activeio.packet.async.filter.WriteBufferedAsyncChannel;
import org.activeio.packet.sync.SyncChannelServer;

import com.ibm.io.async.AsyncServerSocketChannel;

/**
 * @version $Revision$
 */
public class AIOSyncChannelServer implements SyncChannelServer {

    private final AsyncServerSocketChannel serverSocket;
    private final URI bindURI;
    private final URI connectURI;
    private int curentSoTimeout;

    public AIOSyncChannelServer(AsyncServerSocketChannel serverSocket, URI bindURI, URI connectURI) throws IOException {
        this.serverSocket=serverSocket;
        this.bindURI=bindURI;
        this.connectURI=connectURI;
        this.curentSoTimeout = serverSocket.socket().getSoTimeout();
    }
    
    public URI getBindURI() {
        return bindURI;
    }

    public URI getConnectURI() {
        return this.connectURI;
    }

    public void dispose() {
        try {
            serverSocket.close();
        } catch (IOException e) {
        }
    }

    synchronized public void start() throws IOException {
    }

    synchronized public void stop() {
    }

    public Channel accept(long timeout) throws IOException {
        try {
	        
            if (timeout == SyncChannelServer.WAIT_FOREVER_TIMEOUT)
	            setSoTimeout(0);
	        else if (timeout == SyncChannelServer.NO_WAIT_TIMEOUT)
	            setSoTimeout(1);
	        else
	            setSoTimeout((int) timeout);
	
            AsyncChannel channel = new AIOAsyncChannel(serverSocket.accept());
            channel = new WriteBufferedAsyncChannel(channel, ByteBufferPacket.createDefaultBuffer(true), false);
            return channel;
	        
        } catch (SocketTimeoutException ignore) {
        }
        return null;	        
    }

    private void setSoTimeout(int i) throws SocketException {
        if (curentSoTimeout != i) {
            serverSocket.socket().setSoTimeout(i);
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
        return "AIO Server: "+getConnectURI();
    }
    
}