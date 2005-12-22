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
package org.activeio.adapter;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;

import org.activeio.AcceptListener;
import org.activeio.Channel;
import org.activeio.ChannelServer;
import org.activeio.packet.async.AsyncChannelServer;
import org.activeio.packet.sync.SyncChannelServer;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Adapts a {@see org.activeio.AsyncChannelServer} so that it provides an 
 * {@see org.activeio.SynchChannelServer} interface.  
 * 
 * This object buffers asynchronous accepts from the {@see org.activeio.AsyncChannelServer} 
 * abs buffers them in a {@see edu.emory.mathcs.backport.java.util.concurrent.Channel} util the client accepts the 
 * connection.
 * 
 * @version $Revision$
 */
final public class AsyncToSyncChannelServer implements SyncChannelServer, AcceptListener {

    private final AsyncChannelServer asyncChannelServer;    
    private final BlockingQueue acceptBuffer;
    
    static public SyncChannelServer adapt(ChannelServer channel) {
        return adapt(channel, new LinkedBlockingQueue());
    }

    static public SyncChannelServer adapt(ChannelServer channel, BlockingQueue upPacketChannel) {

        // It might not need adapting
        if( channel instanceof SyncChannelServer ) {
            return (SyncChannelServer) channel;
        }

        // Can we just just undo the adaptor
        if( channel.getClass() == SyncToAsyncChannel.class ) {
            return ((SyncToAsyncChannelServer)channel).getSynchChannelServer();
        }
        
        return new AsyncToSyncChannelServer((AsyncChannelServer)channel, upPacketChannel);        
    }
    
    /**
     * @deprecated {@see #adapt(ChannelServer)}
     */
    public AsyncToSyncChannelServer(AsyncChannelServer asyncChannelServer) {
        this(asyncChannelServer,new LinkedBlockingQueue());
    }
    
    /**
     * @deprecated {@see #adapt(ChannelServer, edu.emory.mathcs.backport.java.util.concurrent.Channel)}
     */
    public AsyncToSyncChannelServer(AsyncChannelServer asyncChannelServer, BlockingQueue acceptBuffer) {
        this.asyncChannelServer = asyncChannelServer;
        this.acceptBuffer=acceptBuffer;
        this.asyncChannelServer.setAcceptListener(this);
    }

    /**
     * @see org.activeio.packet.sync.SyncChannelServer#accept(long)
     */
    public org.activeio.Channel accept(long timeout) throws IOException {
        try {
            
            Object o;
            if( timeout == NO_WAIT_TIMEOUT ) {
                o = acceptBuffer.poll(0, TimeUnit.MILLISECONDS);
            } else if( timeout == WAIT_FOREVER_TIMEOUT ) {
                o = acceptBuffer.take();            
            } else {
                o = acceptBuffer.poll(timeout, TimeUnit.MILLISECONDS);                        
            }
            
            if( o == null )
                return null;
            
            if( o instanceof Channel )
                return (Channel)o;
            
            Throwable e = (Throwable)o;
            throw (IOException)new IOException("Async error occurred: "+e).initCause(e);
            
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        }
    }
    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        asyncChannelServer.dispose();
    }

    /**
     * @see org.activeio.Service#start()
     */
    public void start() throws IOException {
        asyncChannelServer.start();
    }

    /**
     * @see org.activeio.Service#stop()
     */
    public void stop() throws IOException {
        asyncChannelServer.stop();
    }

    public URI getBindURI() {
        return asyncChannelServer.getBindURI();
    }

    public URI getConnectURI() {
        return asyncChannelServer.getConnectURI();
    }

    /**
     * @see org.activeio.AcceptListener#onAccept(org.activeio.Channel)
     */
    public void onAccept(org.activeio.Channel channel) {
        try {
            acceptBuffer.put(channel);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }        
    }
    
    /**
     * @see org.activeio.AcceptListener#onAcceptError(java.io.IOException)
     */
    public void onAcceptError(IOException error) {
        try {
            acceptBuffer.put(error);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }        
    }
    
    public AsyncChannelServer getAsyncChannelServer() {
        return asyncChannelServer;
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return asyncChannelServer.getAdapter(target);
    }    
    
    public String toString() {
        return asyncChannelServer.toString();
    }
}