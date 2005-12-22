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
import java.net.URI;

import org.activeio.AcceptListener;
import org.activeio.Channel;
import org.activeio.ChannelFactory;
import org.activeio.ChannelServer;
import org.activeio.packet.async.AsyncChannelServer;
import org.activeio.packet.sync.SyncChannelServer;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapts a {@see org.activeio,SynchChannelServer} so that it provides an 
 * {@see org.activeio.AsyncChannelServer} interface.  When this channel
 * is started, a background thread is used to poll the (@see org.activeio.SynchChannelServer}
 * for accepted channel connections which are then delivered to the {@see org.activeio.AcceptConsumer}.
 * 
 * @version $Revision$
 */
final public class SyncToAsyncChannelServer implements AsyncChannelServer, Runnable {

    private final SyncChannelServer syncChannelServer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Executor executor;
    private AcceptListener acceptListener;
    private CountDownLatch doneCountDownLatch;
    
    
    static public AsyncChannelServer adapt(ChannelServer channel) {
        return adapt(channel, ChannelFactory.DEFAULT_EXECUTOR);
    }

    static public AsyncChannelServer adapt(ChannelServer channel, Executor executor) {

        // It might not need adapting
        if( channel instanceof AsyncChannelServer ) {
            return (AsyncChannelServer) channel;
        }

        // Can we just just undo the adaptor
        if( channel.getClass() == SyncToAsyncChannel.class ) {
            return ((AsyncToSyncChannelServer)channel).getAsyncChannelServer();
        }
        
        return new SyncToAsyncChannelServer((SyncChannelServer)channel, executor);        
    }
    
    public SyncToAsyncChannelServer(SyncChannelServer syncServer) {
        this(syncServer, ChannelFactory.DEFAULT_EXECUTOR);
    }
    
    public SyncToAsyncChannelServer(SyncChannelServer syncServer, Executor executor) {
        this.syncChannelServer = syncServer;        
        this.executor=executor;
    }
    
    synchronized public void start() throws IOException {        
        if (running.compareAndSet(false, true)) {
            
            if( acceptListener == null )
                throw new IllegalStateException("AcceptListener must be set before object can be started.");

            syncChannelServer.start();
            
            doneCountDownLatch = new CountDownLatch(1);
            executor.execute(this);
        }
    }

    synchronized public void stop() throws IOException {
        if (running.compareAndSet(true, false)) {
            try {
                doneCountDownLatch.await(5, TimeUnit.SECONDS);
            } catch (Throwable e) {
            }
            syncChannelServer.stop();
        }
    }

    public void run() {
        // Change the thread name.
        String oldName = Thread.currentThread().getName();        
        Thread.currentThread().setName( syncChannelServer.toString() );
        try {
	        while (running.get()) {
	            try {
	                Channel channel = syncChannelServer.accept(500);
	                if( channel == null )
	                    continue;                
	                acceptListener.onAccept(channel);
	            } catch (IOException e) {
	                if( running.get() )
	                    acceptListener.onAcceptError(e);        
	        	} catch (Throwable e) {        	    
	                if( running.get() )
	                    acceptListener.onAcceptError((IOException)new IOException("Unexpected Error: "+e).initCause(e));
	        	}
	        }
        } finally {
            if( doneCountDownLatch!=null )
                doneCountDownLatch.countDown();
            Thread.currentThread().setName(oldName);            
        }
    }

    /**
     * @see org.activeio.packet.async.AsyncChannelServer#setAcceptListener(org.activeio.AcceptListener)
     */
    public void setAcceptListener(AcceptListener acceptListener) {
        if(running.get()) 
            throw new IllegalStateException("Cannot change the AcceptListener while the object is running.");        
        this.acceptListener = acceptListener;
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        try {
            stop();
        } catch ( IOException ignore) {
        }
        syncChannelServer.dispose();
    }

    public URI getBindURI() {
        return syncChannelServer.getBindURI();
    }

    public URI getConnectURI() {
        return syncChannelServer.getConnectURI();
    }

    public SyncChannelServer getSynchChannelServer() {
        return syncChannelServer;
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return syncChannelServer.getAdapter(target);
    }    
    
    public String toString() {
        return syncChannelServer.toString();
    }
}