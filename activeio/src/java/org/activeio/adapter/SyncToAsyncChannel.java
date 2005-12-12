/** 
 * 
 * Copyright 2004 Protique Ltd
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
package org.activeio.adapter;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import org.activeio.AsyncChannel;
import org.activeio.AsyncChannelListener;
import org.activeio.Channel;
import org.activeio.ChannelFactory;
import org.activeio.Packet;
import org.activeio.Service;
import org.activeio.SyncChannel;
import org.activeio.packet.EOSPacket;

import java.io.IOException;

/**
 * Adapts a {@see org.activeio.SynchChannel} so that it provides an 
 * {@see org.activeio.AsyncChannel} interface.  When this channel
 * is started, a background thread is used to poll the {@see org.activeio.SynchChannel}
 *  for packets comming up the channel which are then delivered to the 
 * {@see org.activeio.ChannelConsumer}.
 * 
 * @version $Revision$
 */
public class SyncToAsyncChannel implements AsyncChannel, Runnable {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SyncChannel syncChannel;
    private final Executor executor;
    private AsyncChannelListener channelListener;
    private CountDownLatch doneCountDownLatch;
    
    
    static public AsyncChannel adapt(Channel channel) {
        return adapt(channel, ChannelFactory.DEFAULT_EXECUTOR);
    }

    static public AsyncChannel adapt(Channel channel, Executor executor) {
        
        // It might not need adapting
        if( channel instanceof AsyncChannel ) {
            return (AsyncChannel) channel;
        }
        
        // Can we just just undo the adaptor
        if( channel.getClass() == SyncToAsyncChannel.class ) {
            return ((AsyncToSyncChannel)channel).getAsyncChannel();
        }
        
        return new SyncToAsyncChannel((SyncChannel) channel, executor);
        
    }

    
    /**
     * @deprecated {@see #adapt(SynchChannel)}
     */
    public SyncToAsyncChannel(SyncChannel syncChannel) {
        this(syncChannel, ChannelFactory.DEFAULT_EXECUTOR);
    }

    /**
     * @deprecated {@see #adapt(SynchChannel, Executor)}
     */
    public SyncToAsyncChannel(SyncChannel syncChannel, Executor executor) {
        this.syncChannel = syncChannel;
        this.executor = executor;
    }

    synchronized public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            
            if (channelListener == null)
                throw new IllegalStateException("UpPacketListener must be set before object can be started.");
            
            syncChannel.start();

            doneCountDownLatch = new CountDownLatch(1);
            executor.execute(this);
        }
    }

    synchronized public void stop(long timeout) throws IOException {
        if (running.compareAndSet(true, false)) {
            try {
                if( timeout == NO_WAIT_TIMEOUT ) {
                    syncChannel.stop(NO_WAIT_TIMEOUT);
                } else if( timeout == WAIT_FOREVER_TIMEOUT ) {
                    doneCountDownLatch.await();
                    syncChannel.stop(WAIT_FOREVER_TIMEOUT);
                } else {
                    
                    long start = System.currentTimeMillis();
                    if( doneCountDownLatch.await(timeout, TimeUnit.MILLISECONDS) ) {
                        timeout -= (System.currentTimeMillis() - start);
                    } else {
                        timeout=0;
                    }
                    
                    if( timeout <= 0 ) {
                        syncChannel.stop(NO_WAIT_TIMEOUT);
                    } else {
                        syncChannel.stop(timeout);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (Throwable e) {
                throw (IOException)new IOException("stop failed: " + e.getMessage()).initCause(e);
            }
        }
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        
        // Change the thread name.
        String oldName = Thread.currentThread().getName();        
        Thread.currentThread().setName( syncChannel.toString() );        
        try {
	        while (running.get()) {
	            try {
	                Packet packet = syncChannel.read(500);
	                if( packet==null )
	                    continue;    
                    
                    if( packet == EOSPacket.EOS_PACKET ) {
                        channelListener.onPacket(packet);
                        return;
                    }
                    
                    if( packet.hasRemaining() ) {
                        channelListener.onPacket(packet);
                    }
                    
	            } catch (IOException e) {
	                channelListener.onPacketError(e);
	            } catch (Throwable e) {
	                channelListener.onPacketError((IOException)new IOException("Unexpected Error: "+e).initCause(e));
	            }
	        }
        } finally {
            if( doneCountDownLatch!=null )
                doneCountDownLatch.countDown();
            Thread.currentThread().setName(oldName);
        }
    }

    /**
     * @see org.activeio.AsyncChannel#setAsyncChannelListener(org.activeio.UpPacketListener)
     */
    public void setAsyncChannelListener(AsyncChannelListener channelListener) {
        if (running.get())
            throw new IllegalStateException("Cannot change the UpPacketListener while the object is running.");
        this.channelListener = channelListener;
    }

    /**
     * @see org.activeio.Channel#write(org.activeio.Packet)
     */
    public void write(org.activeio.Packet packet) throws IOException {
        syncChannel.write(packet);
    }

    /**
     * @see org.activeio.Channel#flush()
     */
    public void flush() throws IOException {
        syncChannel.flush();
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        try {
            stop(Service.NO_WAIT_TIMEOUT);
        } catch ( IOException ignore) {
        }
        syncChannel.dispose();        
    }

    public AsyncChannelListener getAsyncChannelListener() {
        return channelListener;
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return syncChannel.getAdapter(target);
    }    
    
    public SyncChannel getSynchChannel() {
        return syncChannel;
    }

    public String toString() {
        return syncChannel.toString();
    }
}