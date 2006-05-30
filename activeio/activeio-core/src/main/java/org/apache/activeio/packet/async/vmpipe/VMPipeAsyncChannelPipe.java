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
package org.apache.activeio.packet.async.vmpipe;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelListener;

import edu.emory.mathcs.backport.java.util.concurrent.Semaphore;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Used to connect the bottom ends of two Async channel stacks.
 * 
 */
final public class VMPipeAsyncChannelPipe {

    final PipeChannel leftChannel = new PipeChannel();
    final PipeChannel rightChannel = new PipeChannel();

    final public static class PipeChannel implements AsyncChannel {        
        
        private PipeChannel sibiling;        
        private AsyncChannelListener channelListener;
        private final Semaphore runMutext = new Semaphore(0);
        private boolean disposed;
        private boolean running;
        
        public PipeChannel() {
        }
        
        public void setAsyncChannelListener(AsyncChannelListener channelListener) {
            this.channelListener = channelListener;
        }
        public AsyncChannelListener getAsyncChannelListener() {
            return channelListener;
        }
        
        public void write(Packet packet) throws IOException {
            if( disposed )
                throw new IOException("Conneciton closed.");
            sibiling.onPacket(packet, WAIT_FOREVER_TIMEOUT);
        }

        private void onPacket(Packet packet, long timeout) throws IOException {
            try {
                if( timeout == NO_WAIT_TIMEOUT ) {
                    if( !runMutext.tryAcquire(0, TimeUnit.MILLISECONDS) )
                        return;
                } else if( timeout == WAIT_FOREVER_TIMEOUT ) {
                    runMutext.acquire();
                } else {
                    if( !runMutext.tryAcquire(timeout, TimeUnit.MILLISECONDS) ) 
                        return;
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            try {
                if( disposed ) {
                    throw new IOException("Peer connection closed.");
                }            
                channelListener.onPacket(packet);
            } finally {
                runMutext.release();
            }
        }

        public void flush() throws IOException {
        }
        
        public void start() throws IOException {
            if(running)
                return;
            if( channelListener==null )
                throw new IOException("channelListener has not been set.");
            running=true;
            runMutext.release();
        }
        
        public void stop() throws IOException {
            if(!running)
                return;            
            try {
                runMutext.tryAcquire(5, TimeUnit.SECONDS); 
                running=false;
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
        
        public void dispose() {
            if( disposed )
                return;
            
            if( running && channelListener!=null ) {
                this.channelListener.onPacketError(new IOException("Pipe closed."));
                running=false;
            }
            disposed = true;
            runMutext.release();
            
            try {
                // Inform the peer of the End Of Stream if he's listening.
                sibiling.onPacket(EOSPacket.EOS_PACKET, NO_WAIT_TIMEOUT);
            } catch (IOException e) {
            }
        }
        
        public PipeChannel getSibiling() {
            return sibiling;
        }
        public void setSibiling(PipeChannel sibiling) {
            this.sibiling = sibiling;
        }

        public Object getAdapter(Class target) {
            if( target.isAssignableFrom(getClass()) ) {
                return this;
            }
            return null;
        }
        
        public String getId() {
            return "0x"+Integer.toHexString(System.identityHashCode(this));
        }
        
        public String toString() {
            return "Pipe Channel from "+getId()+" to "+sibiling.getId();
        }        
    }
    
    public VMPipeAsyncChannelPipe() {
        leftChannel.setSibiling(rightChannel);
        rightChannel.setSibiling(leftChannel);
    }
    
    public AsyncChannel getLeftAsyncChannel() {
        return leftChannel;
    }

    public AsyncChannel getRightAsyncChannel() {
        return rightChannel;
    }
}
