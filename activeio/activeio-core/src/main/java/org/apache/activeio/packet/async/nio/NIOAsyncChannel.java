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

package org.apache.activeio.packet.async.nio;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.activeio.packet.ByteBufferPacket;
import org.apache.activeio.packet.ByteSequence;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelListener;
import org.apache.activeio.packet.async.nio.NIOAsyncChannelSelectorManager.SelectorManagerListener;
import org.apache.activeio.packet.async.nio.NIOAsyncChannelSelectorManager.SocketChannelAsyncChannelSelection;
import org.apache.activeio.packet.sync.nio.NIOBaseChannel;

/**
 * @version $Revision$
 */
final public class NIOAsyncChannel extends NIOBaseChannel implements AsyncChannel {

    private AsyncChannelListener channelListener;
    private SocketChannelAsyncChannelSelection selection;
    private ByteBuffer inputByteBuffer;
    private boolean running;

    public NIOAsyncChannel(SocketChannel socketChannel, boolean useDirect) throws IOException {
        super(socketChannel, useDirect);

        socketChannel.configureBlocking(false);                
        selection = NIOAsyncChannelSelectorManager.register(socketChannel, new SelectorManagerListener(){
            public void onSelect(SocketChannelAsyncChannelSelection selection) {
                String origName = Thread.currentThread().getName();
                if (selection.isReadable())
                try {
                    Thread.currentThread().setName(NIOAsyncChannel.this.toString());
                    serviceRead();
                 } catch ( Throwable e ) {
                     System.err.println("ActiveIO unexpected error: ");
                     e.printStackTrace(System.err);
                 } finally {
                     Thread.currentThread().setName(origName);
                 }
            }
        });
        
    }
    
    private void serviceRead() {
        try {
            
            while( true ) {
            	
	            if( inputByteBuffer==null || !inputByteBuffer.hasRemaining() ) {
	                inputByteBuffer = allocateBuffer();
	            }
	
	            int size = socketChannel.read(inputByteBuffer);
	            if( size == -1 ) {
	                this.channelListener.onPacket( EOSPacket.EOS_PACKET );
	                selection.close();
	                break;
	            }

	            if( size==0 ) {
	                break;
	            }
	            
	            // Per Mike Spile, some plaforms read 1 byte of data on the first read, and then
	            // a but load of data on the second read.  Try to load the butload here
	            if( size == 1 && inputByteBuffer.hasRemaining() ) {
		            int size2 = socketChannel.read(inputByteBuffer);
		            if( size2 > 0 )
		            		size += size2;
	            }
	            
	            ByteBuffer remaining = inputByteBuffer.slice();            
	            Packet data = new ByteBufferPacket(((ByteBuffer)inputByteBuffer.flip()).slice());
	            this.channelListener.onPacket( data );
	                        
	            // Keep the remaining buffer around to fill with data.
	            inputByteBuffer = remaining;
	            
	            if( inputByteBuffer.hasRemaining() )
	                break;
            }
            
        } catch (IOException e) {
            this.channelListener.onPacketError(e);
        }
    }
    
    synchronized public void write(Packet packet) throws IOException {
        
    	ByteBuffer data;
        if( packet.getClass()==ByteBufferPacket.class ) {
            data = ((ByteBufferPacket)packet).getByteBuffer();            
        } else {
        	ByteSequence sequence = packet.asByteSequence();
        	data = ByteBuffer.wrap(sequence.getData(), sequence.getOffset(), sequence.getLength());
        }

        long delay=1;
        while( data.hasRemaining() ) {
	        
            // Since the write is non-blocking, all the data may not have been written.
            int r1 = data.remaining();        
	        socketChannel.write( data );        
	        int r2 = data.remaining();
	        
	        // We may need to do a little bit of sleeping to avoid a busy loop.
            // Slow down if no data was written out.. 
	        if( r2>0 && r1-r2==0 ) {
	            try {
                    // Use exponential rollback to increase sleep time.
                    Thread.sleep(delay);
                    delay *= 5;
                    if( delay > 1000*1 ) {
                        delay = 1000;
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
	        } else {
	            delay = 1;
	        }
        }
    }

    public void flush() throws IOException {
    }

    public void setAsyncChannelListener(AsyncChannelListener channelListener) {
        this.channelListener = channelListener;
    }

    public AsyncChannelListener getAsyncChannelListener() {
        return channelListener;
    }

    public void dispose() {
        if( running && channelListener!=null ) {
            channelListener.onPacketError(new SocketException("Socket closed."));
        }
        selection.close();
        super.dispose();
    }

    public void start() throws IOException {
        if( running )
            return;
        running=true;
        selection.setInterestOps(SelectionKey.OP_READ);
    }

    public void stop() throws IOException {
        if( !running )
            return;
        running=false;
        selection.setInterestOps(0);        
    }
 }