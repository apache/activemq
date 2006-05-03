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
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.activeio.packet.ByteBufferPacket;
import org.apache.activeio.packet.ByteSequence;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.EmptyPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;

/**
 * A {@see org.apache.activeio.SynchChannel} implementation that uses a {@see java.nio.channels.SocketChannel}
 * to talk to the network.
 * 
 * Using a SocketChannelSynchChannel should be more efficient than using a SocketSynchChannel since
 * direct ByteBuffer can be used to reduce the jvm overhead needed to copy byte[]s.
 * 
 * @version $Revision$
 */
final public class NIOSyncChannel extends NIOBaseChannel implements SyncChannel {

    private ByteBuffer inputByteBuffer;
//    private Packet data2;

    protected NIOSyncChannel(SocketChannel socketChannel) throws IOException {
        this(socketChannel, true );
    }

    protected NIOSyncChannel(SocketChannel socketChannel, boolean useDirect) throws IOException {
        super(socketChannel, useDirect);
    }
    
    public Packet read(long timeout) throws IOException {
        try {
            
            if( timeout==SyncChannelServer.WAIT_FOREVER_TIMEOUT )
                setSoTimeout( 0 );
            else if( timeout==SyncChannelServer.NO_WAIT_TIMEOUT )
                setSoTimeout( 1 );
            else 
                setSoTimeout( (int)timeout );

            if( inputByteBuffer==null || !inputByteBuffer.hasRemaining() ) {
                inputByteBuffer = allocateBuffer();
            }

            int size = socketChannel.read(inputByteBuffer);
            if( size == -1 )
                return EOSPacket.EOS_PACKET;
            if( size == 0 )
                return EmptyPacket.EMPTY_PACKET;

            ByteBuffer remaining = inputByteBuffer.slice();            
            Packet data = new ByteBufferPacket(((ByteBuffer)inputByteBuffer.flip()).slice());
            
            // Keep the remaining buffer around to fill with data.
            inputByteBuffer = remaining;
            return data;
            
        } catch (SocketTimeoutException e) {
            return null;
        }
    }
    
    public void write(Packet packet) throws IOException {
    	ByteBuffer data;
        if( packet.getClass()==ByteBufferPacket.class ) {
            data = ((ByteBufferPacket)packet).getByteBuffer();            
        } else {
        	ByteSequence sequence = packet.asByteSequence();
        	data = ByteBuffer.wrap(sequence.getData(), sequence.getOffset(), sequence.getLength());
        }
        socketChannel.write( data );            
    }

	public void start() throws IOException {
	}

	public void stop() throws IOException {
	}
    
}