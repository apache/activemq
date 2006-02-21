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
import java.io.InputStream;

import org.apache.activeio.Channel;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;

/**
 * Provides an InputStream for a given SynchChannel.
 *  
 * @version $Revision$
 */
public class SyncChannelToInputStream extends InputStream {
    
    private final SyncChannel channel;
    private Packet lastPacket;
    private boolean closed;
    private long timeout = Channel.WAIT_FOREVER_TIMEOUT;
    
    /**
     * @param channel
     */
    public SyncChannelToInputStream(final SyncChannel channel) {
        this.channel = channel;
    }
    
    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException {
        while( true ) {
            if( lastPacket==null ) {
                try {
                    lastPacket = channel.read(timeout);
                } catch (IOException e) {
                    throw (IOException)new IOException("Channel failed: "+e.getMessage()).initCause(e);
                }
            }
            if( lastPacket.hasRemaining() ) {
                return lastPacket.read();
            }
        }
    }

    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        while( true ) {
            if( lastPacket==null || !lastPacket.hasRemaining() ) {
                try {
                    lastPacket = channel.read(timeout);
                } catch (IOException e) {
                    throw (IOException)new IOException("Channel failed: "+e.getMessage()).initCause(e);
                }
            }
            if( lastPacket==EOSPacket.EOS_PACKET ) {
                return -1;
            }
            if( lastPacket!=null && lastPacket.hasRemaining() ) {
                return lastPacket.read(b, off, len);
            }
        }
    }
 
    /**
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        closed=true;
        super.close();
    }
    
    public boolean isClosed() {
        return closed;
    }

    /**
     * @param timeout
     */
    public void setTimeout(long timeout) {
        if( timeout <= 0 )
            timeout = Channel.WAIT_FOREVER_TIMEOUT;
        this.timeout = timeout;
    }

    /**
     * @return
     */
    public long getTimeout() {
        if( timeout == Channel.WAIT_FOREVER_TIMEOUT )
            return 0;
        return timeout;
    }
}
