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
package org.apache.activeio.packet.sync;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;

import org.apache.activeio.Channel;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.FilterSyncChannel;
import org.apache.activeio.packet.sync.FilterSyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelServer;

/**
 * Makes all the channels produced by another [@see org.apache.activeio.SyncChannelFactory}
 * have write operations that have built in delays for testing. 
 * 
 * @version $Revision$
 */
public class SlowWriteSyncChannelFactory implements SyncChannelFactory {
    
    final SyncChannelFactory next;
    private final int maxPacketSize;
    private final long packetDelay;

    public SlowWriteSyncChannelFactory(final SyncChannelFactory next, int maxPacketSize, long packetDelay) {
        this.next = next;
        this.maxPacketSize = maxPacketSize;
        this.packetDelay = packetDelay;
    }
    
    class SlowWriteSyncChannel extends FilterSyncChannel {
        public SlowWriteSyncChannel(SyncChannel next) {
            super(next);
        }
        public void write(Packet packet) throws IOException {
            packet = packet.slice();
            while(packet.hasRemaining()) {
                int size = Math.max(maxPacketSize, packet.remaining());
                packet.position(size);
                Packet remaining = packet.slice();
                packet.flip();
                Packet data = packet.slice();
                super.write(data);
                packet = remaining;
                try {
                    Thread.sleep(packetDelay);
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
    }

    class SlowWriteSyncChannelServer extends FilterSyncChannelServer {
        public SlowWriteSyncChannelServer(SyncChannelServer next) {
            super(next);
        }
        public Channel accept(long timeout) throws IOException {
            Channel channel = super.accept(timeout);
            if( channel != null ) {
                channel =  new SlowWriteSyncChannel((SyncChannel) channel);
            }
            return channel;
        }
    }
    
    public SyncChannelServer bindSyncChannel(URI location) throws IOException {
        return next.bindSyncChannel(location);
    }
    
    public SyncChannel openSyncChannel(URI location) throws IOException {
        return new SlowWriteSyncChannel(next.openSyncChannel(location));
    }
    
}
