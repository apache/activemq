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
package org.activeio.packet.sync.filter;

import java.io.IOException;
import java.util.LinkedList;

import org.activeio.packet.Packet;
import org.activeio.packet.sync.FilterSyncChannel;
import org.activeio.packet.sync.SyncChannel;
import org.activeio.util.PacketAggregator;

/**
 * This PacketAggregatingSynchChannel can be used when the client is sending a
 * 'record' style packet down the channel stack and needs receiving end to
 * receive the same 'record' packets.
 * 
 * This is very usefull since in general, a channel does not garantee that a
 * Packet that is sent down will not be fragmented or combined with other Packet
 * objects.
 * 
 * This {@see org.activeio.SynchChannel} adds a 4 byte header
 * to each packet that is sent down.
 * 
 * @version $Revision$
 */
final public class PacketAggregatingSyncChannel extends FilterSyncChannel {

    private final LinkedList assembledPackets = new LinkedList();    
    private final PacketAggregator aggregator = new PacketAggregator() {
        protected void packetAssembled(Packet packet) {
            assembledPackets.addLast(packet);
        }
    };
    
    /**
     * @param next
     */
    public PacketAggregatingSyncChannel(SyncChannel next) {
        super(next);
    }
    
    public Packet read(long timeout) throws IOException {
        long start = System.currentTimeMillis();
        if( assembledPackets.isEmpty() ) {
            while( true ) {
                
	            Packet packet = getNext().read(timeout);
	            if( packet==null ) {
                    return null;
	            }
	            
	            aggregator.addRawPacket(packet);
	            
	            // Should we try to get more packets?
	            if( assembledPackets.isEmpty() ) {
	                if( timeout == WAIT_FOREVER_TIMEOUT )
	                    continue;
	                
	                timeout = Math.max(0, timeout-(System.currentTimeMillis()-start));
	                if( timeout != 0 )
	                    continue;
	                
	                return null;
	            } else {
	                return (Packet) assembledPackets.removeFirst();
	            }
            }
            
        } else {
            return (Packet) assembledPackets.removeFirst();
        }
        
    }
    
    public void write(Packet packet) throws IOException {
        getNext().write(aggregator.getHeader(packet));
        getNext().write(packet);
    }
}