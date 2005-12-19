/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.activeio.packet.async.filter;

import java.io.IOException;

import org.activeio.packet.Packet;
import org.activeio.packet.async.AsyncChannel;
import org.activeio.packet.async.FilterAsyncChannel;
import org.activeio.util.PacketAggregator;

/**
 * This PacketAggregatingAsyncChannel can be used when the client is sending a
 * 'record' style packet down the channel stack and needs receiving end to
 * receive the same 'record' packets.
 * 
 * This is very useful since in general, a channel does not grantee that a
 * Packet that is sent down will not be fragmented or combined with other Packet
 * objects.
 * 
 * This {@see org.activeio.AsyncChannel} adds a 4 byte header
 * to each packet that is sent down.
 * 
 * @version $Revision$
 */
final public class PacketAggregatingAsyncChannel extends FilterAsyncChannel {

    private final PacketAggregator aggregator = new PacketAggregator() {
        protected void packetAssembled(Packet packet) {
            getAsyncChannelListener().onPacket(packet);
        }
    };
    
    public PacketAggregatingAsyncChannel(AsyncChannel next) {
        super(next);
    }

    public void onPacket(Packet packet) {
        try {
            aggregator.addRawPacket(packet);
        } catch (IOException e) {
            getAsyncChannelListener().onPacketError(e);
        }
    }    
    
    public void write(Packet packet) throws IOException {
        getNext().write(aggregator.getHeader(packet));
        getNext().write(packet);
    }

}