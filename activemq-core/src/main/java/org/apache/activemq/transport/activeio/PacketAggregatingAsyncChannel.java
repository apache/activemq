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
package org.apache.activemq.transport.activeio;

import java.io.IOException;

import org.activeio.AsyncChannel;
import org.activeio.FilterAsyncChannel;
import org.activeio.Packet;
import org.activeio.PacketData;
import org.activeio.packet.AppendedPacket;
import org.activeio.packet.EOSPacket;

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

    private static final int HEADER_LENGTH = 4;        

    Packet incompleteUpPacket;
    boolean headerLoaded;
    private int upPacketLength;
    
    public PacketAggregatingAsyncChannel(AsyncChannel next) {
        super(next);
    }

    public void onPacket(Packet packet) {

        try {
            // Pass through the EOS packet.
            if( packet == EOSPacket.EOS_PACKET ) {
                channelListener.onPacket(packet);
                return;
            }

            if (incompleteUpPacket != null) {
                packet = AppendedPacket.join(incompleteUpPacket, packet);
                incompleteUpPacket = null;
            }

            while (true) {

                if (!headerLoaded) {
                    headerLoaded = packet.remaining() >= HEADER_LENGTH;
                    if( headerLoaded ) {
                        int pos = packet.position();
                        upPacketLength = PacketData.readIntBig(packet);
                        packet.position(pos);
                        
                        if( upPacketLength < 0 ) {
                            throw new IOException("Up packet length was invalid: "+upPacketLength);
                        }
                        upPacketLength+=4;
                    }
                    if( !headerLoaded )
                        break;
                }

                if (packet.remaining() < upPacketLength )
                    break;

                // Get ready to create a slice to send up.
                int origLimit = packet.limit();
                packet.limit(upPacketLength);
                channelListener.onPacket(packet.slice());
                
                // Get a slice of the remaining since that will dump
                // the first packets of an AppendedPacket
                packet.position(upPacketLength);
                packet.limit(origLimit);
                packet = packet.slice();

                // Need to load a header again now.
                headerLoaded = false;
            }
            if (packet.hasRemaining()) {
                incompleteUpPacket = packet;
            }
        } catch (IOException e) {
            channelListener.onPacketError(e);
        }
        
    }

}