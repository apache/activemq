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
package org.apache.activeio.util;

import java.io.IOException;

import org.apache.activeio.packet.AppendedPacket;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.PacketData;

/**
 * @version $Revision$
 */
abstract public class PacketAggregator {

    private static final int HEADER_LENGTH = 4;        
    
    private final ByteArrayPacket headerBuffer = new ByteArrayPacket(new byte[HEADER_LENGTH]);
    private final PacketData headerData = new PacketData(headerBuffer);

    Packet incompleteUpPacket;
    boolean headerLoaded;
    private int upPacketLength;
    
    public void addRawPacket(Packet packet) throws IOException {

        // Passthrough the EOS packet.
        if( packet == EOSPacket.EOS_PACKET ) {
            packetAssembled(packet);
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
                    PacketData data = new PacketData(packet);
                    upPacketLength = data.readInt();
                    if( upPacketLength < 0 ) {
                        throw new IOException("Up packet lenth was invalid: "+upPacketLength);
                    }
                    packet = packet.slice();
                }
                if( !headerLoaded )
                    break;
            }

            if (packet.remaining() < upPacketLength )
                break;

            // Get ready to create a slice to send up.
            int origLimit = packet.limit();
            packet.limit(upPacketLength);
            packetAssembled(packet.slice());
            
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
        
    }

    protected abstract void packetAssembled(Packet packet);
    
    public Packet getHeader( Packet packet ) throws IOException {
        headerBuffer.clear();
        headerData.writeInt(packet.remaining());
        headerBuffer.flip();
        return headerBuffer;
    }
}