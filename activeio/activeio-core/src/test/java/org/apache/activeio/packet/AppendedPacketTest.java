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
package org.apache.activeio.packet;

import org.apache.activeio.packet.AppendedPacket;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;


/**
 */
public class AppendedPacketTest extends PacketTestSupport {

    Packet createTestPacket(int capacity) {
        int c1 = capacity/2;
        int c2 = capacity-c1;
        
        return AppendedPacket.join(
                	new ByteArrayPacket(new byte[c1]),
                	new ByteArrayPacket(new byte[c2]));
    }

}
