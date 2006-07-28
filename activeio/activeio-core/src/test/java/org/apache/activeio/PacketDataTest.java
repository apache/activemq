/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio;

import java.io.IOException;

import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.PacketData;

import junit.framework.TestCase;

/**
 */
public class PacketDataTest extends TestCase {

    ByteArrayPacket packet = new ByteArrayPacket(new byte[200]);
    PacketData data = new PacketData(packet);
    
    public void testInteger() throws IOException {
        data.writeInt(Integer.MAX_VALUE);
        data.writeInt(Integer.MIN_VALUE);
        data.writeInt(551);
        
        packet.flip();
        assertEquals(Integer.MAX_VALUE, data.readInt());
        assertEquals(Integer.MIN_VALUE, data.readInt());      
        assertEquals(551, data.readInt());      
    }
    
}
