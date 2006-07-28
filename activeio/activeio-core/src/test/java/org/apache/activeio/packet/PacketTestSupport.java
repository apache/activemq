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
package org.apache.activeio.packet;

import java.util.Arrays;

import org.apache.activeio.packet.Packet;

import junit.framework.TestCase;


/**
 */
abstract public class PacketTestSupport extends TestCase {
    abstract Packet createTestPacket(int capacity);
    
    public void testInit() {
        Packet packet = createTestPacket(100);
        assertEquals( 100, packet.capacity() );        
        assertEquals( 0, packet.position());        
        assertEquals( 100, packet.limit() );        
        assertEquals( 100, packet.remaining() );        
        assertTrue( packet.hasRemaining() );        
    }
    
    public void testPosition() {
        Packet packet = createTestPacket(100);
        packet.position(10);
        assertEquals( 10, packet.position() );        
    }

    public void testLimit() {
        Packet packet = createTestPacket(100);
        packet.limit(10);
        assertEquals( 10, packet.limit() );        
    }

    public void testRemaining() {
        Packet packet = createTestPacket(100);
        packet.position(5);
        packet.limit(95);
        assertEquals(90, packet.remaining());
        assertTrue(packet.hasRemaining());

        packet.position(5);
        packet.limit(5);
        assertEquals(0, packet.remaining());
        assertFalse(packet.hasRemaining());
    }
    
    public void testFlip() {
        Packet packet = createTestPacket(100);
        packet.position(95);
        packet.flip();        
        assertEquals(0, packet.position());
        assertEquals(95, packet.limit());
    }
    
    public void testClear() {
        Packet packet = createTestPacket(100);
        packet.position(5);
        packet.limit(95);
        packet.clear();        
        assertEquals(0, packet.position());
        assertEquals(100, packet.limit());
    }

    public void testDuplicate() {
        Packet packet = createTestPacket(100);
        packet.position(5);
        packet.limit(95);
        Packet packet2 = packet.duplicate();
        packet2.position(10);
        packet2.limit(20);
        
        assertEquals(5, packet.position());
        assertEquals(95, packet.limit());
        assertEquals(10, packet2.position());
        assertEquals(20, packet2.limit());
    }

    public void testRewind() {
        Packet packet = createTestPacket(100);
        packet.position(5);
        packet.limit(95);
        packet.rewind();
        
        assertEquals(0, packet.position());
        assertEquals(95, packet.limit());
    }
    
    public void testSlice() {
        Packet packet = createTestPacket(100);
        packet.position(5);
        packet.limit(95);
        Packet packet2 = packet.slice();
        
        assertEquals(0, packet2.position());
        assertEquals(90, packet2.capacity());
        assertEquals(90, packet2.limit());
    }

    public void testWriteAndReadByte() {
        
        Packet packet = createTestPacket(256);
        for(int i=0; i < 256; i++) {
            assertTrue(packet.write(i));
        }
        assertFalse(packet.write(0));
        
        packet.flip();
        for(int i=0; i < 256; i++) {
            assertEquals(i, packet.read());
        }       
        assertEquals(-1, packet.read());        
    }
    
    public void testWriteAndReadBulkByte() {
        
        byte data[] = new byte[10];        
        Packet packet = createTestPacket(data.length*10);
        for(int i=0; i < 10; i++) {
            Arrays.fill(data,(byte)i);
            assertEquals(data.length, packet.write(data,0,data.length));
        }
        assertEquals(-1, packet.write(data,0,data.length));
        
        byte buffer[] = new byte[data.length];
        packet.flip();
        for(int i=0; i < 10; i++) {
            assertEquals(buffer.length, packet.read(buffer,0,buffer.length));
            Arrays.fill(data,(byte)i);
            assertEquals(buffer, data);
        }       
        assertEquals(-1, packet.read(buffer,0,buffer.length));
    }
 
    public void assertEquals(byte buffer[], byte data[]) {
        assertEquals(buffer.length, data.length);
        for (int i = 0; i < data.length; i++) {
            assertEquals(buffer[i], data[i]);
        }
    }
}
