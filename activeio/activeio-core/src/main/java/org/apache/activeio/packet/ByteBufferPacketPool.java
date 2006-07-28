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


import java.nio.ByteBuffer;

/**
 * Provides a simple pool of ByteBuffer objects.
 * 
 * @version $Revision: 1.1 $
 */
final public class ByteBufferPacketPool extends PacketPool {
        
	private final int packetSize;
	
	/**
	 * Creates a pool of <code>bufferCount</code> ByteBuffers that are 
	 * directly allocated being <code>bufferSize</code> big.
	 * 
	 * @param packetCount the number of buffers that will be in the pool.
	 * @param packetSize the size of the buffers that are in the pool.
	 */
	public ByteBufferPacketPool(int packetCount,int packetSize) {
		super(packetCount);
		this.packetSize = packetSize;
	}
	
    protected Packet allocateNewPacket() {
        return new ByteBufferPacket(ByteBuffer.allocateDirect(packetSize));
    }	
}
