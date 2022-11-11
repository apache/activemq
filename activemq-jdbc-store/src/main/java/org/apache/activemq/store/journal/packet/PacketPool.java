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
package org.apache.activemq.store.journal.packet;

import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a simple pool of Packet objects.  When the packets that this pool produces are disposed,
 * they are returned to the pool.
 * 
 * @version $Revision: 1.1 $
 */
abstract public class PacketPool {
    
    public static final int DEFAULT_POOL_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultPoolSize", ""+(5)));
    public static final int DEFAULT_PACKET_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultPacketSize", ""+(1024*1024*4)));
    
	private final ArrayList pool = new ArrayList();
	private final int maxPackets;
    private int currentPoolSize;
    private boolean disposed;
    
    public class PooledPacket extends FilterPacket {
        private final AtomicInteger referenceCounter;
        
        public PooledPacket(Packet next) {
            this(next, new AtomicInteger(0));
        }
        
        private PooledPacket(Packet next, AtomicInteger referenceCounter) {
            super(next);
            this.referenceCounter=referenceCounter;
            this.referenceCounter.incrementAndGet();
        }
        
        public Packet filter(Packet packet) {
            return new PooledPacket(next, referenceCounter);
        }

        int getReferenceCounter() {
            return referenceCounter.get();
        }
        
        public void dispose() {
            if( referenceCounter.decrementAndGet()==0 ) {
                returnPacket(next);
            }
        }
    }
	
	/**
	 * @param maxPackets the number of buffers that will be in the pool.
	 */
	public PacketPool(int maxPackets) {
		this.maxPackets = maxPackets;
	}
	
	/**
	 * Blocks until a ByteBuffer can be retreived from the pool.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public Packet getPacket() throws InterruptedException {
	    Packet answer=null;
		synchronized(this) {
			while(answer==null) {
                 if( disposed )
                     return null;                 
				if( pool.size()>0) {
					answer = (Packet) pool.remove(pool.size()-1);
				} else if( currentPoolSize < maxPackets ) {
                     answer = allocateNewPacket();
                     currentPoolSize++;
                 }
				if( answer==null ) {
					this.wait();
				}
			}
		}
		return new PooledPacket(answer);
	}

	/**
	 * Returns a ByteBuffer to the pool.
	 * 
	 * @param packet
	 */
	private void returnPacket(Packet packet) {
		packet.clear();
		synchronized(this) {
			pool.add(packet);
			this.notify();
		}
	}
    
    synchronized public void dispose() {
        disposed = true;
        while( currentPoolSize>0 ) {
            if( pool.size()>0) {
                currentPoolSize -= pool.size();
                pool.clear();
            } else {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
    
    synchronized public void waitForPacketsToReturn() {
        while( currentPoolSize!=pool.size() ) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /**
     * @return
     */
    abstract protected Packet allocateNewPacket();
        
}
