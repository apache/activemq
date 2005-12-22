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

package org.activeio.packet.sync;

import java.io.IOException;

import org.activeio.Channel;
import org.activeio.packet.Packet;


/**
 * SyncChannel objects allow threaded to synchronously block on the <code>receiveUpPacket</code>
 * method to get 'up' {@see org.activeio.Packet} objects when they arrive.
 * 
 * @version $Revision$
 */
public interface SyncChannel extends Channel {
    
    /**
     * Used to synchronously receive a packet of information going 'up' the channel.
     * This method blocks until a packet is received or the operation experiences timeout.
     * 
     * @param timeout
     * @return the packet received or null if the timeout occurred.
     * @throws IOException
     */
    Packet read(long timeout) throws IOException;
    
    /**
     * Sends a packet down the channel towards the media.
     * 
     * @param packet
     * @throws IOException
     */
    void write(Packet packet) throws IOException;

    /**
     * Some channels may buffer data which may be sent down if flush() is called.
     * 
     * @throws IOException
     */
    void flush() throws IOException;    

}
