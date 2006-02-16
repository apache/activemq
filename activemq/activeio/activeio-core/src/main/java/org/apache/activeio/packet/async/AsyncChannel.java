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
package org.apache.activeio.packet.async;

import java.io.IOException;

import org.apache.activeio.Channel;
import org.apache.activeio.packet.Packet;


/**
 * AsyncChannel objects asynchronously push 'up' {@see org.apache.activeio.Packet} objects
 * to a registered {@see org.apache.activeio.ChannelConsumer}.
 * 
 * @version $Revision$
 */
public interface AsyncChannel extends Channel {
    
    /**
     * Registers the {@see ChannelConsumer} that the protcol will use to deliver packets
     * coming 'up' the channel.
     *  
     * @param packetListener
     */
    void setAsyncChannelListener(AsyncChannelListener channelListener);
    
    /**
     * @return the registered Packet consumer
     */
    AsyncChannelListener getAsyncChannelListener();
    
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
