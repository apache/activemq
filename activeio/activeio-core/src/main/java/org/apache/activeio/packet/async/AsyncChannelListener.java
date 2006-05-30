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

import org.apache.activeio.packet.Packet;


/**
 * A ChannelConsumer object is used to receive 'up' {@see org.apache.activeio.Packet} objects.
 * 
 * TODO: describe the threading model so that the implementor of this interface can know if
 * the methods in this interface can block for a long time or not.  I'm thinking that it would
 * be best if these methods are not allowed to block for a long time to encourage SEDA style 
 * processing.
 * 
 * @version $Revision$
 */
public interface AsyncChannelListener {
	
	/**
	 * A {@see AsyncChannel} will call this method to deliver an 'up' packet to a consumer. 
	 *   
	 * @param packet
	 */
    void onPacket(Packet packet);
    
    /**
	 * A {@see AsyncChannel} will call this method when a async failure occurs in the channel. 
     * 
     * @param error the exception that describes the failure.
     */
    void onPacketError(IOException error);
    
}
