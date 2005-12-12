/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.activeio;

/**
 * InputAsyncChannel objects asynchronously push 'up' {@see org.activeio.Packet} objects
 * to a registered {@see org.activeio.AsyncChannelListener}.
 * 
 * @version $Revision$
 */
public interface InputAsyncChannel extends Channel {

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
    
}
