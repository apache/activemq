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

import java.io.IOException;


/**
 * RequestChannel are used to model the request/reponse exchange that is used
 * by higher level protcols such as HTTP and RMI. 
 * 
 * @version $Revision$
 */
public interface RequestChannel extends Channel {
	
	/**
	 * Used to send a packet of information going 'down' the channel and wait for
	 * it's reponse 'up' packet.
	 * 
	 * This method blocks until the response packet is received or the operation 
	 * experiences a timeout.
	 * 
	 * @param request
	 * @param timeout
	 * @return the respnse packet or null if the timeout occured.
	 * @throws IOException
	 */
	Packet request(Packet request, long timeout) throws IOException;

	/**
	 * Registers the {@see RequestListener} that the protcol will use to deliver request packets
	 * comming 'up' the channel.
	 *  
	 * @param packetListener
	 * @throws IOException
	 */
    void setRequestListener(RequestListener requestListener) throws IOException;
    
	/**
	 * @return the registered RequestListener
	 */
    RequestListener getRequestListener();
	
}
