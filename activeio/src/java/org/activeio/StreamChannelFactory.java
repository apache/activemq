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
import java.net.URI;

/**
 * StreamChannelFactory objects can create {@see org.activeio.StreamChannel}
 * and {@see org.activeio.StreamChannelServer} objects. 
 * 
 * @version $Revision$
 */
public interface StreamChannelFactory {

	/**
     * Opens a connection to server.
     * 
     * @param location 
     * @return
     */
	public StreamChannel openStreamChannel(URI location) throws IOException;
	
	/**
     * Binds a server at the URI location.
     * 
     * @param location
     * @return
     */
	public StreamChannelServer bindStreamChannel(URI location) throws IOException;
	
}
