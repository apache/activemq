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
package org.apache.activeio;

import java.io.IOException;


/**
 * An AcceptListener object is used to receive accepted {@see org.apache.activeio.Channel} connections.
 * 
 * @version $Revision$
 */
public interface AcceptListener {
    
	/**
	 * A {@see AsyncChannelServer} will call this method to when a new channel connection has been
	 * accepted. 
	 *   
	 * @param channel
	 */
	void onAccept(Channel channel);

	/**
	 * A {@see AsyncChannelServer} will call this method when a async failure occurs when accepting
	 * a connection. 
     * 
     * @param error the exception that describes the failure.
     */
    void onAcceptError(IOException error);
}