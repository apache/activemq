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
package org.activeio.stream.sync;

import java.io.IOException;

import org.activeio.Channel;
import org.activeio.ChannelServer;



/**
 * A StreamChannelServer object provides an <code>accept</code> method to synchronously 
 * accept and create {@see org.activeio.channel.Channel} objects.
 * 
 * @version $Revision$
 */
public interface StreamChannelServer extends ChannelServer {

    static final public long NO_WAIT_TIMEOUT=0;
	static final public long WAIT_FOREVER_TIMEOUT=-1;	
	
	public Channel accept(long timeout) throws IOException;
	
}
