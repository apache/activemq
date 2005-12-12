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
 * An RequestListener object is used to receive remote requests from a a {@see org.activeio.RequestChannel}
 * 
 * @version $Revision$
 */
public interface RequestListener {
    
	/**
	 * A {@see RequestChannel} will call this method when a new request arrives.
	 *   
	 * @param packet
	 */
	Packet onRequest(Packet request);

	/**
	 * A {@see RequestChannel} will call this method when a async failure occurs when receiving a request.
     * 
     * @param error the exception that describes the failure.
     */
    void onRquestError(IOException error);
}