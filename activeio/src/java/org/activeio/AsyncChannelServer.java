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
 * AsyncChannelServer objects asynchronously accept and create {@see org.activeio.Channel} objects
 * and then delivers those objects to a {@see org.activeio.AcceptConsumer}.
 * 
 * @version $Revision$
 */
public interface AsyncChannelServer extends ChannelServer {
	
	/**
	 * Registers an AcceptListener which is notified of accepted channels.
	 *  
	 * @param acceptListener
	 */
    void setAcceptListener(AcceptListener acceptListener);


}
