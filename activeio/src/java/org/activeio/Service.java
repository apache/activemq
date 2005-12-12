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
 * The Service interface is used control the running state of a channel.
 *  
 * Some channels may use background threads to provide SEDA style processing.  By
 * implenting the Service interface, a protcol can allow a container to
 * control those threads.
 *  
 * @version $Revision$
 */
public interface Service {

	static final public long NO_WAIT_TIMEOUT=0;
	static final public long WAIT_FOREVER_TIMEOUT=-1;	

	/**
	 * Starts the channel.  Once started, the channel is in the running state.  
	 *  
	 * @throws IOException
	 */
    void start() throws IOException;

    /**
	 * Stops the channel.  Once stopped, the channel is in the stopped state.
	 * 
	 * @param timeout The amount of time the channel is allowed to take to gracefully stop.  If the timeout
	 *        is exceeded, the channel should do a forcefull stop.
	 * 
	 * @throws IOException
	 */
    void stop(long timeout) throws IOException;
        
}
