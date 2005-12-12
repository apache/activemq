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
 * @version $Revision$
 */
public interface OutputChannel extends Channel {
    
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
