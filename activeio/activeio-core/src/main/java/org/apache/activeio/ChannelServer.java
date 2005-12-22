/**
 *
 * Copyright 2004 The Apache Software Foundation
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

import java.net.URI;

/**
 * A ChannelServer is used to accept incoming requests to establish new Channel sessions.
 * 
 * Like a normal {@see org.apache.activeio.Channel}, A ChannelServer comes in two falvors, either:
 * {@see org.apache.activeio.AsyncChannelServer} or 
 * {@see org.apache.activeio.SynchChannelServer}.
 * 
 * @version $Revision$
 */
public interface ChannelServer extends Service, Adaptable {

    /**
     * The URI that was used when the channel was bound.  This could be different
     * than what is used by a client to connect to the ChannelServer.  For example,
     * the bind URI might be tcp://localhost:0 which means the channel should bind to 
     * an anonymous port.
     * 
     * @return The URI that was used when the channel was bound
     */
    public URI getBindURI();
    
    /**
     * Once bound, the channel may be able to construct a URI that is more sutible for when 
     * a client needs to connect to the server.  For examle the port of the URI may be 
     * updated to reflect the actual local port that the channel server is listening on.
     * 
     * @return a URI that a client can use to connect to the server or null if the channel cannot construct the URI.
     */
    public URI getConnectURI();
    
}