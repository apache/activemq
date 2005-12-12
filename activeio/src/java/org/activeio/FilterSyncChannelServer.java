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
 * A SynchChannelFilter can be used as a filter another {@see org.activeio.SynchChannel}
 * Most {@see org.activeio.SynchChannel} that are not directly accessing the network will 
 * extends the SynchChannelFilter since they act as a filter between the client and the network.
 *    
 * @version $Revision$
 */
public class FilterSyncChannelServer implements SyncChannelServer {

    private final SyncChannelServer next;

    public FilterSyncChannelServer(SyncChannelServer next) {
        this.next = next;
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        next.dispose();
    }

    /**
     * @see org.activeio.Service#start()
     */
    public void start() throws IOException {
        next.start();
    }

    /**
     * @see org.activeio.Service#stop(long)
     */
    public void stop(long timeout) throws IOException {
        next.stop(timeout);
    }

    /**
     * @return Returns the next.
     */
    public SyncChannelServer getNext() {
        return next;
    }

    /**
     * @see org.activeio.SyncChannelServer#accept(long)
     */
    public Channel accept(long timeout) throws IOException {
        return next.accept(timeout);
    }

    /**
     * @see org.activeio.ChannelServer#getBindURI()
     */
    public URI getBindURI() {
        return next.getBindURI();
    }

    /**
     * @see org.activeio.ChannelServer#getConnectURI()
     */
    public URI getConnectURI() {
        return next.getConnectURI();
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.getAdapter(target);
    }    

    public String toString() {
        return next.toString();
    }
}