/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet.sync;

import java.io.IOException;
import java.net.URI;

import org.apache.activeio.Channel;


/**
 * A SynchChannelFilter can be used as a filter another {@see org.apache.activeio.SynchChannel}
 * Most {@see org.apache.activeio.SynchChannel} that are not directly accessing the network will 
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
     * @see org.apache.activeio.Disposable#dispose()
     */
    public void dispose() {
        next.dispose();
    }

    /**
     * @see org.apache.activeio.Service#start()
     */
    public void start() throws IOException {
        next.start();
    }

    /**
     * @see org.apache.activeio.Service#stop()
     */
    public void stop() throws IOException {
        next.stop();
    }

    /**
     * @return Returns the next.
     */
    public SyncChannelServer getNext() {
        return next;
    }

    /**
     * @see org.apache.activeio.packet.sync.SyncChannelServer#accept(long)
     */
    public Channel accept(long timeout) throws IOException {
        return next.accept(timeout);
    }

    /**
     * @see org.apache.activeio.ChannelServer#getBindURI()
     */
    public URI getBindURI() {
        return next.getBindURI();
    }

    /**
     * @see org.apache.activeio.ChannelServer#getConnectURI()
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