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
package org.apache.activeio.adapter;

import java.io.IOException;
import java.net.URI;

import org.apache.activeio.packet.async.AsyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelServer;

/**
 * @version $Revision$
 */
final public class AsyncToSyncChannelFactory implements SyncChannelFactory {
    
    private AsyncChannelFactory asyncChannelFactory;
    
    static public SyncChannelFactory adapt(AsyncChannelFactory channelFactory ) {

        // It might not need adapting
        if( channelFactory instanceof SyncChannelServer ) {
            return (SyncChannelFactory) channelFactory;
        }

        // Can we just just undo the adaptor
        if( channelFactory.getClass() == SyncToAsyncChannelFactory.class ) {
            return ((SyncToAsyncChannelFactory)channelFactory).getSyncChannelFactory();
        }
        
        return new AsyncToSyncChannelFactory((AsyncChannelFactory)channelFactory);        
    }
    
    
    private AsyncToSyncChannelFactory(AsyncChannelFactory asyncChannelFactory) {
        this.asyncChannelFactory = asyncChannelFactory;
    }
        
    public SyncChannel openSyncChannel(URI location) throws IOException {
        return AsyncToSyncChannel.adapt( asyncChannelFactory.openAsyncChannel(location) );
    }
    
    public SyncChannelServer bindSyncChannel(URI location) throws IOException {
        return AsyncToSyncChannelServer.adapt(asyncChannelFactory.bindAsyncChannel(location));
    }
    
    public AsyncChannelFactory getAsyncChannelFactory() {
        return asyncChannelFactory;
    }
}
