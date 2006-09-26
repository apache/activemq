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
package org.apache.activeio.adapter;

import java.io.IOException;
import java.net.URI;

import org.apache.activeio.ChannelFactory;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelFactory;
import org.apache.activeio.packet.async.AsyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannelFactory;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;

/**
 * @version $Revision$
 */
public class SyncToAsyncChannelFactory implements AsyncChannelFactory {
    
    private final SyncChannelFactory syncChannelFactory;
    private final Executor executor;
        
    static public AsyncChannelFactory adapt(SyncChannelFactory channelFactory) {
        return adapt(channelFactory, ChannelFactory.DEFAULT_EXECUTOR);
    }
    
    static public AsyncChannelFactory adapt(SyncChannelFactory channelFactory, Executor executor ) {

        // It might not need adapting
        if( channelFactory instanceof AsyncChannelFactory ) {
            return (AsyncChannelFactory) channelFactory;
        }

        // Can we just just undo the adaptor
        if( channelFactory.getClass() == AsyncToSyncChannelFactory.class ) {
            return ((AsyncToSyncChannelFactory)channelFactory).getAsyncChannelFactory();
        }
        
        return new SyncToAsyncChannelFactory((SyncChannelFactory)channelFactory, executor);        
    }
    
    /**
     * @deprecated {@see #adapt(SyncChannelFactory)}
     */
    public SyncToAsyncChannelFactory(final SyncChannelFactory next) {
        this(next, ChannelFactory.DEFAULT_EXECUTOR);
    }
    
    /**
     * @deprecated {@see #adapt(SyncChannelFactory, Executor)}
     */
    public SyncToAsyncChannelFactory(final SyncChannelFactory next, Executor executor) {
        this.syncChannelFactory = next;
        this.executor = executor;
    }
        
    public AsyncChannel openAsyncChannel(URI location) throws IOException {
        return SyncToAsyncChannel.adapt(syncChannelFactory.openSyncChannel(location),executor);
    }

    public AsyncChannelServer bindAsyncChannel(URI location) throws IOException {
        return new SyncToAsyncChannelServer(syncChannelFactory.bindSyncChannel(location),executor);
    }
    
    public SyncChannelFactory getSyncChannelFactory() {
        return syncChannelFactory;
    }
}
