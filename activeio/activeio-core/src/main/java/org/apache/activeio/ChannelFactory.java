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
package org.apache.activeio;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.activeio.adapter.AsyncToSyncChannelFactory;
import org.apache.activeio.adapter.SyncToAsyncChannelFactory;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelFactory;
import org.apache.activeio.packet.async.AsyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelServer;
import org.apache.activeio.util.FactoryFinder;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * A {@see ChannelFactory}uses the requested URI's scheme to determine the
 * actual {@see org.apache.activeio.SynchChannelFactory}or
 * {@see org.apache.activeio.AsyncChannelFactory}implementation to use to create it's
 * {@see org.apache.activeio.Channel}s and {@see org.apache.activeio.ChannelServer}s.
 * 
 * Each URI scheme that {@see ChannelFactory}object handles will have a
 * properties file located at: "META-INF/org.apache.activeio.ChannelFactory/{scheme}".
 * 
 */
public class ChannelFactory implements SyncChannelFactory, AsyncChannelFactory {

    private final HashMap syncChannelFactoryMap = new HashMap();
    private final HashMap asyncChannelFactoryMap = new HashMap();

    
    static public final Executor DEFAULT_EXECUTOR = new ThreadPoolExecutor(10, Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue());
    static {
        ((ThreadPoolExecutor) DEFAULT_EXECUTOR).setThreadFactory(new ThreadFactory() {
            public Thread newThread(Runnable run) {
                Thread thread = new Thread(run);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private static FactoryFinder finder = new FactoryFinder("META-INF/org.apache.activeio.ChannelFactory/");

    public SyncChannel openSyncChannel(URI location) throws IOException {
        SyncChannelFactory factory = getSynchChannelFactory(location.getScheme());
        return factory.openSyncChannel(location);
    }

    public SyncChannelServer bindSyncChannel(URI location) throws IOException {
        SyncChannelFactory factory = getSynchChannelFactory(location.getScheme());
        return factory.bindSyncChannel(location);
    }

    public AsyncChannel openAsyncChannel(URI location) throws IOException {
        AsyncChannelFactory factory = getAsyncChannelFactory(location.getScheme());
        return factory.openAsyncChannel(location);
    }

    public AsyncChannelServer bindAsyncChannel(URI location) throws IOException {
        AsyncChannelFactory factory = getAsyncChannelFactory(location.getScheme());
        return factory.bindAsyncChannel(location);
    }

    private SyncChannelFactory getSynchChannelFactory(String protocol) throws IOException {
        try {
            SyncChannelFactory rc = (SyncChannelFactory) syncChannelFactoryMap.get(protocol);
            if (rc == null) {
                try {
                    rc = (SyncChannelFactory) finder.newInstance(protocol, "SyncChannelFactory.");
                } catch (Throwable original) {
                    // try to recovery by using AsyncChannelFactory and adapt
                    // it to be sync.
                    try {
                        AsyncChannelFactory f = (AsyncChannelFactory) finder.newInstance(protocol,
                                "AsyncChannelFactory.");
                        rc = AsyncToSyncChannelFactory.adapt(f);
                    } catch (Throwable e) {
                        // Recovery strategy failed.. throw original exception.
                        throw original;
                    }
                }
                syncChannelFactoryMap.put(protocol, rc);
            }
            return rc;
        } catch (Throwable e) {
            throw (IOException) new IOException("Could not load a SyncChannelFactory for protcol: " + protocol
                    + ", reason: " + e).initCause(e);
        }
    }

    private AsyncChannelFactory getAsyncChannelFactory(String protocol) throws IOException {
        try {
            AsyncChannelFactory rc = (AsyncChannelFactory) asyncChannelFactoryMap.get(protocol);
            if (rc == null) {

                try {
                    rc = (AsyncChannelFactory) finder.newInstance(protocol, "AsyncChannelFactory.");
                } catch (Throwable original) {
                    // try to recovery by using SynchChannelFactory and adapt it
                    // to be async.
                    try {
                        SyncChannelFactory f = (SyncChannelFactory) finder.newInstance(protocol,
                                "SyncChannelFactory.");
                        rc = SyncToAsyncChannelFactory.adapt(f);
                    } catch (Throwable e) {
                        // Recovery strategy failed.. throw original exception.
                        throw original;
                    }
                }

                asyncChannelFactoryMap.put(protocol, rc);
            }
            return rc;
        } catch (Throwable e) {
            throw (IOException) new IOException("Could not load a AsyncChannelFactory for protcol: " + protocol
                    + ", reason: " + e).initCause(e);
        }
    }

}