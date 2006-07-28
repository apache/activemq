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
package org.apache.activeio.packet.async.aio;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.activeio.AcceptListener;
import org.apache.activeio.Channel;
import org.apache.activeio.ChannelFactory;
import org.apache.activeio.adapter.AsyncToSyncChannel;
import org.apache.activeio.adapter.SyncToAsyncChannel;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelServer;
import org.apache.activeio.packet.async.aio.AIOAsyncChannel;
import org.apache.activeio.packet.async.aio.AIOSyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 */
public class ChannelFactoryTest extends TestCase {

    static final Log log = LogFactory.getLog(ChannelFactoryTest.class);
    static boolean aioDisabled = System.getProperty("disable.aio.tests", "false").equals("true");

    ChannelFactory factory = new ChannelFactory();

    private SyncChannelServer syncChannelServer;
    private SyncChannel clientSynchChannel;
    private SyncChannel serverSynchChannel;

    private AsyncChannelServer asyncChannelServer;
    private AsyncChannel clientAsyncChannel;
    private AsyncChannel serverAsyncChannel;
    
    protected void setUp() throws Exception {
        log.info("Running: "+getName());
    }

    public void testAIO() throws IOException, URISyntaxException, InterruptedException {

        if( aioDisabled ) {
            return;
        }
        
        createSynchObjects("aio://localhost:0");
        assertNotNull( syncChannelServer.getAdapter(AIOSyncChannelServer.class) );
        assertNotNull( clientSynchChannel.getAdapter(AIOAsyncChannel.class) );
        assertNotNull( serverSynchChannel.getAdapter(AIOAsyncChannel.class) );
        
        createAsynchObjects("aio://localhost:0");
        assertNotNull( asyncChannelServer.getAdapter(AIOSyncChannelServer.class) );
        assertNotNull( clientAsyncChannel.getAdapter(AIOAsyncChannel.class) );
        assertNotNull( serverAsyncChannel.getAdapter(AIOAsyncChannel.class) );
        
    }    

    private void createSynchObjects(String bindURI) throws IOException, URISyntaxException {
        syncChannelServer = factory.bindSyncChannel(new URI(bindURI));
        syncChannelServer.start();
        clientSynchChannel = factory.openSyncChannel(syncChannelServer.getConnectURI());
        serverSynchChannel = AsyncToSyncChannel.adapt( syncChannelServer.accept(1000*5) );
        serverSynchChannel.dispose();        
        clientSynchChannel.dispose();        
        syncChannelServer.dispose();
    }

    private void createAsynchObjects(String bindURI) throws IOException, URISyntaxException, InterruptedException {
        asyncChannelServer = factory.bindAsyncChannel(new URI(bindURI));
        final CountDownLatch accepted = new CountDownLatch(1);
        asyncChannelServer.setAcceptListener(new AcceptListener() {
            public void onAccept(Channel channel) {
                serverAsyncChannel = SyncToAsyncChannel.adapt(channel);
                channel.dispose();
                accepted.countDown();
            }
            public void onAcceptError(IOException error) {
                error.printStackTrace();
            }
        });
        asyncChannelServer.start();
        clientAsyncChannel = factory.openAsyncChannel(asyncChannelServer.getConnectURI());
        accepted.await(1000*10, TimeUnit.MILLISECONDS);
        clientAsyncChannel.dispose();        
        asyncChannelServer.dispose();
    }

}
