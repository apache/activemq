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
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.activeio.AcceptListener;
import org.apache.activeio.Channel;
import org.apache.activeio.ChannelFactory;
import org.apache.activeio.adapter.AsyncToSyncChannel;
import org.apache.activeio.adapter.SyncToAsyncChannel;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelServer;
import org.apache.activeio.packet.async.nio.NIOAsyncChannel;
import org.apache.activeio.packet.async.nio.NIOAsyncChannelServer;
import org.apache.activeio.packet.async.vmpipe.VMPipeAsyncChannelPipe;
import org.apache.activeio.packet.async.vmpipe.VMPipeAsyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;
import org.apache.activeio.packet.sync.nio.NIOSyncChannel;
import org.apache.activeio.packet.sync.nio.NIOSyncChannelServer;
import org.apache.activeio.packet.sync.socket.SocketSyncChannel;
import org.apache.activeio.packet.sync.socket.SocketSyncChannelServer;
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

    public void testSocket() throws IOException, URISyntaxException, InterruptedException {
        
        createSynchObjects("socket://localhost:0");
        assertNotNull( syncChannelServer.getAdapter(SocketSyncChannelServer.class) );
        assertNotNull( clientSynchChannel.getAdapter(SocketSyncChannel.class) );
        assertNotNull( serverSynchChannel.getAdapter(SocketSyncChannel.class) );
        
        createAsynchObjects("socket://localhost:0");
        assertNotNull( asyncChannelServer.getAdapter(SocketSyncChannelServer.class) );
        assertNotNull( clientAsyncChannel.getAdapter(SocketSyncChannel.class) );
        assertNotNull( serverAsyncChannel.getAdapter(SocketSyncChannel.class) );
        
    }

    public void testNIO() throws IOException, URISyntaxException, InterruptedException {
        
        createSynchObjects("nio://localhost:0");
        assertNotNull( syncChannelServer.getAdapter(NIOSyncChannelServer.class) );
        assertNotNull( clientSynchChannel.getAdapter(NIOSyncChannel.class) );
        assertNotNull( serverSynchChannel.getAdapter(NIOSyncChannel.class) );
        
        createAsynchObjects("nio://localhost:0");
        assertNotNull( asyncChannelServer.getAdapter(NIOAsyncChannelServer.class) );
        assertNotNull( clientAsyncChannel.getAdapter(NIOAsyncChannel.class) );
        assertNotNull( serverAsyncChannel.getAdapter(NIOAsyncChannel.class) );
        
    }    

    public void testVMPipe() throws IOException, URISyntaxException, InterruptedException {
        
        createSynchObjects("vmpipe://localhost");
        assertNotNull( syncChannelServer.getAdapter(VMPipeAsyncChannelServer.class) );
        assertNotNull( clientSynchChannel.getAdapter(VMPipeAsyncChannelPipe.PipeChannel.class) );
        assertNotNull( serverSynchChannel.getAdapter(VMPipeAsyncChannelPipe.PipeChannel.class) );
        
        createAsynchObjects("vmpipe://localhost");
        assertNotNull( asyncChannelServer.getAdapter(VMPipeAsyncChannelServer.class) );
        assertNotNull( clientAsyncChannel.getAdapter(VMPipeAsyncChannelPipe.PipeChannel.class) );
        assertNotNull( serverAsyncChannel.getAdapter(VMPipeAsyncChannelPipe.PipeChannel.class) );
        
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
