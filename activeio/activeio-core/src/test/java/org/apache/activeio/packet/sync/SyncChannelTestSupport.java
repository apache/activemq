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

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.Semaphore;

import org.apache.activeio.Channel;
import org.apache.activeio.ChannelServer;
import org.apache.activeio.adapter.AsyncToSyncChannel;
import org.apache.activeio.adapter.AsyncToSyncChannelServer;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;
import org.apache.activeio.stream.sync.socket.SocketMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;


/**
 * Used to test the {@see org.apache.activeio.net.TcpSynchChannel}
 *  
 * @version $Revision$
 */
abstract public class SyncChannelTestSupport extends TestCase {

    Log log = LogFactory.getLog(SyncChannelTestSupport.class);
    private SyncChannelServer server;
    private SyncChannel clientChannel;
    private SyncChannel serverChannel;
    Executor sendExecutor = new ScheduledThreadPoolExecutor(1);

    public void testSmallSendReceive() throws IOException, URISyntaxException, InterruptedException {
        if( isDisabled() ) {
            log.info("test disabled: "+getName());
            return;
        }
        Packet outboundPacket = new ByteArrayPacket("Hello World".getBytes());
        doSendReceive(outboundPacket.duplicate());
    }

    public void testPeerDisconnect() throws IOException, URISyntaxException, InterruptedException {
        if( isDisabled() ) {
            log.info("test disabled: "+getName());
            return;
        }

        Packet outboundPacket = new ByteArrayPacket("Hello World".getBytes());
        doSendReceive(outboundPacket.duplicate());
        // disconnect the client.
        clientChannel.dispose();

        // The server should get an EOS packet.
        Packet packet = serverChannel.read(1000);
        assertEquals(EOSPacket.EOS_PACKET, packet);
    }

    public void testManySmallSendReceives() throws IOException, URISyntaxException, InterruptedException {
        if( isDisabled() ) {
            log.info("test disabled: "+getName());
            return;
        }
        log.info("Start of testManySmallSendReceives");
        Packet outboundPacket = new ByteArrayPacket("Hello World".getBytes());
        long start = System.currentTimeMillis();
        for( int i=0; i < getTestIterations(); i++ ) {
            doSendReceive(outboundPacket.duplicate());
        }
        long end = System.currentTimeMillis();
        log.info("done. Duration: "+duration(start,end)+", duration per send: "+unitDuration(start, end, getTestIterations()));
    }

    private float unitDuration(long start, long end, int testIterations) {
                return duration(start,end)/testIterations;
        }

        private float duration(long start, long end) {
                return (float) (((float)(end-start))/1000.0f);
        }

        protected int getTestIterations() {
        return 1000;
    }

    protected void setUp() throws Exception {

        log.info("Running: "+getName());

        if( isDisabled() ) {
            return;
        }

        log.info("Bind to an annonymous tcp port.");
        server = AsyncToSyncChannelServer.adapt(bindChannel());
        server.start();
        log.info("Server Bound at URI: "+server.getBindURI());

        log.info("Client connecting to: "+server.getConnectURI());
        clientChannel = AsyncToSyncChannel.adapt( openChannel(server.getConnectURI()));
        clientChannel.start();
        SocketMetadata socket = (SocketMetadata) clientChannel.getAdapter(SocketMetadata.class);
        if( socket != null )
            socket.setTcpNoDelay(true);
        log.info("Get connection that was accepted on the server side.");

        Channel c = server.accept(1000*5);
        assertNotNull(c);

        serverChannel = AsyncToSyncChannel.adapt(c);
        serverChannel.start();
        socket = (SocketMetadata) serverChannel.getAdapter(SocketMetadata.class);
        if( socket != null ) {
            socket.setTcpNoDelay(true);
            log.info("Server Channel's Remote addreess: "+socket.getRemoteSocketAddress());
            log.info("Server Channel's Local addreess: "+socket.getLocalSocketAddress());
        }
    }

    /**
     * @param outboundPacket
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    private void doSendReceive(final Packet outboundPacket) throws IOException, URISyntaxException, InterruptedException {
        ByteArrayPacket inboundPacket = new ByteArrayPacket(new byte[outboundPacket.remaining()]);
        final Semaphore runMutext = new Semaphore(0);

        // Do the send async.
        sendExecutor.execute( new Runnable() {
            public void run() {
                try {
                    clientChannel.write(outboundPacket);
                    clientChannel.flush();
                    runMutext.release();
                } catch (IOException e) {
                }
            }
        });

        while( inboundPacket.hasRemaining() ) {
            Packet packet = serverChannel.read(1000*5);
            assertNotNull(packet);
            packet.read(inboundPacket);
        }
        outboundPacket.clear();
        inboundPacket.clear();
        assertEquals(outboundPacket.sliceAsBytes(), inboundPacket.sliceAsBytes());

        runMutext.acquire();
    }

    protected void tearDown() throws Exception {
        if( isDisabled() ) {
            return;
        }
        log.info("Closing down the channels.");
        serverChannel.dispose();
        clientChannel.dispose();
        server.dispose();
    }

    protected boolean isDisabled() {
        return false;
    }

    public void assertEquals(byte []b1, byte[] b2 ) {
        assertEquals(b1.length, b2.length);
        for (int i = 0; i < b2.length; i++) {
            assertEquals(b1[i], b2[i]);
        }
    }

    abstract protected Channel openChannel(URI connectURI) throws IOException ;
    abstract protected ChannelServer bindChannel() throws IOException, URISyntaxException;


}
