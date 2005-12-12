/**
 *
 * Copyright 2005 (C) The original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activeio.net;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;

import org.activeio.Channel;
import org.activeio.Packet;
import org.activeio.SyncChannel;
import org.activeio.adapter.AsyncToSyncChannel;
import org.activeio.packet.ByteArrayPacket;
import org.activeio.packet.FilterPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;


/**
 * @version $Revision$
 */
abstract public class ConnectionlessSyncChannelTestSupport extends TestCase {

    private final Log log = LogFactory.getLog(ConnectionlessSyncChannelTestSupport.class);
    private SyncChannel clientChannel;
    private SyncChannel serverChannel;
    private final Executor sendExecutor = new ScheduledThreadPoolExecutor(1);

    public void testSmallSendReceive() throws IOException, URISyntaxException, InterruptedException {
        if (isDisabled()) {
            log.info("test disabled: " + getName());
            return;
        }
        UDPFilterPacket fp = new UDPFilterPacket("Hello World".getBytes(), new InetSocketAddress(getAddress(), 4444));
        doSendReceive(fp.duplicate());
    }

    public void testManySmallSendReceives() throws IOException, URISyntaxException, InterruptedException {
        if (isDisabled()) {
            log.info("test disabled: " + getName());
            return;
        }
        log.info("Start of testManySmallSendReceives");
        UDPFilterPacket fp = new UDPFilterPacket("Hello World".getBytes(), new InetSocketAddress(getAddress(), 4444));
        long start = System.currentTimeMillis();
        for (int i = 0; i < getTestIterations(); i++) {
            doSendReceive(fp.duplicate());
        }
        long end = System.currentTimeMillis();
        log.info("done. Duration: " + duration(start, end) + "s, duration per send: " + (unitDuration(start, end, getTestIterations()) * 1000.0) + "ms");
    }

    private float unitDuration(long start, long end, int testIterations) {
        return duration(start, end) / testIterations;
    }

    private float duration(long start, long end) {
        return (float) (((float) (end - start)) / 1000.0f);
    }

    protected int getTestIterations() {
        return 1000;
    }

    protected void setUp() throws Exception {

        log.info("Running: " + getName());

        if (isDisabled()) {
            return;
        }

        log.info("Client connecting to: " + getAddress() + ":4444");

        clientChannel = AsyncToSyncChannel.adapt(openClientChannel(new URI("test://" + getAddress() + ":4444")));
        clientChannel.start();

        serverChannel = AsyncToSyncChannel.adapt(openServerChannel(new URI("test://" + getAddress() + ":4444")));
        serverChannel.start();
    }

    private void doSendReceive(final Packet outboundPacket) throws IOException, InterruptedException {
        ByteArrayPacket ip = new ByteArrayPacket(new byte[outboundPacket.remaining()]);

        // Do the send async.
        sendExecutor.execute(new Runnable() {
            public void run() {
                try {
                    clientChannel.write(outboundPacket);
                    clientChannel.flush();
                }
                catch (Exception e) {
                    int i = 0;
                }
            }
        });

        while (ip.hasRemaining()) {
            Packet packet = serverChannel.read(1000 * 5);
            assertNotNull(packet);
            packet.read(ip);
        }
        outboundPacket.clear();
        ip.clear();
        assertEquals(outboundPacket.sliceAsBytes(), ip.sliceAsBytes());
    }

    protected void tearDown() throws Exception {
        if (isDisabled()) return;

        log.info("Closing down the channels.");

        serverChannel.dispose();
        clientChannel.dispose();
    }

    protected boolean isDisabled() {
        return false;
    }

    public void assertEquals(byte []b1, byte[] b2) {
        assertEquals(b1.length, b2.length);
        for (int i = 0; i < b2.length; i++) {
            assertEquals(b1[i], b2[i]);
        }
    }

    abstract protected Channel openClientChannel(URI connectURI) throws IOException;

    abstract protected Channel openServerChannel(URI connectURI) throws IOException;

    abstract protected String getAddress();

    private final class UDPFilterPacket extends FilterPacket {
        private final DatagramPacket packet;

        private UDPFilterPacket(byte[] buf, SocketAddress address) throws SocketException {
            super(new ByteArrayPacket(buf));
            this.packet = new DatagramPacket(buf, buf.length, address);
        }

        private UDPFilterPacket(Packet op, DatagramPacket packet) {
            super(op);
            this.packet = packet;
        }

        public Object getAdapter(Class target) {
            if (target == DatagramContext.class) {
                return new DatagramContext(packet);
            }
            return super.getAdapter(target);
        }

        public Packet filter(Packet packet) {
            return new UDPFilterPacket(packet, this.packet);
        }
    }
}
