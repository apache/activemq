/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.tcp;

import static java.lang.Thread.getAllStackTraces;
import static java.util.stream.Collectors.toList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import junit.framework.Test;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InactivityMonitorTest extends CombinationTestSupport implements TransportAcceptListener {

    private static final Logger LOG = LoggerFactory.getLogger(InactivityMonitorTest.class);

    public Runnable serverRunOnCommand;
    public Runnable clientRunOnCommand;

    private TransportServer server;
    private Transport clientTransport;
    private Transport serverTransport;
    private int serverPort;

    private final AtomicInteger clientReceiveCount = new AtomicInteger(0);
    private final AtomicInteger clientErrorCount = new AtomicInteger(0);
    private final AtomicInteger serverReceiveCount = new AtomicInteger(0);
    private final AtomicInteger serverErrorCount = new AtomicInteger(0);

    private final AtomicBoolean ignoreClientError = new AtomicBoolean(false);
    private final AtomicBoolean ignoreServerError = new AtomicBoolean(false);


    public static Test suite() {
         return suite(InactivityMonitorTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        startTransportServer();
    }

    /**
     * @throws Exception
     * @throws URISyntaxException
     */
    private void startClient() throws Exception, URISyntaxException {
        clientTransport = TransportFactory.connect(new URI("tcp://localhost:" + serverPort + "?trace=true&wireFormat.maxInactivityDuration=1000"));
        clientTransport.setTransportListener(new TestClientTransportListener());

        clientTransport.start();
    }

    /**
     * @throws IOException
     * @throws URISyntaxException
     * @throws Exception
     */
    private void startTransportServer() throws IOException, URISyntaxException, Exception {
        server = TransportFactory.bind(new URI("tcp://localhost:0?trace=true&wireFormat.maxInactivityDuration=1000"));
        server.setAcceptListener(this);
        server.start();

        serverPort = server.getSocketAddress().getPort();
    }

    @Override
    protected void tearDown() throws Exception {
        ignoreClientError.set(true);
        ignoreServerError.set(true);
        try {
            if (clientTransport != null) {
                clientTransport.stop();
            }
            if (serverTransport != null) {
                serverTransport.stop();
            }
            if (server != null) {
                server.stop();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        super.tearDown();
    }

    @Override
    public void onAccept(Transport transport) {
        try {
            LOG.info("[" + getName() + "] Server Accepted a Connection");
            serverTransport = transport;
            serverTransport.setTransportListener(new TransportListener() {
                @Override
                public void onCommand(Object command) {
                    serverReceiveCount.incrementAndGet();
                    if (serverRunOnCommand != null) {
                        serverRunOnCommand.run();
                    }
                }

                @Override
                public void onException(IOException error) {
                    if (!ignoreServerError.get()) {
                        LOG.info("Server transport error:", error);
                        serverErrorCount.incrementAndGet();
                    }
                }

                @Override
                public void transportInterupted() {
                }

                @Override
                public void transportResumed() {
                }
            });
            serverTransport.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onAcceptError(Exception error) {
        LOG.trace(error.toString());
    }

    public void testClientHang() throws Exception {
        // Manually create a client transport so that it does not send KeepAlive
        // packets.  this should simulate a client hang.
        clientTransport = new TcpTransport(new OpenWireFormat(), SocketFactory.getDefault(), new URI("tcp://localhost:" + serverPort), null);
        clientTransport.setTransportListener(new TestClientTransportListener());

        clientTransport.start();
        WireFormatInfo info = new WireFormatInfo();
        info.setVersion(OpenWireFormat.DEFAULT_LEGACY_VERSION);
        info.setMaxInactivityDuration(1000);
        clientTransport.oneway(info);

        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());

        // Server should consider the client timed out right away since the
        // client is not hart beating fast enough.
        Thread.sleep(6000);

        assertEquals(0, clientErrorCount.get());
        assertTrue(serverErrorCount.get() > 0);
    }

    public void testNoClientHang() throws Exception {
        startClient();

        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());

        Thread.sleep(4000);

        assertEquals(0, clientErrorCount.get());
        assertEquals(0, serverErrorCount.get());
    }

    public void testReadCheckTimerIsNotLeakedOnError() throws Exception {
        // Intentionally picks a port that is not the listening port to generate a failure
        clientTransport = TransportFactory.connect(new URI("tcp://localhost:" + (serverPort ^ 1)));
        clientTransport.setTransportListener(new TestClientTransportListener());

        // Control test to verify there was no timer from a previous test
        assertThat(getCurrentThreadNames(), not(hasReadCheckTimer()));

        try {
            clientTransport.start();
            fail("A ConnectionException was expected");
        } catch (SocketException e) {
            // A SocketException is expected.
        }

        // If there is any read check timer at this point, calling stop should clean it up (because CHECKER_COUNTER becomes 0)
        clientTransport.stop();
        assertThat(getCurrentThreadNames(), not(hasReadCheckTimer()));
    }

    private static Matcher<Iterable<? super String>> hasReadCheckTimer() {
        return hasItem("ActiveMQ InactivityMonitor ReadCheckTimer");
    }

    private static List<String> getCurrentThreadNames() {
        return getAllStackTraces().keySet().stream().map(Thread::getName).collect(toList());
    }

    /**
     * Used to test when a operation blocks. This should not cause transport to
     * get disconnected.
     *
     * @throws Exception
     * @throws URISyntaxException
     */
    public void initCombosForTestNoClientHangWithServerBlock() throws Exception {

        addCombinationValues("clientInactivityLimit", new Object[] {1000L});
        addCombinationValues("serverInactivityLimit", new Object[] {1000L});
        addCombinationValues("serverRunOnCommand", new Object[] {new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Sleeping");
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                }
            }
        }});
    }

    public void testNoClientHangWithServerBlock() throws Exception {

        startClient();

        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());

        Thread.sleep(4000);

        assertEquals(0, clientErrorCount.get());
        assertEquals(0, serverErrorCount.get());
    }

    private class TestClientTransportListener implements TransportListener {
        @Override
        public void onCommand(Object command) {
            clientReceiveCount.incrementAndGet();
            if (clientRunOnCommand != null) {
                clientRunOnCommand.run();
            }
        }

        @Override
        public void onException(IOException error) {
            if (!ignoreClientError.get()) {
                LOG.info("Client transport error:");
                error.printStackTrace();
                clientErrorCount.incrementAndGet();
            }
        }

        @Override
        public void transportInterupted() {
        }

        @Override
        public void transportResumed() {
        }
    }
}
