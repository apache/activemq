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

import static org.apache.activemq.transport.AbstractInactivityMonitor.READ_CHECK_THREAD_NAME;
import static org.apache.activemq.transport.AbstractInactivityMonitor.WRITE_CHECK_THREAD_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import junit.framework.Test;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.AbstractInactivityMonitor;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.Wait;
import org.apache.commons.lang.math.RandomUtils;
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

    public void testCheckTimersNotLeakingConcurrentConnections() throws Exception {
        final ExecutorService service = Executors.newFixedThreadPool(10);
        try {
            // Create 250 random connections concurrently, with a combination
            // of hung connections, normal connections, and invalid port
            for (int i = 0; i < 250; i++) {
                service.submit(() -> {
                    // Generate random int of either 0, 1, or 2
                    int num = RandomUtils.nextInt(3);
                    try {
                        // Case 0, create with invalid port so only connect task is created
                        if (num == 0) {
                            // Intentionally picks a port that is not the listening port to generate a failure
                            Transport clientTransport = TransportFactory.connect(new URI("tcp://localhost:" + (serverPort ^ 1)));
                            clientTransport.setTransportListener(new TestClientTransportListener());
                            try {
                                clientTransport.start();
                                fail("A ConnectionException was expected");
                            } catch (SocketException e) {}
                            Thread.sleep(100);
                            clientTransport.stop();
                        // Case 1, Create a successful connection and then stop after a brief sleep
                        } else if (num == 1) {
                            var clientTransport = TransportFactory.connect(new URI("tcp://localhost:" + serverPort + "?trace=true&wireFormat.maxInactivityDuration=1000"));
                            clientTransport.setTransportListener(new TestClientTransportListener());
                            clientTransport.start();
                            Thread.sleep(100);
                            clientTransport.stop();
                        // Case 2, Simulate hung clients that will be closed by the inactivity monitor
                        } else {
                            var clientTransport = new TcpTransport(new OpenWireFormat(), SocketFactory.getDefault(), new URI("tcp://localhost:" + serverPort), null);
                            clientTransport.setTransportListener(new TestClientTransportListener());
                            clientTransport.start();
                            WireFormatInfo info = new WireFormatInfo();
                            info.setVersion(OpenWireFormat.DEFAULT_LEGACY_VERSION);
                            info.setMaxInactivityDuration(1000);
                            clientTransport.oneway(info);
                            // Do not stop, the inactivty monitor will stop
                        }
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });
            }
            // Shutdown and wait for up to 15 seconds to stop
            service.shutdown();
            assertTrue(service.awaitTermination(15, TimeUnit.SECONDS));

            // Wait and verify no more timer threads running
            Wait.waitFor(() -> getCurrentThreadNames().stream()
                    .noneMatch(InactivityMonitorTest::hasTimerThread), 5000, 100);
            assertThat(getCurrentThreadNames(), not(hasReadCheckTimer()));
            assertThat(getCurrentThreadNames(), not(hasWriteCheckTimer()));
        } finally {
            service.shutdownNow();
        }
    }

    /**
     * Verifies that a connection succeeds when READ_CHECK_TIMER is already in a cancelled
     * state with CHECKER_COUNTER == 0. This can happen when the timer's internal thread
     * dies due to an unhandled Error in a TimerTask: Java marks the Timer as cancelled but
     * the static field is not nulled, so subsequent schedule() calls throw
     * "Timer already cancelled".
     */
    public void testRecoversFromCancelledTimerWhenCheckerCounterIsZero() throws Exception {
        Field readCheckTimerField = AbstractInactivityMonitor.class.getDeclaredField("READ_CHECK_TIMER");
        readCheckTimerField.setAccessible(true);

        // Inject a pre-cancelled (but non-null) timer with CHECKER_COUNTER == 0.
        // Without the fix, READ_CHECK_TIMER.schedule() in startConnectCheckTask()
        // throws IllegalStateException and the connection is rejected.
        Timer cancelledTimer = new Timer(READ_CHECK_THREAD_NAME, true);
        cancelledTimer.cancel();
        readCheckTimerField.set(null, cancelledTimer);

        startClient();

        // The recovery in startConnectCheckTask() is synchronous: the new timer is
        // installed before startClient() returns, so no wait is required here.
        Timer recovered = (Timer) readCheckTimerField.get(null);
        assertNotNull("A new timer should have been installed by the recovery path", recovered);
        assertNotSame("Recovered timer must be a new instance, not the cancelled one", cancelledTimer, recovered);

        // Confirm no errors materialised asynchronously after the connection was set up.
        assertFalse("Client should connect without errors",
                Wait.waitFor(() -> clientErrorCount.get() > 0, 2000, 50));
        assertFalse("Server should accept without errors",
                Wait.waitFor(() -> serverErrorCount.get() > 0, 2000, 50));
    }

    /**
     * Verifies that a new connection succeeds when READ_CHECK_TIMER is in a cancelled
     * state while CHECKER_COUNTER > 0 (active connections exist). This is the exact
     * production scenario logged as WARN "Could not accept connection: Timer already
     * cancelled" from TransportConnector when the timer thread dies unexpectedly.
     */
    public void testRecoversFromCancelledTimerWithActiveConnections() throws Exception {
        // Establish a first connection so CHECKER_COUNTER > 0 with a valid timer.
        // startConnectCheckTask() is synchronous: READ_CHECK_TIMER and CHECKER_COUNTER > 0
        // are both set before startClient() returns, so no sleep is needed.
        startClient();

        // Save the first server-side transport before a second connection overwrites the field.
        Transport firstServerTransport = serverTransport;

        Field readCheckTimerField = AbstractInactivityMonitor.class.getDeclaredField("READ_CHECK_TIMER");
        readCheckTimerField.setAccessible(true);

        // Replace the valid static timer with a cancelled one while CHECKER_COUNTER > 0.
        // This simulates the timer thread dying (e.g. due to OOM) with active connections
        // still alive and their tasks silently orphaned on the dead timer.
        Timer cancelledTimer = new Timer(READ_CHECK_THREAD_NAME, true);
        cancelledTimer.cancel();
        readCheckTimerField.set(null, cancelledTimer);

        // A second connection attempt with CHECKER_COUNTER > 0 and a cancelled timer
        // previously hit the unguarded READ_CHECK_TIMER.schedule() call directly
        // (the null-check was skipped) and threw IllegalStateException.
        AtomicBoolean secondClientError = new AtomicBoolean(false);
        Transport secondClient = TransportFactory.connect(
                new URI("tcp://localhost:" + serverPort + "?trace=true&wireFormat.maxInactivityDuration=1000"));
        secondClient.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {}
            @Override
            public void onException(IOException error) {
                secondClientError.set(true);
            }
            @Override
            public void transportInterupted() {}
            @Override
            public void transportResumed() {}
        });

        try {
            secondClient.start();
        } catch (Exception e) {
            fail("Second connection should not fail after cancelled timer recovery: " + e.getMessage());
        }

        // The recovery in startConnectCheckTask() is synchronous: the new timer is
        // installed before secondClient.start() returns.
        Timer recovered = (Timer) readCheckTimerField.get(null);
        assertNotNull("A new timer should have been installed", recovered);
        assertNotSame("Recovered timer must differ from the cancelled one", cancelledTimer, recovered);

        // Confirm no errors materialised asynchronously.
        assertFalse("Second client should connect without error",
                Wait.waitFor(() -> secondClientError.get(), 2000, 50));

        ignoreClientError.set(true);
        ignoreServerError.set(true);
        secondClient.stop();
        firstServerTransport.stop();
    }

    /**
     * Verifies that a new connection succeeds when WRITE_CHECK_TIMER is in a cancelled
     * state. The write timer is set in startMonitorThreads(), which is called during the
     * WireFormatInfo exchange. This test covers the recovery path in that method.
     */
    public void testRecoversFromCancelledWriteCheckTimer() throws Exception {
        // Establish a first connection so startMonitorThreads() runs and initialises
        // WRITE_CHECK_TIMER. That method is triggered asynchronously by the WireFormatInfo
        // exchange, so we wait until the field is populated.
        startClient();

        Field writeCheckTimerField = AbstractInactivityMonitor.class.getDeclaredField("WRITE_CHECK_TIMER");
        writeCheckTimerField.setAccessible(true);

        assertTrue("WRITE_CHECK_TIMER should be set after the WireFormatInfo exchange",
                Wait.waitFor(() -> writeCheckTimerField.get(null) != null, 3000, 50));

        // Save the first server-side transport before a second connection overwrites the field.
        Transport firstServerTransport = serverTransport;

        // Replace the valid WRITE_CHECK_TIMER with a cancelled one while monitor threads
        // are active, simulating the timer thread dying under load.
        Timer cancelledTimer = new Timer(WRITE_CHECK_THREAD_NAME, true);
        cancelledTimer.cancel();
        writeCheckTimerField.set(null, cancelledTimer);

        // The second client's WireFormatInfo exchange triggers startMonitorThreads(), which
        // calls WRITE_CHECK_TIMER.schedule(). The cancelled timer causes an ISE that the
        // fix catches, replacing the timer and rescheduling the write checker.
        AtomicBoolean secondClientError = new AtomicBoolean(false);
        Transport secondClient = TransportFactory.connect(
                new URI("tcp://localhost:" + serverPort + "?trace=true&wireFormat.maxInactivityDuration=1000"));
        secondClient.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {}
            @Override
            public void onException(IOException error) {
                secondClientError.set(true);
            }
            @Override
            public void transportInterupted() {}
            @Override
            public void transportResumed() {}
        });

        try {
            secondClient.start();
        } catch (Exception e) {
            fail("Second connection should not fail after cancelled write-check timer recovery: " + e.getMessage());
        }

        // The write-timer recovery happens inside the WireFormatInfo exchange, which is
        // asynchronous after start() returns. Wait until the cancelled timer is replaced.
        assertTrue("WRITE_CHECK_TIMER should be replaced by the recovery path",
                Wait.waitFor(() -> writeCheckTimerField.get(null) != cancelledTimer, 3000, 50));

        Timer recovered = (Timer) writeCheckTimerField.get(null);
        assertNotNull("A new write check timer should have been installed", recovered);
        assertNotSame("Recovered timer must differ from the cancelled one", cancelledTimer, recovered);

        assertFalse("Second client should connect without error",
                Wait.waitFor(() -> secondClientError.get(), 2000, 50));

        ignoreClientError.set(true);
        ignoreServerError.set(true);
        secondClient.stop();
        firstServerTransport.stop();
    }

    private static boolean hasTimerThread(String name) {
        return name.equals(READ_CHECK_THREAD_NAME) || name.equals(WRITE_CHECK_THREAD_NAME);
    }

    private static Matcher<Iterable<? super String>> hasReadCheckTimer() {
        return hasItem(READ_CHECK_THREAD_NAME);
    }

    private static Matcher<Iterable<? super String>> hasWriteCheckTimer() {
        return hasItem(WRITE_CHECK_THREAD_NAME);
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
