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
package org.apache.activemq.transport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Test for AMQ-9569: WriteTimeoutFilter does not timeout SSL write (handshake).
 *
 * This test demonstrates that when a client connects via SSL to a server that
 * accepts the TCP connection but never responds to the SSL handshake, the
 * WriteTimeoutFilter does NOT enforce the soWriteTimeout during transport start().
 *
 * The SSL handshake is triggered during WireFormatNegotiator.start() ->
 * sendWireFormat() -> TcpTransport.oneway() -> TcpBufferedOutputStream.flush(),
 * which calls SSLSocketImpl.startHandshake() implicitly on the first write.
 * Since WriteTimeoutFilter.start() does not call registerWrite(), the
 * TimeoutThread has nothing to monitor, and the connection blocks indefinitely.
 */
public class SoWriteTimeoutSslHandshakeTest {

    private static final Logger LOG = LoggerFactory.getLogger(SoWriteTimeoutSslHandshakeTest.class);

    private static final String KEYSTORE_TYPE = "jks";
    private static final String PASSWORD = "password";
    private static final String SERVER_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";
    private static final String TRUST_KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";

    /** A plain TCP ServerSocket that accepts connections but never responds (simulates unresponsive SSL peer) */
    private ServerSocket silentServer;
    private ExecutorService executor;
    private final AtomicBoolean serverRunning = new AtomicBoolean(true);

    @Before
    public void setUp() throws Exception {
        // Configure SSL system properties for the client side
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);

        // Start a plain TCP server that accepts connections but never reads/writes
        // This simulates a peer that is unreachable at the SSL layer
        silentServer = new ServerSocket(0);
        executor = Executors.newCachedThreadPool();
        executor.execute(() -> {
            while (serverRunning.get()) {
                try {
                    Socket accepted = silentServer.accept();
                    LOG.info("Silent server accepted connection from: {}", accepted.getRemoteSocketAddress());
                    // Intentionally do nothing - don't read, don't write, don't close
                    // This will cause the SSL handshake to block on the client side
                } catch (IOException e) {
                    if (serverRunning.get()) {
                        LOG.debug("Silent server accept error: {}", e.getMessage());
                    }
                }
            }
        });
        LOG.info("Silent TCP server started on port: {}", silentServer.getLocalPort());
    }

    @After
    public void tearDown() throws Exception {
        serverRunning.set(false);
        if (silentServer != null) {
            silentServer.close();
        }
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * This test proves the bug: WriteTimeoutFilter.start() does NOT register
     * the write timeout, so the SSL handshake blocks beyond the configured
     * soWriteTimeout.
     *
     * Expected behavior (after fix): connection attempt should fail within
     * roughly soWriteTimeout + TimeoutThread polling interval (~2s + ~5s = ~7s).
     *
     * Current behavior (bug): connection attempt blocks for much longer than
     * soWriteTimeout because WriteTimeoutFilter.start() never calls registerWrite().
     *
     * We use a generous upper bound of 15 seconds. If the write timeout worked
     * during start(), the connection should fail within ~7-8 seconds (2s timeout
     * + 5s polling interval + margin). If it takes more than 15 seconds, the
     * timeout is NOT being enforced during start().
     */
    @Test
    public void testSslHandshakeWriteTimeoutNotEnforcedDuringStart() throws Exception {
        final int soWriteTimeout = 2000; // 2 second write timeout
        // Upper bound: soWriteTimeout + TimeoutThread sleep (5s) + margin
        final int expectedMaxSeconds = 15;

        // Use ssl:// with soWriteTimeout pointing to our silent TCP server.
        // The failover transport ensures the connection attempt doesn't just throw
        // immediately but actually tries to establish the SSL connection.
        // maxReconnectAttempts=1 to avoid infinite reconnects.
        String uri = "failover:(ssl://localhost:" + silentServer.getLocalPort()
                + "?soWriteTimeout=" + soWriteTimeout
                + "&socket.verifyHostName=false"
                + ")?maxReconnectAttempts=2"
                + "&startupMaxReconnectAttempts=1"
                + "&initialReconnectDelay=500";

        LOG.info("Connecting with URI: {}", uri);

        final CountDownLatch connectFinished = new CountDownLatch(1);
        final AtomicReference<Exception> connectException = new AtomicReference<>();

        // Run connection attempt in a separate thread since it may block
        executor.execute(() -> {
            Connection connection = null;
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
                connection = factory.createConnection();
                connection.start();
                LOG.info("Connection unexpectedly succeeded");
            } catch (JMSException e) {
                LOG.info("Connection failed as expected: {}", e.getMessage());
                connectException.set(e);
            } finally {
                connectFinished.countDown();
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (JMSException ignored) {
                    }
                }
            }
        });

        // Wait for the connection attempt to complete or timeout
        boolean finished = connectFinished.await(expectedMaxSeconds, TimeUnit.SECONDS);

        if (finished) {
            // The connection attempt completed within the time limit.
            // This means the timeout WAS enforced during start() (fix is working).
            assertNotNull("Connection should have failed with an exception", connectException.get());
            LOG.info("PASS: SSL handshake was timed out correctly within {} seconds", expectedMaxSeconds);
        } else {
            // The connection attempt is still blocking after expectedMaxSeconds.
            // This proves the bug: WriteTimeoutFilter.start() does NOT enforce
            // the write timeout during SSL handshake.
            LOG.warn("BUG CONFIRMED: SSL handshake blocked for more than {} seconds. "
                    + "WriteTimeoutFilter.start() does not register the write timeout. "
                    + "See AMQ-9569.", expectedMaxSeconds);
            fail("AMQ-9569: WriteTimeoutFilter.start() did not enforce soWriteTimeout during SSL handshake. "
                    + "Connection blocked for more than " + expectedMaxSeconds + " seconds. "
                    + "Expected the write timeout (" + soWriteTimeout + "ms) to abort the blocked handshake.");
        }
    }

}
