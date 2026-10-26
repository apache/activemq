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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TcpTransportSocketOptionsTest {

    private static final int LARGE_BUFFER = 512 * 1024;

    private ServerSocket serverSocket;
    private RecordingSocket clientSocket;
    private TcpTransport transport;

    @Before
    public void setUp() throws Exception {
        serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        clientSocket = new RecordingSocket();
    }

    @After
    public void tearDown() throws Exception {
        if (transport != null) {
            transport.stop();
        }
        serverSocket.close();
    }

    @Test(timeout = 30000)
    public void testSocketOptionsAppliedToGivenSocket() throws Exception {
        transport = newTransport(64 * 1024);
        Map<String, Object> options = new HashMap<>();
        options.put("keepAlive", "true");
        transport.setSocketOptions(options);

        try (Socket other = new Socket()) {
            transport.initialiseSocket(other);
            assertTrue(other.getKeepAlive());
            assertFalse(clientSocket.getKeepAlive());
        }
    }

    @Test(timeout = 30000)
    public void testDefaultBufferSizeAppliedBeforeConnect() throws Exception {
        transport = newTransport(64 * 1024);
        transport.start();

        assertEquals(64 * 1024, clientSocket.receiveBufferSizeAtConnect);
        assertEquals(64 * 1024, clientSocket.sendBufferSizeAtConnect);
    }

    @Test(timeout = 30000)
    public void testSingleSocketBufferOptionKeepsSocketBufferSizeForOtherDirection() throws Exception {
        transport = newTransport(128 * 1024);
        Map<String, Object> options = new HashMap<>();
        options.put("receiveBufferSize", String.valueOf(LARGE_BUFFER));
        transport.setSocketOptions(options);
        transport.start();

        assertEquals(LARGE_BUFFER, clientSocket.receiveBufferSizeAtConnect);
        assertEquals(128 * 1024, clientSocket.sendBufferSizeAtConnect);
        assertTrue(clientSocket.getReceiveBufferSize() >= LARGE_BUFFER);
    }

    @Test(timeout = 30000)
    public void testWarnsWhenSocketBufferSizeConflictsWithSocketOption() throws Exception {
        transport = newTransport(128 * 1024);
        Map<String, Object> options = new HashMap<>();
        options.put("receiveBufferSize", String.valueOf(LARGE_BUFFER));
        transport.setSocketOptions(options);

        List<String> warnings = captureWarnings(() -> transport.start());

        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("socket.receiveBufferSize=" + LARGE_BUFFER));
    }

    @Test(timeout = 30000)
    public void testNoWarningWhenSocketBufferSizeNotExplicitlySet() throws Exception {
        URI uri = new URI("tcp://localhost:" + serverSocket.getLocalPort());
        transport = new TcpTransport(new OpenWireFormat(), new SingleSocketFactory(clientSocket), uri, null);
        transport.setTransportListener(new DefaultTransportListener());
        Map<String, Object> options = new HashMap<>();
        options.put("receiveBufferSize", String.valueOf(LARGE_BUFFER));
        transport.setSocketOptions(options);

        assertTrue(captureWarnings(() -> transport.start()).isEmpty());
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private List<String> captureWarnings(ThrowingRunnable action) throws Exception {
        List<String> warnings = new ArrayList<>();
        Logger logger = (Logger) LogManager.getLogger(TcpTransport.class);
        DefaultTestAppender appender = new DefaultTestAppender() {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warnings.add(event.getMessage().getFormattedMessage());
                }
            }
        };
        appender.start();
        logger.addAppender(appender);
        try {
            action.run();
        } finally {
            logger.removeAppender(appender);
        }
        return warnings;
    }

    private TcpTransport newTransport(int socketBufferSize) throws Exception {
        URI uri = new URI("tcp://localhost:" + serverSocket.getLocalPort());
        TcpTransport tcpTransport = new TcpTransport(new OpenWireFormat(), new SingleSocketFactory(clientSocket), uri, null);
        tcpTransport.setSocketBufferSize(socketBufferSize);
        tcpTransport.setTransportListener(new DefaultTransportListener());
        return tcpTransport;
    }

    private static class RecordingSocket extends Socket {
        volatile int receiveBufferSizeAtConnect = -1;
        volatile int sendBufferSizeAtConnect = -1;
        private volatile int requestedReceive = -1;
        private volatile int requestedSend = -1;

        @Override
        public synchronized void setReceiveBufferSize(int size) throws SocketException {
            requestedReceive = size;
            super.setReceiveBufferSize(size);
        }

        @Override
        public synchronized void setSendBufferSize(int size) throws SocketException {
            requestedSend = size;
            super.setSendBufferSize(size);
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            receiveBufferSizeAtConnect = requestedReceive;
            sendBufferSizeAtConnect = requestedSend;
            super.connect(endpoint, timeout);
        }
    }

    private static class SingleSocketFactory extends SocketFactory {
        private final Socket socket;

        SingleSocketFactory(Socket socket) {
            this.socket = socket;
        }

        @Override
        public Socket createSocket() {
            return socket;
        }

        @Override
        public Socket createSocket(String host, int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Socket createSocket(InetAddress host, int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) {
            throw new UnsupportedOperationException();
        }
    }
}
