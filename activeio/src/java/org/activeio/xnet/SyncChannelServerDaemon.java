/**
 *
 * Copyright 2005 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.activeio.xnet;

import org.activeio.SyncChannel;
import org.activeio.SyncChannelServer;
import org.activeio.adapter.SyncChannelToSocket;
import org.activeio.net.SocketSyncChannelFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;

public class SyncChannelServerDaemon implements Runnable {
    private static final Log log = LogFactory.getLog(SyncChannelServerDaemon.class);

    private final SocketService socketService;
    private InetAddress address;
    private int port;

    private int timeout;
    private String name;
    private URI bindURI;
    private SyncChannelServer server;

    public SyncChannelServerDaemon(SocketService socketService, InetAddress address, int port) {
        this(null, socketService, address, port);
    }

    public SyncChannelServerDaemon(String name, SocketService socketService, InetAddress address, int port) {
        this.name = name;
        if (socketService == null) {
            throw new IllegalArgumentException("socketService is null");
        }
        this.socketService = socketService;
        this.address = address;
        this.port = port;
        try {
            this.bindURI = new URI("uri", null, address.getHostName(), port, null, null, null);
        } catch (URISyntaxException e) {
            throw (IllegalArgumentException) new IllegalArgumentException().initCause(e);
        }
    }

    public void doStart() throws Exception {
        SocketSyncChannelFactory factory = new SocketSyncChannelFactory();
        server = null;

        try {
            server = factory.bindSyncChannel(bindURI);
            port = server.getConnectURI().getPort();
            address = InetAddress.getByName(server.getConnectURI().getHost());
            stopped = false;
//            server.setSoTimeout(timeout);
        } catch (Exception e) {
            throw new ServiceException("Service failed to open socket", e);
        }
        Thread thread = new Thread(this);
        thread.setName("service." + name + "@" + hashCode());
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void doStop() {
        stopped = true;
    }

    public synchronized void doFail() {
        doStop();
        if (server != null) {
            server.dispose();
        }
    }

    public void setSoTimeout(int timeout) throws SocketException {
        this.timeout = timeout;
    }

    public int getSoTimeout() throws IOException {
        return timeout;
    }

    /**
     * Gets the inetAddress number that the
     * daemon is listening on.
     */
    public InetAddress getAddress() {
        return address;
    }

    /**
     * Gets the port number that the
     * daemon is listening on.
     */
    public int getPort() {
        return port;
    }

    public void run() {
        while (!shouldStop()) {
            Socket socket = null;
            try {
                SyncChannel channel = (SyncChannel) server.accept(timeout);
                socket = new SyncChannelToSocket(channel);
                socket.setTcpNoDelay(true);
                if (!shouldStop()) {
                    // the server service is responsible
                    // for closing the socket.
                    this.socketService.service(socket);
                }
            } catch (SocketTimeoutException e) {
                // we don't really care
                log.debug("Socket timed-out", e);
            } catch (Throwable e) {
                log.error("Unexpected error", e);
            } finally {
                log.info("Processed");
            }
        }

        if (server != null) {
            try {
                server.dispose();
            } catch (Exception ioException) {
                log.debug("Error cleaning up socked", ioException);
            }
            server = null;
        }
    }

    private boolean stopped;

    public synchronized void stop() {
        stopped = true;
    }

    private synchronized boolean shouldStop() {
        return stopped;
    }
}

