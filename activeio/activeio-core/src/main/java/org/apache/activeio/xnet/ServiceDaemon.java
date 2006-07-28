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
package org.apache.activeio.xnet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Properties;

public class ServiceDaemon implements ServerService {
    private static final Log log = LogFactory.getLog(ServiceDaemon.class);

    private final SocketService socketService;
    private final InetAddress address;
    private int port;

    private SocketListener socketListener;
    private int timeout;
    private String name;

    public ServiceDaemon(SocketService socketService, InetAddress address, int port) {
        this(null, socketService, address, port);
    }

    public ServiceDaemon(String name, SocketService socketService, InetAddress address, int port) {
        this.name = name;
        if (socketService == null) {
            throw new IllegalArgumentException("socketService is null");
        }
        this.socketService = socketService;
        this.address = address;
        this.port = port;
    }

    public void setSoTimeout(int timeout) throws SocketException {
        this.timeout = timeout;
        if (socketListener != null) {
            socketListener.setSoTimeout(timeout);
        }
    }

    public int getSoTimeout() throws IOException {
        if (socketListener == null) return 0;
        return socketListener.getSoTimeout();
    }

    public String getServiceName() {
        return socketService.getName();
    }

    /**
     * Gets the inetAddress number that the
     * daemon is listening on.
     */
    public InetAddress getAddress() {
        return address;
    }

    public void init(Properties props) throws Exception {
    }

    public void start() throws ServiceException {
        synchronized (this) {
            // Don't bother if we are already started/starting
            if (socketListener != null) {
                return;
            }

            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(port, 20, address);
                port = serverSocket.getLocalPort();
                serverSocket.setSoTimeout(timeout);
            } catch (Exception e) {
                throw new ServiceException("Service failed to open socket", e);
            }

            socketListener = new SocketListener(socketService, serverSocket);
            Thread thread = new Thread(socketListener);
            thread.setName("service." + name + "@" + socketListener.hashCode());
            thread.setDaemon(true);
            thread.start();
        }
    }

    public void stop() throws ServiceException {
        synchronized (this) {
            if (socketListener != null) {
                socketListener.stop();
                socketListener = null;
            }
        }
    }

    public String getIP() {
        return null;
    }

    /**
     * Gets the port number that the
     * daemon is listening on.
     */
    public int getPort() {
        return port;
    }

    public void service(Socket socket) throws ServiceException, IOException {
    }

    public String getName() {
        return null;
    }

    private static class SocketListener implements Runnable {
        private SocketService serverService;
        private ServerSocket serverSocket;
        private boolean stopped;

        public SocketListener(SocketService serverService, ServerSocket serverSocket) {
            this.serverService = serverService;
            this.serverSocket = serverSocket;
            stopped = false;
        }

        public synchronized void stop() {
            stopped = true;
        }

        private synchronized boolean shouldStop() {
            return stopped;
        }

        public void run() {
            while (!shouldStop()) {
                Socket socket = null;
                try {
                    socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    if (!shouldStop()) {
                        // the server service is responsible
                        // for closing the socket.
                        serverService.service(socket);
                    }
                } catch (SocketTimeoutException e) {
                    // we don't really care
                    // log.debug("Socket timed-out",e);
                } catch (Throwable e) {
                    log.error("Unexpected error", e);
                }
            }

            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException ioException) {
                    log.debug("Error cleaning up socked", ioException);
                }
                serverSocket = null;
            }
            serverService = null;
        }

        public void setSoTimeout(int timeout) throws SocketException {
            serverSocket.setSoTimeout(timeout);
        }

        public int getSoTimeout() throws IOException {
            return serverSocket.getSoTimeout();
        }
    }


}

