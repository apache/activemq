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

package org.apache.activemq.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SocketProxy {

    private static final transient Log LOG = LogFactory.getLog(SocketProxy.class);

    public static final int ACCEPT_TIMEOUT_MILLIS = 1000;

    private URI proxyUrl;
    private URI target;
    private Acceptor acceptor;
    private ServerSocket serverSocket;

    public List<Connection> connections = new LinkedList<Connection>();

    private int listenPort = 0;

    public SocketProxy(URI uri) throws Exception {
        this(0, uri);
    }

    public SocketProxy(int port, URI uri) throws Exception {
        listenPort = port;
        target = uri;
        open();
    }

    protected void open() throws Exception {
        if (proxyUrl == null) {
            serverSocket = new ServerSocket(listenPort);
            proxyUrl = urlFromSocket(target, serverSocket);
        } else {
            serverSocket = new ServerSocket(proxyUrl.getPort());
        }
        acceptor = new Acceptor(serverSocket, target);
        new Thread(null, acceptor, "SocketProxy-Acceptor-" + serverSocket.getLocalPort()).start();
    }

    public URI getUrl() {
        return proxyUrl;
    }

    /*
     * close all proxy connections and acceptor
     */
    public void close() {
        List<Connection> connections;
        synchronized(this.connections) {
            connections = new ArrayList<Connection>(this.connections);
        }            
        LOG.info("close, numConnectons=" + connections.size());
        for (Connection con : connections) {
            closeConnection(con);
        }
        acceptor.close();
    }

    /*
     * called after a close to restart the acceptor on the same port
     */
    public void reopen() {
        LOG.info("reopen");
        try {
            open();
        } catch (Exception e) {
            LOG.debug("exception on reopen url:" + getUrl(), e);
        }
    }

    /*
     * pause accepting new connecitons and data transfer through existing proxy
     * connections. All sockets remain open
     */
    public void pause() {
        synchronized(connections) {
            LOG.info("pause, numConnectons=" + connections.size());
            acceptor.pause();
            for (Connection con : connections) {
                con.pause();
            }
        }
    }

    /*
     * continue after pause
     */
    public void goOn() {
        synchronized(connections) {
            LOG.info("goOn, numConnectons=" + connections.size());
            for (Connection con : connections) {
                con.goOn();
            }
        }
        acceptor.goOn();
    }

    private void closeConnection(Connection c) {
        try {
            c.close();
        } catch (Exception e) {
            LOG.debug("exception on close of: " + c, e);
        }
    }

    private URI urlFromSocket(URI uri, ServerSocket serverSocket) throws Exception {
        int listenPort = serverSocket.getLocalPort();

        return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), listenPort, uri.getPath(), uri.getQuery(), uri.getFragment());
    }

    public class Connection {

        private Socket receiveSocket;
        private Socket sendSocket;
        private Pump requestThread;
        private Pump responseThread;

        public Connection(Socket socket, URI target) throws Exception {
            receiveSocket = socket;
            sendSocket = new Socket(target.getHost(), target.getPort());
            linkWithThreads(receiveSocket, sendSocket);
            LOG.info("proxy connection " + sendSocket);
        }

        public void goOn() {
            responseThread.goOn();
            requestThread.goOn();
        }

        public void pause() {
            requestThread.pause();
            responseThread.pause();
        }

        public void close() throws Exception {
            synchronized(connections) {
                connections.remove(this);
            }
            receiveSocket.close();
            sendSocket.close();
        }

        private void linkWithThreads(Socket source, Socket dest) {
            requestThread = new Pump(source, dest);
            responseThread = new Pump(dest, source);
            requestThread.start();
            responseThread.start();
        }

        public class Pump extends Thread {

            protected Socket src;
            private Socket destination;
            private AtomicReference<CountDownLatch> pause = new AtomicReference<CountDownLatch>();

            public Pump(Socket source, Socket dest) {
                super("SocketProxy-DataTransfer-" + source.getPort() + ":" + dest.getPort());
                src = source;
                destination = dest;
                pause.set(new CountDownLatch(0));
            }

            public void pause() {
                pause.set(new CountDownLatch(1));
            }

            public void goOn() {
                pause.get().countDown();
            }

            public void run() {
                byte[] buf = new byte[1024];
                try {
                    InputStream in = src.getInputStream();
                    OutputStream out = destination.getOutputStream();
                    while (true) {
                        int len = in.read(buf);
                        if (len == -1) {
                            break;
                        }
                        pause.get().await();
                        out.write(buf, 0, len);
                    }
                } catch (Exception e) {
                    LOG.debug("read/write failed, reason: " + e.getLocalizedMessage());
                    try {
                        close();
                    } catch (Exception ignore) {
                    }
                }
            }

        }
    }

    public class Acceptor implements Runnable {

        private ServerSocket socket;
        private URI target;
        private AtomicReference<CountDownLatch> pause = new AtomicReference<CountDownLatch>();


        public Acceptor(ServerSocket serverSocket, URI uri) {
            socket = serverSocket;
            target = uri;
            pause.set(new CountDownLatch(0));
            try {
                socket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        public void pause() {
            pause.set(new CountDownLatch(1));
        }
        
        public void goOn() {
            pause.get().countDown();
        }

        public void run() {
            try {
                while(!socket.isClosed()) {
                    pause.get().await();
                    try {
                        Socket source = socket.accept();
                        LOG.info("accepted " + source);
                        synchronized(connections) {
                            connections.add(new Connection(source, target));
                        }
                    } catch (SocketTimeoutException expected) {
                    }
                }
            } catch (Exception e) {
                LOG.debug("acceptor: finished for reason: " + e.getLocalizedMessage());
            }
        }
        
        public void close() {
            try {
                socket.close();
                goOn();
            } catch (IOException ignored) {
            }
        }
    }
}

