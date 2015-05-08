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
package org.apache.activemq.transport.amqp.client.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.transport.tcp.TcpBufferedInputStream;
import org.apache.activemq.transport.tcp.TcpBufferedOutputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.InetAddressUtil;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple TCP based transport used by the client.
 */
public class ClientTcpTransport implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTcpTransport.class);

    public interface TransportListener {

        /**
         * Called when new incoming data has become available.
         *
         * @param incoming
         *        the next incoming packet of data.
         */
        void onData(Buffer incoming);

        /**
         * Called if the connection state becomes closed.
         */
        void onTransportClosed();

        /**
         * Called when an error occurs during normal Transport operations.
         *
         * @param cause
         *        the error that triggered this event.
         */
        void onTransportError(Throwable cause);

    }

    private final URI remoteLocation;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> connectionError = new AtomicReference<Throwable>();

    private final Socket socket;
    private DataOutputStream dataOut;
    private DataInputStream dataIn;
    private Thread runner;
    private TransportListener listener;

    private int socketBufferSize = 64 * 1024;
    private int soTimeout = 0;
    private int soLinger = Integer.MIN_VALUE;
    private Boolean keepAlive;
    private Boolean tcpNoDelay = true;
    private boolean useLocalHost = false;
    private int ioBufferSize = 8 * 1024;

    /**
     * Create a new instance of the transport.
     *
     * @param listener
     *        The TransportListener that will receive data from this Transport instance.
     * @param remoteLocation
     *        The remote location where this transport should connection to.
     */
    public ClientTcpTransport(URI remoteLocation) {
        this.remoteLocation = remoteLocation;

        Socket temp = null;
        try {
            temp = createSocketFactory().createSocket();
        } catch (IOException e) {
            connectionError.set(e);
        }

        this.socket = temp;
    }

    public void connect() throws IOException {
        if (connectionError.get() != null) {
            throw IOExceptionSupport.create(connectionError.get());
        }

        if (listener == null) {
            throw new IllegalStateException("Cannot connect until a listener has been set.");
        }

        if (socket == null) {
            throw new IllegalStateException("Cannot connect if the socket or socketFactory have not been set");
        }

        InetSocketAddress remoteAddress = null;

        if (remoteLocation != null) {
            String host = resolveHostName(remoteLocation.getHost());
            remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        }

        socket.connect(remoteAddress);

        connected.set(true);

        initialiseSocket(socket);
        initializeStreams();

        runner = new Thread(null, this, "ClientTcpTransport: " + toString());
        runner.setDaemon(false);
        runner.start();
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (socket == null) {
                return;
            }

            // Closing the streams flush the sockets before closing.. if the socket
            // is hung.. then this hangs the close so we perform an asynchronous close
            // by default which will timeout if the close doesn't happen after a delay.
            final CountDownLatch latch = new CountDownLatch(1);

            final ExecutorService closer = Executors.newSingleThreadExecutor();
            closer.execute(new Runnable() {
                @Override
                public void run() {
                    LOG.trace("Closing socket {}", socket);
                    try {
                        socket.close();
                        LOG.debug("Closed socket {}", socket);
                    } catch (IOException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Caught exception closing socket " + socket + ". This exception will be ignored.", e);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });

            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                closer.shutdownNow();
            }
        }
    }

    public void send(ByteBuffer output) throws IOException {
        checkConnected();
        LOG.trace("Client Transport sending packet of size: {}", output.remaining());
        WritableByteChannel channel = Channels.newChannel(dataOut);
        channel.write(output);
        dataOut.flush();
    }

    public void send(Buffer output) throws IOException {
        checkConnected();
        send(output.toByteBuffer());
    }

    public URI getRemoteURI() {
        return this.remoteLocation;
    }

    public boolean isConnected() {
        return this.connected.get();
    }

    public TransportListener getTransportListener() {
        return this.listener;
    }

    public void setTransportListener(TransportListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be set to null");
        }

        this.listener = listener;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(Boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getSoLinger() {
        return soLinger;
    }

    public void setSoLinger(int soLinger) {
        this.soLinger = soLinger;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isUseLocalHost() {
        return useLocalHost;
    }

    public void setUseLocalHost(boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
    }

    public int getIoBufferSize() {
        return ioBufferSize;
    }

    public void setIoBufferSize(int ioBufferSize) {
        this.ioBufferSize = ioBufferSize;
    }

    //---------- Transport internal implementation ---------------------------//

    @Override
    public void run() {
        LOG.trace("TCP consumer thread for {} starting", this);
        try {
            while (isConnected()) {
                doRun();
            }
        } catch (IOException e) {
            connectionError.set(e);
            onException(e);
        } catch (Throwable e) {
            IOException ioe = new IOException("Unexpected error occured: " + e);
            connectionError.set(ioe);
            ioe.initCause(e);
            onException(ioe);
        }
    }

    protected void doRun() throws IOException {
        int size = dataIn.available();
        if (size <= 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            return;
        }

        byte[] buffer = new byte[size];
        dataIn.readFully(buffer);
        Buffer incoming = new Buffer(buffer);
        listener.onData(incoming);
    }

    /**
     * Passes any IO exceptions into the transport listener
     */
    public void onException(IOException e) {
        if (listener != null) {
            try {
                listener.onTransportError(e);
            } catch (RuntimeException e2) {
                LOG.debug("Unexpected runtime exception: {}", e2.getMessage(), e2);
            }
        }
    }

    protected SocketFactory createSocketFactory() throws IOException {
        if (remoteLocation.getScheme().equalsIgnoreCase("ssl")) {
            return SSLSocketFactory.getDefault();
        } else {
            return SocketFactory.getDefault();
        }
    }

    protected void initialiseSocket(Socket sock) throws SocketException, IllegalArgumentException {
        try {
            sock.setReceiveBufferSize(socketBufferSize);
            sock.setSendBufferSize(socketBufferSize);
        } catch (SocketException se) {
            LOG.warn("Cannot set socket buffer size = {}", socketBufferSize);
            LOG.debug("Cannot set socket buffer size. Reason: {}. This exception is ignored.", se.getMessage(), se);
        }

        sock.setSoTimeout(soTimeout);

        if (keepAlive != null) {
            sock.setKeepAlive(keepAlive.booleanValue());
        }

        if (soLinger > -1) {
            sock.setSoLinger(true, soLinger);
        } else if (soLinger == -1) {
            sock.setSoLinger(false, 0);
        }

        if (tcpNoDelay != null) {
            sock.setTcpNoDelay(tcpNoDelay.booleanValue());
        }
    }

    protected void initializeStreams() throws IOException {
        try {
            TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), ioBufferSize);
            this.dataIn = new DataInputStream(buffIn);
            TcpBufferedOutputStream outputStream = new TcpBufferedOutputStream(socket.getOutputStream(), ioBufferSize);
            this.dataOut = new DataOutputStream(outputStream);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    protected String resolveHostName(String host) throws UnknownHostException {
        if (isUseLocalHost()) {
            String localName = InetAddressUtil.getLocalHostName();
            if (localName != null && localName.equals(host)) {
                return "localhost";
            }
        }
        return host;
    }

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }
}
