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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocket;

import org.apache.activemq.Service;
import org.apache.activemq.ThreadPriorities;
import org.apache.activemq.TransportLoggerSupport;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerThreadSupport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceListener;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TCP based implementation of {@link TransportServer}
 */
public class TcpTransportServer extends TransportServerThreadSupport implements ServiceListener {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportServer.class);

    protected volatile ServerSocket serverSocket;
    protected volatile Selector selector;
    protected int backlog = 5000;
    protected WireFormatFactory wireFormatFactory = new OpenWireFormatFactory();
    protected final TcpTransportFactory transportFactory;
    protected long maxInactivityDuration = 30000;
    protected long maxInactivityDurationInitalDelay = 10000;
    protected int minmumWireFormatVersion;
    protected boolean useQueueForAccept = true;
    protected boolean allowLinkStealing;

    /**
     * trace=true -> the Transport stack where this TcpTransport object will be, will have a TransportLogger layer
     * trace=false -> the Transport stack where this TcpTransport object will be, will NOT have a TransportLogger layer,
     * and therefore will never be able to print logging messages. This parameter is most probably set in Connection or
     * TransportConnector URIs.
     */
    protected boolean trace = false;

    protected int soTimeout = 0;
    protected int socketBufferSize = 64 * 1024;
    protected int connectionTimeout = 30000;

    /**
     * Name of the LogWriter implementation to use. Names are mapped to classes in the
     * resources/META-INF/services/org/apache/activemq/transport/logwriters directory. This parameter is most probably
     * set in Connection or TransportConnector URIs.
     */
    protected String logWriterName = TransportLoggerSupport.defaultLogWriterName;

    /**
     * Specifies if the TransportLogger will be manageable by JMX or not. Also, as long as there is at least 1
     * TransportLogger which is manageable, a TransportLoggerControl MBean will me created.
     */
    protected boolean dynamicManagement = false;

    /**
     * startLogging=true -> the TransportLogger object of the Transport stack will initially write messages to the log.
     * startLogging=false -> the TransportLogger object of the Transport stack will initially NOT write messages to the
     * log. This parameter only has an effect if trace == true. This parameter is most probably set in Connection or
     * TransportConnector URIs.
     */
    protected boolean startLogging = true;
    protected int jmxPort = TransportLoggerSupport.defaultJmxPort;
    protected final ServerSocketFactory serverSocketFactory;
    protected final BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>();
    protected Thread socketHandlerThread;

    /**
     * The maximum number of sockets allowed for this server
     */
    protected int maximumConnections = Integer.MAX_VALUE;
    protected final AtomicInteger currentTransportCount = new AtomicInteger();

    public TcpTransportServer(TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory) throws IOException,
        URISyntaxException {
        super(location);
        this.transportFactory = transportFactory;
        this.serverSocketFactory = serverSocketFactory;
    }

    public void bind() throws IOException {
        URI bind = getBindLocation();

        String host = bind.getHost();
        host = (host == null || host.length() == 0) ? "localhost" : host;
        InetAddress addr = InetAddress.getByName(host);

        try {
            serverSocket = serverSocketFactory.createServerSocket(bind.getPort(), backlog, addr);
            configureServerSocket(serverSocket);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to bind to server socket: " + bind + " due to: " + e, e);
        }
        try {
            setConnectURI(new URI(bind.getScheme(), bind.getUserInfo(), resolveHostName(serverSocket, addr), serverSocket.getLocalPort(), bind.getPath(),
                bind.getQuery(), bind.getFragment()));
        } catch (URISyntaxException e) {
            // it could be that the host name contains invalid characters such
            // as _ on unix platforms so lets try use the IP address instead
            try {
                setConnectURI(new URI(bind.getScheme(), bind.getUserInfo(), addr.getHostAddress(), serverSocket.getLocalPort(), bind.getPath(),
                    bind.getQuery(), bind.getFragment()));
            } catch (URISyntaxException e2) {
                throw IOExceptionSupport.create(e2);
            }
        }
    }

    private void configureServerSocket(ServerSocket socket) throws SocketException {
        socket.setSoTimeout(2000);
        if (transportOptions != null) {

            // If the enabledCipherSuites option is invalid we don't want to ignore it as the call
            // to SSLServerSocket to configure it has a side effect on the socket rendering it
            // useless as all suites are enabled many of which are considered as insecure.  We
            // instead trap that option here and throw an exception.  We should really consider
            // all invalid options as breaking and not start the transport but the current design
            // doesn't really allow for this.
            //
            //  see: https://issues.apache.org/jira/browse/AMQ-4582
            //
            if (socket instanceof SSLServerSocket) {
                if (transportOptions.containsKey("enabledCipherSuites")) {
                    Object cipherSuites = transportOptions.remove("enabledCipherSuites");

                    if (!IntrospectionSupport.setProperty(socket, "enabledCipherSuites", cipherSuites)) {
                        throw new SocketException(String.format(
                            "Invalid transport options {enabledCipherSuites=%s}", cipherSuites));
                    }
                }
            }

            //AMQ-6599 - don't strip out set properties on the socket as we need to set them
            //on the Transport as well later
            IntrospectionSupport.setProperties(socket, transportOptions, false);
        }
    }

    /**
     * @return Returns the wireFormatFactory.
     */
    public WireFormatFactory getWireFormatFactory() {
        return wireFormatFactory;
    }

    /**
     * @param wireFormatFactory
     *            The wireFormatFactory to set.
     */
    public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
        this.wireFormatFactory = wireFormatFactory;
    }

    /**
     * Associates a broker info with the transport server so that the transport can do discovery advertisements of the
     * broker.
     *
     * @param brokerInfo
     */
    @Override
    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    public long getMaxInactivityDuration() {
        return maxInactivityDuration;
    }

    public void setMaxInactivityDuration(long maxInactivityDuration) {
        this.maxInactivityDuration = maxInactivityDuration;
    }

    public long getMaxInactivityDurationInitalDelay() {
        return this.maxInactivityDurationInitalDelay;
    }

    public void setMaxInactivityDurationInitalDelay(long maxInactivityDurationInitalDelay) {
        this.maxInactivityDurationInitalDelay = maxInactivityDurationInitalDelay;
    }

    public int getMinmumWireFormatVersion() {
        return minmumWireFormatVersion;
    }

    public void setMinmumWireFormatVersion(int minmumWireFormatVersion) {
        this.minmumWireFormatVersion = minmumWireFormatVersion;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public String getLogWriterName() {
        return logWriterName;
    }

    public void setLogWriterName(String logFormat) {
        this.logWriterName = logFormat;
    }

    public boolean isDynamicManagement() {
        return dynamicManagement;
    }

    public void setDynamicManagement(boolean useJmx) {
        this.dynamicManagement = useJmx;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public boolean isStartLogging() {
        return startLogging;
    }

    public void setStartLogging(boolean startLogging) {
        this.startLogging = startLogging;
    }

    /**
     * @return the backlog
     */
    public int getBacklog() {
        return backlog;
    }

    /**
     * @param backlog
     *            the backlog to set
     */
    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    /**
     * @return the useQueueForAccept
     */
    public boolean isUseQueueForAccept() {
        return useQueueForAccept;
    }

    /**
     * @param useQueueForAccept
     *            the useQueueForAccept to set
     */
    public void setUseQueueForAccept(boolean useQueueForAccept) {
        this.useQueueForAccept = useQueueForAccept;
    }

    /**
     * pull Sockets from the ServerSocket
     */
    @Override
    public void run() {
        if (!isStopped() && !isStopping()) {
            final ServerSocket serverSocket = this.serverSocket;
            if (serverSocket == null) {
                onAcceptError(new IOException("Server started without a valid ServerSocket"));
            }

            final ServerSocketChannel channel = serverSocket.getChannel();
            if (channel != null) {
                doRunWithServerSocketChannel(channel);
            } else {
                doRunWithServerSocket(serverSocket);
            }
        }
    }

    private void doRunWithServerSocketChannel(final ServerSocketChannel channel) {
        try {
            channel.configureBlocking(false);
            final Selector selector = Selector.open();

            try {
                channel.register(selector, SelectionKey.OP_ACCEPT);
            } catch (ClosedChannelException ex) {
                try {
                    selector.close();
                } catch (IOException ignore) {}

                throw ex;
            }

            // Update object instance for later cleanup.
            this.selector = selector;

            while (!isStopped()) {
                int count = selector.select(10);

                if (count == 0) {
                    continue;
                }

                Set<SelectionKey> keys = selector.selectedKeys();

                for (Iterator<SelectionKey> i = keys.iterator(); i.hasNext(); ) {
                    final SelectionKey key = i.next();
                    if (key.isAcceptable()) {
                        try {
                            SocketChannel sc = channel.accept();
                            if (sc != null) {
                                if (isStopped() || getAcceptListener() == null) {
                                    sc.close();
                                } else {
                                    if (useQueueForAccept) {
                                        socketQueue.put(sc.socket());
                                    } else {
                                        handleSocket(sc.socket());
                                    }
                                }
                            }

                        } catch (SocketTimeoutException ste) {
                            // expect this to happen
                        } catch (Exception e) {
                            e.printStackTrace();
                            if (!isStopping()) {
                                onAcceptError(e);
                            } else if (!isStopped()) {
                                LOG.warn("run()", e);
                                onAcceptError(e);
                            }
                        }
                    }
                    i.remove();
                }
            }
        } catch (IOException ex) {
            if (!isStopping()) {
                onAcceptError(ex);
            } else if (!isStopped()) {
                LOG.warn("run()", ex);
                onAcceptError(ex);
            }
        }
    }

    private void doRunWithServerSocket(final ServerSocket serverSocket) {
        while (!isStopped()) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                if (socket != null) {
                    if (isStopped() || getAcceptListener() == null) {
                        socket.close();
                    } else {
                        if (useQueueForAccept) {
                            socketQueue.put(socket);
                        } else {
                            handleSocket(socket);
                        }
                    }
                }
            } catch (SocketTimeoutException ste) {
                // expect this to happen
            } catch (Exception e) {
                if (!isStopping()) {
                    onAcceptError(e);
                } else if (!isStopped()) {
                    LOG.warn("run()", e);
                    onAcceptError(e);
                }
            }
        }
    }

    /**
     * Allow derived classes to override the Transport implementation that this transport server creates.
     *
     * @param socket
     * @param format
     *
     * @return a new Transport instance.
     *
     * @throws IOException
     */
    protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
        return new TcpTransport(format, socket);
    }

    /**
     * @return pretty print of this
     */
    @Override
    public String toString() {
        return "" + getBindLocation();
    }

    /**
     * @param socket
     * @param bindAddress
     * @return real hostName
     * @throws UnknownHostException
     */
    protected String resolveHostName(ServerSocket socket, InetAddress bindAddress) throws UnknownHostException {
        String result = null;
        if (socket.isBound()) {
            if (socket.getInetAddress().isAnyLocalAddress()) {
                // make it more human readable and useful, an alternative to 0.0.0.0
                result = InetAddressUtil.getLocalHostName();
            } else {
                result = socket.getInetAddress().getCanonicalHostName();
            }
        } else {
            result = bindAddress.getCanonicalHostName();
        }
        return result;
    }

    @Override
    protected void doStart() throws Exception {
        if (useQueueForAccept) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        while (!isStopped() && !isStopping()) {
                            Socket sock = socketQueue.poll(1, TimeUnit.SECONDS);
                            if (sock != null) {
                                try {
                                    handleSocket(sock);
                                } catch (Throwable thrown) {
                                    if (!isStopping()) {
                                        onAcceptError(new Exception(thrown));
                                    } else if (!isStopped()) {
                                        LOG.warn("Unexpected error thrown during accept handling: ", thrown);
                                        onAcceptError(new Exception(thrown));
                                    }
                                }
                            }
                        }

                    } catch (InterruptedException e) {
                        if (!isStopped() || !isStopping()) {
                            LOG.info("socketQueue interrupted - stopping");
                            onAcceptError(e);
                        }
                    }
                }
            };
            socketHandlerThread = new Thread(null, run, "ActiveMQ Transport Server Thread Handler: " + toString(), getStackSize());
            socketHandlerThread.setDaemon(true);
            socketHandlerThread.setPriority(ThreadPriorities.BROKER_MANAGEMENT - 1);
            socketHandlerThread.start();
        }
        super.doStart();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        Exception firstFailure = null;

        try {
            if (selector != null) {
                selector.close();
                selector = null;
            }
        } catch (Exception error) {
        }

        try {
            final ServerSocket serverSocket = this.serverSocket;
            if (serverSocket != null) {
                this.serverSocket = null;
                serverSocket.close();
            }
        } catch (Exception error) {
            firstFailure = error;
        }

        if (socketHandlerThread != null) {
            socketHandlerThread.interrupt();
            socketHandlerThread = null;
        }

        try {
            super.doStop(stopper);
        } catch (Exception error) {
            if (firstFailure != null) {
                firstFailure = error;
            }
        }

        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    protected void handleSocket(Socket socket) {
        doHandleSocket(socket);
    }

    final protected void doHandleSocket(Socket socket) {
        boolean closeSocket = true;
        boolean countIncremented = false;
        try {
            int currentCount;
            do {
                currentCount = currentTransportCount.get();
                if (currentCount >= this.maximumConnections) {
                     throw new ExceededMaximumConnectionsException(
                         "Exceeded the maximum number of allowed client connections. See the '" +
                         "maximumConnections' property on the TCP transport configuration URI " +
                         "in the ActiveMQ configuration file (e.g., activemq.xml)");
                 }

            //Increment this value before configuring the transport
            //This is necessary because some of the transport servers must read from the
            //socket during configureTransport() so we want to make sure this value is
            //accurate as the transport server could pause here waiting for data to be sent from a client
            } while(!currentTransportCount.compareAndSet(currentCount, currentCount + 1));
            countIncremented = true;

            HashMap<String, Object> options = new HashMap<String, Object>();
            options.put("maxInactivityDuration", Long.valueOf(maxInactivityDuration));
            options.put("maxInactivityDurationInitalDelay", Long.valueOf(maxInactivityDurationInitalDelay));
            options.put("minmumWireFormatVersion", Integer.valueOf(minmumWireFormatVersion));
            options.put("trace", Boolean.valueOf(trace));
            options.put("soTimeout", Integer.valueOf(soTimeout));
            options.put("socketBufferSize", Integer.valueOf(socketBufferSize));
            options.put("connectionTimeout", Integer.valueOf(connectionTimeout));
            options.put("logWriterName", logWriterName);
            options.put("dynamicManagement", Boolean.valueOf(dynamicManagement));
            options.put("startLogging", Boolean.valueOf(startLogging));
            options.put("jmxPort", Integer.valueOf(jmxPort));
            options.putAll(transportOptions);

            TransportInfo transportInfo = configureTransport(this, socket);
            closeSocket = false;

            if (transportInfo.transport instanceof ServiceSupport) {
                ((ServiceSupport) transportInfo.transport).addServiceListener(this);
            }

            Transport configuredTransport = transportInfo.transportFactory.serverConfigure(
                    transportInfo.transport, transportInfo.format, options);

            getAcceptListener().onAccept(configuredTransport);

        } catch (SocketTimeoutException ste) {
            // expect this to happen
        } catch (Exception e) {
            if (closeSocket) {
                try {
                    //if closing the socket, only decrement the count it was actually incremented
                    //where it was incremented
                    if (countIncremented) {
                        currentTransportCount.decrementAndGet();
                    }
                    socket.close();
                } catch (Exception ignore) {
                }
            }

            if (!isStopping()) {
                onAcceptError(e);
            } else if (!isStopped()) {
                LOG.warn("run()", e);
                onAcceptError(e);
            }
        }
    }

    protected TransportInfo configureTransport(final TcpTransportServer server, final Socket socket) throws Exception {
        WireFormat format = wireFormatFactory.createWireFormat();
        Transport transport = createTransport(socket, format);
        return new TransportInfo(format, transport, transportFactory);
    }

    protected class TransportInfo {
        final WireFormat format;
        final Transport transport;
        final TransportFactory transportFactory;

        public TransportInfo(WireFormat format, Transport transport, TransportFactory transportFactory) {
            this.format = format;
            this.transport = transport;
            this.transportFactory = transportFactory;
        }
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * @return the maximumConnections
     */
    public int getMaximumConnections() {
        return maximumConnections;
    }

    /**
     * @param maximumConnections
     *            the maximumConnections to set
     */
    public void setMaximumConnections(int maximumConnections) {
        this.maximumConnections = maximumConnections;
    }

    public AtomicInteger getCurrentTransportCount() {
        return currentTransportCount;
    }

    @Override
    public void started(Service service) {
    }

    @Override
    public void stopped(Service service) {
        this.currentTransportCount.decrementAndGet();
    }

    @Override
    public boolean isSslServer() {
        return false;
    }

    @Override
    public boolean isAllowLinkStealing() {
        return allowLinkStealing;
    }

    @Override
    public void setAllowLinkStealing(boolean allowLinkStealing) {
        this.allowLinkStealing = allowLinkStealing;
    }
}
