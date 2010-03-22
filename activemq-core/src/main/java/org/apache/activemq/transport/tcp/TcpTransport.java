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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.SocketFactory;
import org.apache.activemq.Service;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportThreadSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 * @version $Revision$
 */
public class TcpTransport extends TransportThreadSupport implements Transport, Service, Runnable {
    private static final Log LOG = LogFactory.getLog(TcpTransport.class);
    private static final ThreadPoolExecutor SOCKET_CLOSE;
    protected final URI remoteLocation;
    protected final URI localLocation;
    protected final WireFormat wireFormat;

    protected int connectionTimeout = 30000;
    protected int soTimeout;
    protected int socketBufferSize = 64 * 1024;
    protected int ioBufferSize = 8 * 1024;
    protected boolean closeAsync=true;
    protected Socket socket;
    protected DataOutputStream dataOut;
    protected DataInputStream dataIn;
    protected TcpBufferedOutputStream buffOut = null;
    /**
     * The Traffic Class to be set on the socket.
     */
    protected int trafficClass = 0;
    /**
     * Keeps track of attempts to set the Traffic Class on the socket.
     */
    private boolean trafficClassSet = false;
    /**
     * Prevents setting both the Differentiated Services and Type of Service
     * transport options at the same time, since they share the same spot in
     * the TCP/IP packet headers.
     */
    protected boolean diffServChosen = false;
    protected boolean typeOfServiceChosen = false;
    /**
     * trace=true -> the Transport stack where this TcpTransport
     * object will be, will have a TransportLogger layer
     * trace=false -> the Transport stack where this TcpTransport
     * object will be, will NOT have a TransportLogger layer, and therefore
     * will never be able to print logging messages.
     * This parameter is most probably set in Connection or TransportConnector URIs.
     */
    protected boolean trace = false;
    /**
     * Name of the LogWriter implementation to use.
     * Names are mapped to classes in the resources/META-INF/services/org/apache/activemq/transport/logwriters directory.
     * This parameter is most probably set in Connection or TransportConnector URIs.
     */
    protected String logWriterName = TransportLoggerFactory.defaultLogWriterName;
    /**
     * Specifies if the TransportLogger will be manageable by JMX or not.
     * Also, as long as there is at least 1 TransportLogger which is manageable,
     * a TransportLoggerControl MBean will me created.
     */
    protected boolean dynamicManagement = false;
    /**
     * startLogging=true -> the TransportLogger object of the Transport stack
     * will initially write messages to the log.
     * startLogging=false -> the TransportLogger object of the Transport stack
     * will initially NOT write messages to the log.
     * This parameter only has an effect if trace == true.
     * This parameter is most probably set in Connection or TransportConnector URIs.
     */
    protected boolean startLogging = true;
    /**
     * Specifies the port that will be used by the JMX server to manage
     * the TransportLoggers.
     * This should only be set in an URI by a client (producer or consumer) since
     * a broker will already create a JMX server.
     * It is useful for people who test a broker and clients in the same machine
     * and want to control both via JMX; a different port will be needed.
     */
    protected int jmxPort = 1099;
    protected boolean useLocalHost = true;
    protected int minmumWireFormatVersion;
    protected SocketFactory socketFactory;
    protected final AtomicReference<CountDownLatch> stoppedLatch = new AtomicReference<CountDownLatch>();

    private Map<String, Object> socketOptions;
    private Boolean keepAlive;
    private Boolean tcpNoDelay;
    private Thread runnerThread;
    private volatile int receiveCounter;

    /**
     * Connect to a remote Node - e.g. a Broker
     * 
     * @param wireFormat
     * @param socketFactory
     * @param remoteLocation
     * @param localLocation - e.g. local InetAddress and local port
     * @throws IOException
     * @throws UnknownHostException
     */
    public TcpTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation,
                        URI localLocation) throws UnknownHostException, IOException {
        this.wireFormat = wireFormat;
        this.socketFactory = socketFactory;
        try {
            this.socket = socketFactory.createSocket();
        } catch (SocketException e) {
            this.socket = null;
        }
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        setDaemon(false);
    }

    /**
     * Initialize from a server Socket
     * 
     * @param wireFormat
     * @param socket
     * @throws IOException
     */
    public TcpTransport(WireFormat wireFormat, Socket socket) throws IOException {
        this.wireFormat = wireFormat;
        this.socket = socket;
        this.remoteLocation = null;
        this.localLocation = null;
        setDaemon(true);
    }

    /**
     * A one way asynchronous send
     */
    public void oneway(Object command) throws IOException {
        checkStarted();
        wireFormat.marshal(command, dataOut);
        dataOut.flush();
    }

    /**
     * @return pretty print of 'this'
     */
    @Override
    public String toString() {
        return "tcp://" + socket.getInetAddress() + ":" + socket.getPort();
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        LOG.trace("TCP consumer thread for " + this + " starting");
        this.runnerThread=Thread.currentThread();
        try {
            while (!isStopped()) {
                doRun();
            }
        } catch (IOException e) {
            stoppedLatch.get().countDown();
            onException(e);
        } catch (Throwable e){
            stoppedLatch.get().countDown();
            IOException ioe=new IOException("Unexpected error occured");
            ioe.initCause(e);
            onException(ioe);
        }finally {
            stoppedLatch.get().countDown();
        }
    }

    protected void doRun() throws IOException {
        try {
            Object command = readCommand();
            doConsume(command);
        } catch (SocketTimeoutException e) {
        } catch (InterruptedIOException e) {
        }
    }

    protected Object readCommand() throws IOException {
        return wireFormat.unmarshal(dataIn);
    }

    // Properties
    // -------------------------------------------------------------------------
    public String getDiffServ() {
        // This is the value requested by the user by setting the Tcp Transport
        // options. If the socket hasn't been created, then this value may not
        // reflect the value returned by Socket.getTrafficClass().
        return Integer.toString(this.trafficClass);
    }

    public void setDiffServ(String diffServ) throws IllegalArgumentException {
        this.trafficClass = QualityOfServiceUtils.getDSCP(diffServ);
        this.diffServChosen = true;
    }

    public int getTypeOfService() {
        // This is the value requested by the user by setting the Tcp Transport
        // options. If the socket hasn't been created, then this value may not
        // reflect the value returned by Socket.getTrafficClass().
        return this.trafficClass;
    }
  
    public void setTypeOfService(int typeOfService) {
        this.trafficClass = QualityOfServiceUtils.getToS(typeOfService);
        this.typeOfServiceChosen = true;
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

    public boolean isStartLogging() {
        return startLogging;
    }

    public void setStartLogging(boolean startLogging) {
        this.startLogging = startLogging;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }
    
    public int getMinmumWireFormatVersion() {
        return minmumWireFormatVersion;
    }

    public void setMinmumWireFormatVersion(int minmumWireFormatVersion) {
        this.minmumWireFormatVersion = minmumWireFormatVersion;
    }

    public boolean isUseLocalHost() {
        return useLocalHost;
    }

    /**
     * Sets whether 'localhost' or the actual local host name should be used to
     * make local connections. On some operating systems such as Macs its not
     * possible to connect as the local host name so localhost is better.
     */
    public void setUseLocalHost(boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /**
     * Sets the buffer size to use on the socket
     */
    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    /**
     * Sets the socket timeout
     */
    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the timeout used to connect to the socket
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Boolean getKeepAlive() {
        return keepAlive;
    }

    /**
     * Enable/disable TCP KEEP_ALIVE mode
     */
    public void setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public Boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Enable/disable the TCP_NODELAY option on the socket
     */
    public void setTcpNoDelay(Boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * @return the ioBufferSize
     */
    public int getIoBufferSize() {
        return this.ioBufferSize;
    }

    /**
     * @param ioBufferSize the ioBufferSize to set
     */
    public void setIoBufferSize(int ioBufferSize) {
        this.ioBufferSize = ioBufferSize;
    }
    
    /**
     * @return the closeAsync
     */
    public boolean isCloseAsync() {
        return closeAsync;
    }

    /**
     * @param closeAsync the closeAsync to set
     */
    public void setCloseAsync(boolean closeAsync) {
        this.closeAsync = closeAsync;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected String resolveHostName(String host) throws UnknownHostException {
        String localName = InetAddress.getLocalHost().getHostName();
        if (localName != null && isUseLocalHost()) {
            if (localName.equals(host)) {
                return "localhost";
            }
        }
        return host;
    }

    /**
     * Configures the socket for use
     * 
     * @param sock
     * @throws SocketException, IllegalArgumentException if setting the options
     *         on the socket failed.
     */
    protected void initialiseSocket(Socket sock) throws SocketException,
            IllegalArgumentException {
        if (socketOptions != null) {
            IntrospectionSupport.setProperties(socket, socketOptions);
        }

        try {
            sock.setReceiveBufferSize(socketBufferSize);
            sock.setSendBufferSize(socketBufferSize);
        } catch (SocketException se) {
            LOG.warn("Cannot set socket buffer size = " + socketBufferSize);
            LOG.debug("Cannot set socket buffer size. Reason: " + se, se);
        }
        sock.setSoTimeout(soTimeout);

        if (keepAlive != null) {
            sock.setKeepAlive(keepAlive.booleanValue());
        }
        if (tcpNoDelay != null) {
            sock.setTcpNoDelay(tcpNoDelay.booleanValue());
        }
        if (!this.trafficClassSet) {
            this.trafficClassSet = setTrafficClass(sock);
        }
    }

    @Override
    protected void doStart() throws Exception {
        connect();
        stoppedLatch.set(new CountDownLatch(1));
        super.doStart();
    }

    protected void connect() throws Exception {

        if (socket == null && socketFactory == null) {
            throw new IllegalStateException("Cannot connect if the socket or socketFactory have not been set");
        }

        InetSocketAddress localAddress = null;
        InetSocketAddress remoteAddress = null;

        if (localLocation != null) {
            localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()),
                                                 localLocation.getPort());
        }

        if (remoteLocation != null) {
            String host = resolveHostName(remoteLocation.getHost());
            remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        }
        // Set the traffic class before the socket is connected when possible so
        // that the connection packets are given the correct traffic class.
        this.trafficClassSet = setTrafficClass(socket);

        if (socket != null) {

            if (localAddress != null) {
                socket.bind(localAddress);
            }

            // If it's a server accepted socket.. we don't need to connect it
            // to a remote address.
            if (remoteAddress != null) {
                if (connectionTimeout >= 0) {
                    socket.connect(remoteAddress, connectionTimeout);
                } else {
                    socket.connect(remoteAddress);
                }
            }

        } else {
            // For SSL sockets.. you can't create an unconnected socket :(
            // This means the timout option are not supported either.
            if (localAddress != null) {
                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort(),
                                                    localAddress.getAddress(), localAddress.getPort());
            } else {
                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort());
            }
        }

        initialiseSocket(socket);
        initializeStreams();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping transport " + this);
        }

        // Closing the streams flush the sockets before closing.. if the socket
        // is hung.. then this hangs the close.
        // closeStreams();
        if (socket != null) {
            if (closeAsync) {
                //closing the socket can hang also 
                final CountDownLatch latch = new CountDownLatch(1);
                
                SOCKET_CLOSE.execute(new Runnable() {
    
                    public void run() {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            LOG.debug("Caught exception closing socket",e);
                        }finally {
                            latch.countDown();
                        }
                    }
                    
                });
                latch.await(1,TimeUnit.SECONDS);
            }else {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.debug("Caught exception closing socket",e);
                }
            }
           
        }
    }

    /**
     * Override so that stop() blocks until the run thread is no longer running.
     */
    @Override
    public void stop() throws Exception {
        super.stop();
        CountDownLatch countDownLatch = stoppedLatch.get();
        if (countDownLatch != null && Thread.currentThread() != this.runnerThread) {
            countDownLatch.await(1,TimeUnit.SECONDS);
        }
    }

    protected void initializeStreams() throws Exception {
        TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), ioBufferSize) {
            @Override
            public int read() throws IOException {
                receiveCounter++;
                return super.read();
            }
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                receiveCounter++;
                return super.read(b, off, len);
            }
            @Override
            public long skip(long n) throws IOException {
                receiveCounter++;
                return super.skip(n);
            }
            @Override
            protected void fill() throws IOException {
                receiveCounter++;
                super.fill();
            }
        };
        this.dataIn = new DataInputStream(buffIn);
        buffOut = new TcpBufferedOutputStream(socket.getOutputStream(), ioBufferSize);
        this.dataOut = new DataOutputStream(buffOut);
    }

    protected void closeStreams() throws IOException {
        if (dataOut != null) {
            dataOut.close();
        }
        if (dataIn != null) {
            dataIn.close();
        }
    }

    public void setSocketOptions(Map<String, Object> socketOptions) {
        this.socketOptions = new HashMap<String, Object>(socketOptions);
    }

    public String getRemoteAddress() {
        if (socket != null) {
            return "" + socket.getRemoteSocketAddress();
        }
        return null;
    }
    
    @Override
    public <T> T narrow(Class<T> target) {
        if (target == Socket.class) {
            return target.cast(socket);
        } else if ( target == TcpBufferedOutputStream.class) {
            return target.cast(buffOut);
        }
        return super.narrow(target);
    }
    

    static {
        SOCKET_CLOSE =   new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "TcpSocketClose: "+runnable);
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setDaemon(true);
                return thread;
            }
        });
    }


    public int getReceiveCounter() {
        return receiveCounter;
    }
    

    /**
     * @param sock The socket on which to set the Traffic Class.
     * @return Whether or not the Traffic Class was set on the given socket.
     * @throws SocketException if the system does not support setting the
     *         Traffic Class.
     * @throws IllegalArgumentException if both the Differentiated Services and
     *         Type of Services transport options have been set on the same
     *         connection.
     */
    private boolean setTrafficClass(Socket sock) throws SocketException,
            IllegalArgumentException {
        if (sock == null
            || (!this.diffServChosen && !this.typeOfServiceChosen)) {
            return false;
        }
        if (this.diffServChosen && this.typeOfServiceChosen) {
            throw new IllegalArgumentException("Cannot set both the "
                + " Differentiated Services and Type of Services transport "
                + " options on the same connection.");
        }
        sock.setTrafficClass(this.trafficClass);
        // Reset the guards that prevent both the Differentiated Services
        // option and the Type of Service option from being set on the same
        // connection.
        this.diffServChosen = false;
        this.typeOfServiceChosen = false;
        return true;
    }
}
