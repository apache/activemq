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
package org.apache.activemq.transport.tcp;

import org.apache.activemq.Service;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportThreadSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
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

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 *
 * @version $Revision$
 */
public class TcpTransport extends TransportThreadSupport implements Transport, Service, Runnable {
    private static final Log log = LogFactory.getLog(TcpTransport.class);

    protected final URI remoteLocation;
    protected final URI localLocation;
    protected final WireFormat wireFormat;

    protected int connectionTimeout = 30000;
    protected int soTimeout = 0;
    protected int socketBufferSize = 64 * 1024;
    protected int ioBufferSize = 8 * 1024;
    protected Socket socket;
    protected DataOutputStream dataOut;
    protected DataInputStream dataIn;
    protected boolean trace;
    protected boolean useLocalHost = true;
    protected int minmumWireFormatVersion;
    protected SocketFactory socketFactory;

    private Map socketOptions;
    private Boolean keepAlive;
    private Boolean tcpNoDelay;

    /**
     * Connect to a remote Node - e.g. a Broker
     *
     * @param wireFormat
     * @param socketFactory
     * @param remoteLocation
     * @param localLocation  -
     *                       e.g. local InetAddress and local port
     * @throws IOException
     * @throws UnknownHostException
     */
    public TcpTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        this.wireFormat = wireFormat;
        this.socketFactory = socketFactory;
        try {
            this.socket = socketFactory.createSocket();
        }
        catch (SocketException e) {
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
    public String toString() {
        return "tcp://" + socket.getInetAddress() + ":" + socket.getPort();
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        log.trace("TCP consumer thread starting");
        while (!isStopped()) {
            try {
                Object command = readCommand();
                doConsume(command);
            }
            catch (SocketTimeoutException e) {
            }
            catch (InterruptedIOException e) {
            }
            catch (IOException e) {
                try {
                    stop();
                }
                catch (Exception e2) {
                    log.warn("Caught while closing: " + e2 + ". Now Closed", e2);
                }
                onException(e);
            }
        }
    }

    protected Object readCommand() throws IOException {
        return wireFormat.unmarshal(dataIn);
    }

    // Properties
    // -------------------------------------------------------------------------

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
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
    public int getIoBufferSize(){
        return this.ioBufferSize;
    }

    /**
     * @param ioBufferSize the ioBufferSize to set
     */
    public void setIoBufferSize(int ioBufferSize){
        this.ioBufferSize=ioBufferSize;
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
     * @throws SocketException
     */
    protected void initialiseSocket(Socket sock) throws SocketException {
        if (socketOptions != null) {
            IntrospectionSupport.setProperties(socket, socketOptions);
        }

        try {
            sock.setReceiveBufferSize(socketBufferSize);
            sock.setSendBufferSize(socketBufferSize);
        }
        catch (SocketException se) {
            log.warn("Cannot set socket buffer size = " + socketBufferSize);
            log.debug("Cannot set socket buffer size. Reason: " + se, se);
        }
        sock.setSoTimeout(soTimeout);

        if (keepAlive != null) {
            sock.setKeepAlive(keepAlive.booleanValue());
        }
        if (tcpNoDelay != null) {
            sock.setTcpNoDelay(tcpNoDelay.booleanValue());
        }
    }

    protected void doStart() throws Exception {
        connect();
        super.doStart();
    }

    protected void connect() throws Exception {

        if (socket == null && socketFactory == null) {
            throw new IllegalStateException("Cannot connect if the socket or socketFactory have not been set");
        }

        InetSocketAddress localAddress = null;
        InetSocketAddress remoteAddress = null;

        if (localLocation != null) {
            localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort());
        }

        if (remoteLocation != null) {
            String host = resolveHostName(remoteLocation.getHost());
            remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        }

        if (socket != null) {

            if (localAddress != null) {
                socket.bind(localAddress);
            }

            // If it's a server accepted socket.. we don't need to connect it
            // to a remote address.
            if (remoteAddress != null) {
                if (connectionTimeout >= 0) {
                    socket.connect(remoteAddress, connectionTimeout);
                }
                else {
                    socket.connect(remoteAddress);
                }
            }

        }
        else {
            // For SSL sockets.. you can't create an unconnected socket :(
            // This means the timout option are not supported either.
            if (localAddress != null) {
                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort(), localAddress.getAddress(), localAddress.getPort());
            }
            else {
                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort());
            }
        }

        initialiseSocket(socket);
        initializeStreams();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        // Closing the streams flush the sockets before closing.. if the socket
        // is hung.. then this hangs the close.
        // closeStreams();
        if (socket != null) {
            socket.close();
        }
    }

    protected void initializeStreams() throws Exception {
        TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), ioBufferSize);
        this.dataIn = new DataInputStream(buffIn);
        TcpBufferedOutputStream buffOut = new TcpBufferedOutputStream(socket.getOutputStream(), ioBufferSize);
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

    public void setSocketOptions(Map socketOptions) {
        this.socketOptions = new HashMap(socketOptions);
    }

    public String getRemoteAddress() {
        if (socket != null) {
            return "" + socket.getRemoteSocketAddress();
        }
        return null;
    }


    
  
}
