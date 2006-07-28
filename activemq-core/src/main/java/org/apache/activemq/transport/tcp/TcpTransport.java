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
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.activeio.command.WireFormat;
import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportThreadSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 * 
 * @version $Revision$
 */
public class TcpTransport extends TransportThreadSupport implements Transport, Service, Runnable {
    private static final Log log = LogFactory.getLog(TcpTransport.class);

    private int connectionTimeout = 30000;
    private int soTimeout = 0;
    private int socketBufferSize = 128 * 1024;
    private Socket socket;
    private DataOutputStream dataOut;
    private DataInputStream dataIn;
    private WireFormat wireFormat;
    private boolean trace;
    private boolean useLocalHost = true;
    private int minmumWireFormatVersion;
    private InetSocketAddress remoteAddress;
	private InetSocketAddress localAddress;
    
    /**
     * Construct basic helpers
     * 
     * @param wireFormat
     */
    protected TcpTransport(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    /**
     * Connect to a remote Node - e.g. a Broker
     * 
     * @param wireFormat
     * @param socketFactory 
     * @param remoteLocation
     * @param localLocation -
     *            e.g. local InetAddress and local port
     * @throws IOException
     * @throws UnknownHostException
     */
    public TcpTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        this(wireFormat);
        this.socket = createSocket(socketFactory, remoteLocation, localLocation);
    }

    /**
     * Initialize from a server Socket
     * 
     * @param wireFormat
     * @param socket
     * @throws IOException
     */
    public TcpTransport(WireFormat wireFormat, Socket socket) throws IOException {
        this(wireFormat);
        this.socket = socket;
        setDaemon(true);
    }

    /**
     * A one way asynchronous send
     */
    public void oneway(Command command) throws IOException {
        checkStarted(command);
        wireFormat.marshal(command, dataOut);
        dataOut.flush();
    }

    /**
     * @return pretty print of 'this'
     */
    public String toString() {
        return "tcp://"+socket.getInetAddress()+":"+socket.getPort();
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        log.trace("TCP consumer thread starting");
        while (!isStopped()) {
            try {
                Command command = (Command) wireFormat.unmarshal(dataIn);
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
    

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * Factory method to create a new socket
     * 
     * @param remoteLocation
     * @param localLocation ignored if null
     * @return
     * @throws IOException
     * @throws IOException
     * @throws UnknownHostException
     */
    protected Socket createSocket(SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws IOException, UnknownHostException {
    	
        String host = resolveHostName(remoteLocation.getHost());
        remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        
        if( localLocation!=null ) {
        	localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort());
        }
        
        Socket sock = socketFactory.createSocket();        
        return sock;
    }

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
        try {
            sock.setReceiveBufferSize(socketBufferSize);
            sock.setSendBufferSize(socketBufferSize);
        }
        catch (SocketException se) {
            log.warn("Cannot set socket buffer size = " + socketBufferSize);
            log.debug("Cannot set socket buffer size. Reason: " + se, se);
        }
        sock.setSoTimeout(soTimeout);
    }

    protected void doStart() throws Exception {
        initialiseSocket(socket);
        if( localAddress!=null ) {
        	socket.bind(localAddress);
        }
        if (remoteAddress != null) {
            if (connectionTimeout >= 0) {
                socket.connect(remoteAddress, connectionTimeout);
            }
            else {
                socket.connect(remoteAddress);
            }
        }
        initializeStreams();
        super.doStart();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        closeStreams();
        if (socket != null) {
            socket.close();
        }
    }

    protected void initializeStreams() throws IOException {
        TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), 8*1024);
        this.dataIn = new DataInputStream(buffIn);
        TcpBufferedOutputStream buffOut = new TcpBufferedOutputStream(socket.getOutputStream(), 16*1024);
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
        IntrospectionSupport.setProperties(socket, socketOptions);
    }
}
