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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;

import org.apache.activemq.Service;
import org.apache.activemq.ThreadPriorities;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportLoggerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerThreadSupport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceListener;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A TCP based implementation of {@link TransportServer}
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 * @version $Revision: 1.1 $
 */

public class TcpTransportServer extends TransportServerThreadSupport implements ServiceListener{

    private static final Log LOG = LogFactory.getLog(TcpTransportServer.class);
    protected ServerSocket serverSocket;
    protected int backlog = 5000;
    protected WireFormatFactory wireFormatFactory = new OpenWireFormatFactory();
    protected final TcpTransportFactory transportFactory;
    protected long maxInactivityDuration = 30000;
    protected int minmumWireFormatVersion;
    protected boolean useQueueForAccept=true;
       
    /**
     * trace=true -> the Transport stack where this TcpTransport
     * object will be, will have a TransportLogger layer
     * trace=false -> the Transport stack where this TcpTransport
     * object will be, will NOT have a TransportLogger layer, and therefore
     * will never be able to print logging messages.
     * This parameter is most probably set in Connection or TransportConnector URIs.
     */
    protected boolean trace = false;

    protected int soTimeout = 0;
    protected int socketBufferSize = 64 * 1024;
    protected int connectionTimeout =  30000;

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
    protected Map<String, Object> transportOptions;
    protected final ServerSocketFactory serverSocketFactory;
    protected BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>();
    protected Thread socketHandlerThread;
    /**
     * The maximum number of sockets allowed for this server
     */
    protected int maximumConnections = Integer.MAX_VALUE;
    protected int currentTransportCount=0;
  
    public TcpTransportServer(TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
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

            this.serverSocket = serverSocketFactory.createServerSocket(bind.getPort(), backlog, addr);
            configureServerSocket(this.serverSocket);
            
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to bind to server socket: " + bind + " due to: " + e, e);
        }
        try {
            setConnectURI(new URI(bind.getScheme(), bind.getUserInfo(), resolveHostName(serverSocket, addr), serverSocket.getLocalPort(), bind.getPath(), bind.getQuery(), bind
                .getFragment()));
        } catch (URISyntaxException e) {

            // it could be that the host name contains invalid characters such
            // as _ on unix platforms
            // so lets try use the IP address instead
            try {
                setConnectURI(new URI(bind.getScheme(), bind.getUserInfo(), addr.getHostAddress(), serverSocket.getLocalPort(), bind.getPath(), bind.getQuery(), bind.getFragment()));
            } catch (URISyntaxException e2) {
                throw IOExceptionSupport.create(e2);
            }
        }
    }

    private void configureServerSocket(ServerSocket socket) throws SocketException {
        socket.setSoTimeout(2000);
        if (transportOptions != null) {
            IntrospectionSupport.setProperties(socket, transportOptions);
        }
    }

    /**
     * @return Returns the wireFormatFactory.
     */
    public WireFormatFactory getWireFormatFactory() {
        return wireFormatFactory;
    }

    /**
     * @param wireFormatFactory The wireFormatFactory to set.
     */
    public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
        this.wireFormatFactory = wireFormatFactory;
    }

    /**
     * Associates a broker info with the transport server so that the transport
     * can do discovery advertisements of the broker.
     * 
     * @param brokerInfo
     */
    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    public long getMaxInactivityDuration() {
        return maxInactivityDuration;
    }

    public void setMaxInactivityDuration(long maxInactivityDuration) {
        this.maxInactivityDuration = maxInactivityDuration;
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
     * @param backlog the backlog to set
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
     * @param useQueueForAccept the useQueueForAccept to set
     */
    public void setUseQueueForAccept(boolean useQueueForAccept) {
        this.useQueueForAccept = useQueueForAccept;
    }
    

    /**
     * pull Sockets from the ServerSocket
     */
    public void run() {
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
                        }else {
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
     * Allow derived classes to override the Transport implementation that this
     * transport server creates.
     * 
     * @param socket
     * @param format
     * @return
     * @throws IOException
     */
    protected  Transport createTransport(Socket socket, WireFormat format) throws IOException {
        return new TcpTransport(format, socket);
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "" + getBindLocation();
    }

    /**
     * @param socket 
     * @param inetAddress
     * @return real hostName
     * @throws UnknownHostException
     */
    protected String resolveHostName(ServerSocket socket, InetAddress bindAddress) throws UnknownHostException {
        String result = null;
        if (socket.isBound()) {
            if (socket.getInetAddress().isAnyLocalAddress()) {
                // make it more human readable and useful, an alternative to 0.0.0.0
                result = InetAddress.getLocalHost().getHostName();
            } else {
                result = socket.getInetAddress().getCanonicalHostName();
            }
        } else {
            result = bindAddress.getCanonicalHostName();
        }
        return result;
    }
    
    protected void doStart() throws Exception {
        if(useQueueForAccept) {
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        while (!isStopped() && !isStopping()) {
                            Socket sock = socketQueue.poll(1, TimeUnit.SECONDS);
                            if (sock != null) {
                                handleSocket(sock);
                            }
                        }
    
                    } catch (InterruptedException e) {
                        LOG.info("socketQueue interuppted - stopping");
                        if (!isStopping()) {
                            onAcceptError(e);
                        }
                    }
    
                }
    
            };
            socketHandlerThread = new Thread(null, run,
                    "ActiveMQ Transport Server Thread Handler: " + toString(),
                    getStackSize());
            socketHandlerThread.setDaemon(true);
            socketHandlerThread.setPriority(ThreadPriorities.BROKER_MANAGEMENT-1);
            socketHandlerThread.start();
        }
        super.doStart();
        
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        super.doStop(stopper);
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress)serverSocket.getLocalSocketAddress();
    }

    public void setTransportOption(Map<String, Object> transportOptions) {
        this.transportOptions = transportOptions;
    }
    
    protected final void handleSocket(Socket socket) {
        try {
            if (this.currentTransportCount >= this.maximumConnections) {
                
            }else {
            HashMap<String, Object> options = new HashMap<String, Object>();
            options.put("maxInactivityDuration", Long
                    .valueOf(maxInactivityDuration));
            options.put("minmumWireFormatVersion", Integer
                    .valueOf(minmumWireFormatVersion));
            options.put("trace", Boolean.valueOf(trace));
            options.put("soTimeout", Integer.valueOf(soTimeout));
            options.put("socketBufferSize", Integer.valueOf(socketBufferSize));
            options.put("connectionTimeout", Integer.valueOf(connectionTimeout));
            options.put("logWriterName", logWriterName);
            options
                    .put("dynamicManagement", Boolean
                            .valueOf(dynamicManagement));
            options.put("startLogging", Boolean.valueOf(startLogging));

            options.putAll(transportOptions);
            WireFormat format = wireFormatFactory.createWireFormat();
            Transport transport = createTransport(socket, format);
            if (transport instanceof ServiceSupport) {
                ((ServiceSupport) transport).addServiceListener(this);
            }
            Transport configuredTransport = transportFactory.serverConfigure(
                    transport, format, options);
            getAcceptListener().onAccept(configuredTransport);
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
     * @param maximumConnections the maximumConnections to set
     */
    public void setMaximumConnections(int maximumConnections) {
        this.maximumConnections = maximumConnections;
    }

    
    public void started(Service service) {
       this.currentTransportCount++;
    }

    public void stopped(Service service) {
        this.currentTransportCount--;
    }
}