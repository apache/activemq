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
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.TransportServerThreadSupport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A TCP based implementation of {@link TransportServer}
 * 
 * @version $Revision: 1.1 $
 */

public class TcpTransportServer extends TransportServerThreadSupport {

    private static final Log LOG = LogFactory.getLog(TcpTransportServer.class);
    protected ServerSocket serverSocket;
    protected int backlog = 5000;
    protected WireFormatFactory wireFormatFactory = new OpenWireFormatFactory();
    protected final TcpTransportFactory transportFactory;
    protected long maxInactivityDuration = 30000;
    protected int minmumWireFormatVersion;
    protected boolean trace;
    protected Map<String, Object> transportOptions;
    protected final ServerSocketFactory serverSocketFactory;

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
            if (host.trim().equals("localhost") || addr.equals(InetAddress.getLocalHost())) {
                this.serverSocket = serverSocketFactory.createServerSocket(bind.getPort(), backlog);
            } else {
                this.serverSocket = serverSocketFactory.createServerSocket(bind.getPort(), backlog, addr);
            }
            this.serverSocket.setSoTimeout(2000);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to bind to server socket: " + bind + " due to: " + e, e);
        }
        try {
            setConnectURI(new URI(bind.getScheme(), bind.getUserInfo(), resolveHostName(bind.getHost()), serverSocket.getLocalPort(), bind.getPath(), bind.getQuery(), bind
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
                        HashMap<String, Object> options = new HashMap<String, Object>();
                        options.put("maxInactivityDuration", Long.valueOf(maxInactivityDuration));
                        options.put("minmumWireFormatVersion", Integer.valueOf(minmumWireFormatVersion));
                        options.put("trace", Boolean.valueOf(trace));
                        options.putAll(transportOptions);
                        WireFormat format = wireFormatFactory.createWireFormat();
                        Transport transport = createTransport(socket, format);
                        Transport configuredTransport = transportFactory.serverConfigure(transport, format, options);
                        getAcceptListener().onAccept(configuredTransport);
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
    protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
        return new TcpTransport(format, socket);
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "" + getBindLocation();
    }

    /**
     * @param hostName
     * @return real hostName
     * @throws UnknownHostException
     */
    protected String resolveHostName(String hostName) throws UnknownHostException {
        String result = hostName;
        // hostname can be null for vm:// protocol ...
        if (hostName != null && (hostName.equalsIgnoreCase("localhost") || hostName.equals("127.0.0.1"))) {
            result = InetAddress.getLocalHost().getHostName();
        }
        return result;
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
}
