/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.activeio.command.WireFormat;
import org.activeio.command.WireFormatFactory;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServerThreadSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

/**
 * A TCP based implementation of {@link TransportServer}
 * 
 * @version $Revision: 1.1 $
 */

public class TcpTransportServer extends TransportServerThreadSupport {
    private static final Log log = LogFactory.getLog(TcpTransportServer.class);
    private ServerSocket serverSocket;
    private int backlog = 5000;
    private WireFormatFactory wireFormatFactory = new OpenWireFormatFactory();
    private TcpTransportFactory transportFactory = new TcpTransportFactory();
    private long maxInactivityDuration = 30000;
    private int minmumWireFormatVersion;
    private boolean trace;
    
    /**
     * Constructor
     * 
     * @param location
     * @throws IOException
     * @throws URISyntaxException
     */
    public TcpTransportServer(URI location) throws IOException, URISyntaxException {
        super(location);
        serverSocket = createServerSocket(location);
        serverSocket.setSoTimeout(2000);
        updatePhysicalUri(location);
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
                    }
                    else {
                        HashMap options = new HashMap();
                        options.put("maxInactivityDuration", new Long(maxInactivityDuration));
                        options.put("minmumWireFormatVersion", new Integer(minmumWireFormatVersion));
                        options.put("trace", new Boolean(trace));
                        WireFormat format = wireFormatFactory.createWireFormat();
                        TcpTransport transport = new TcpTransport(format, socket);
                        Transport configuredTransport = transportFactory.configure(transport, format, options);
                        getAcceptListener().onAccept(configuredTransport);
                    }
                }
            }
            catch (SocketTimeoutException ste) {
                // expect this to happen
            }
            catch (Exception e) {
                if (!isStopping()) {
                    onAcceptError(e); 
                } else if (!isStopped()) {
                    log.warn("run()", e);
                    onAcceptError(e);
                }
            }
        }
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "TcpTransportServer@" + getLocation();
    }

    /**
     * In cases where we construct ourselves with a zero port we need to
     * regenerate the URI with the real physical port so that people can connect
     * to us via discovery
     * 
     * @throws UnknownHostException
     */
    protected void updatePhysicalUri(URI bindAddr) throws URISyntaxException, UnknownHostException {
        setLocation(new URI(bindAddr.getScheme(), bindAddr.getUserInfo(), resolveHostName(bindAddr.getHost()), serverSocket.getLocalPort(), bindAddr.getPath(),
                bindAddr.getQuery(), bindAddr.getFragment()));
    }

    /**
     * 
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

    /**
     * Factory method to create a new ServerSocket
     * 
     * @throws UnknownHostException
     * @throws IOException
     */
    protected ServerSocket createServerSocket(URI bind) throws UnknownHostException, IOException {
        ServerSocket answer = null;
        String host = bind.getHost();
        host = (host == null || host.length() == 0) ? "localhost" : host;
        InetAddress addr = InetAddress.getByName(host);
        if (host.trim().equals("localhost") || addr.equals(InetAddress.getLocalHost())) {
            answer = new ServerSocket(bind.getPort(), backlog);
        }
        else {
            answer = new ServerSocket(bind.getPort(), backlog, addr);
        }
        return answer;
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
}
