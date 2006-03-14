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
package org.apache.activemq.transport.udp;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.Response;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportThreadSupport;
import org.apache.activemq.transport.reliable.ExceptionIfDroppedReplayStrategy;
import org.apache.activemq.transport.reliable.ReplayStrategy;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;

/**
 * An implementation of the {@link Transport} interface using raw UDP
 * 
 * @version $Revision$
 */
public class UdpTransport extends TransportThreadSupport implements Transport, Service, Runnable {
    private static final Log log = LogFactory.getLog(UdpTransport.class);

    private CommandChannel commandChannel;
    private OpenWireFormat wireFormat;
    private ByteBufferPool bufferPool;
    private ReplayStrategy replayStrategy = new ExceptionIfDroppedReplayStrategy();
    private int datagramSize = 4 * 1024;
    private long maxInactivityDuration = 0; // 30000;
    private SocketAddress targetAddress;
    private SocketAddress originalTargetAddress;
    private DatagramChannel channel;
    private boolean trace = false;
    private boolean useLocalHost = true;
    private boolean checkSequenceNumbers = true;
    private int port;
    private int minmumWireFormatVersion;
    private String description = null;
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();
    private int lastCommandId = 0;

    private Runnable runnable;

    protected UdpTransport(OpenWireFormat wireFormat) throws IOException {
        this.wireFormat = wireFormat;
    }

    public UdpTransport(OpenWireFormat wireFormat, URI remoteLocation) throws UnknownHostException, IOException {
        this(wireFormat);
        this.targetAddress = createAddress(remoteLocation);
        description = remoteLocation.toString() + "@";
    }

    public UdpTransport(OpenWireFormat wireFormat, SocketAddress socketAddress) throws IOException {
        this(wireFormat);
        this.targetAddress = socketAddress;
        this.description = getProtocolName() + "ServerConnection@";
    }

    /**
     * Used by the server transport
     */
    public UdpTransport(OpenWireFormat wireFormat, int port) throws UnknownHostException, IOException {
        this(wireFormat);
        this.port = port;
        this.targetAddress = null;
        this.description = getProtocolName() + "Server@";
    }

    public TransportFilter createFilter(Transport transport) {
        return new TransportFilter(transport) {
            public void onCommand(Command command) {
                boolean debug = log.isDebugEnabled();
                if (command.isResponse()) {
                    Response response = (Response) command;
                    FutureResponse future = (FutureResponse) requestMap.remove(new Integer(response.getCorrelationId()));
                    if (future != null) {
                        future.set(response);
                    }
                    else {
                        if (debug)
                            log.debug("Received unexpected response for command id: " + response.getCorrelationId());
                    }
                }
                else {
                    super.onCommand(command);
                }
            }
        };
    }

    /**
     * A one way asynchronous send
     */
    public void oneway(Command command) throws IOException {
        oneway(command, targetAddress);
    }

    /**
     * A one way asynchronous send
     */
    public void oneway(Command command, FutureResponse future) throws IOException {
        oneway(command, targetAddress, future);
    }

    protected void oneway(Command command, SocketAddress address, FutureResponse future) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Sending oneway from: " + this + " to target: " + targetAddress + " command: " + command);
        }
        checkStarted(command);
        commandChannel.write(command, address, requestMap, future);
    }

    /**
     * A one way asynchronous send to a given address
     */
    public void oneway(Command command, SocketAddress address) throws IOException {
        oneway(command, address, null);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        if (command.getCommandId() == 0) {
            command.setCommandId(getNextCommandId());
        }
        command.setResponseRequired(true);
        FutureResponse future = new FutureResponse();
        oneway(command, future);
        return future;
    }

    public Response request(Command command) throws IOException {
        FutureResponse response = asyncRequest(command);
        return response.getResult();
    }

    public Response request(Command command, int timeout) throws IOException {
        FutureResponse response = asyncRequest(command);
        return response.getResult(timeout);
    }


    public void setStartupRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    /**
     * @return pretty print of 'this'
     */
    public String toString() {
        if (description != null) {
            return description + port;
        }
        else {
            return getProtocolUriScheme() + targetAddress + "@" + port;
        }
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        log.trace("Consumer thread starting for: " + toString());
        if (runnable != null) {
            runnable.run();
        }
        while (!isStopped()) {
            try {
                Command command = commandChannel.read();
                doConsume(command);
            }
            catch (AsynchronousCloseException e) {
                // DatagramChannel closed
                try {
                    stop();
                }
                catch (Exception e2) {
                    log.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            }
            catch (SocketException e) {
                // DatagramSocket closed
                log.debug("Socket closed: " + e, e);
                try {
                    stop();
                }
                catch (Exception e2) {
                    log.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            }
            catch (EOFException e) {
                // DataInputStream closed
                log.debug("Socket closed: " + e, e);
                try {
                    stop();
                }
                catch (Exception e2) {
                    log.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            }
            catch (Exception e) {
                try {
                    stop();
                }
                catch (Exception e2) {
                    log.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
                if (e instanceof IOException) {
                    onException((IOException) e);
                }
                else {
                    log.error("Caught: " + e, e);
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * We have received the WireFormatInfo from the server on the actual channel
     * we should use for all future communication with the server, so lets set
     * the target to be the actual channel that the server has chosen for us to
     * talk on.
     */
    public void setTargetEndpoint(Endpoint newTarget) {
        if (newTarget instanceof DatagramEndpoint) {
            DatagramEndpoint endpoint = (DatagramEndpoint) newTarget;
            SocketAddress address = endpoint.getAddress();
            if (address != null) {
                if (originalTargetAddress == null) {
                    originalTargetAddress = targetAddress;
                }
                targetAddress = address;
                commandChannel.setTargetAddress(address);
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

    public long getMaxInactivityDuration() {
        return maxInactivityDuration;
    }

    public int getDatagramSize() {
        return datagramSize;
    }

    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    /**
     * Sets the maximum inactivity duration
     */
    public void setMaxInactivityDuration(long maxInactivityDuration) {
        this.maxInactivityDuration = maxInactivityDuration;
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

    public CommandChannel getCommandChannel() {
        return commandChannel;
    }

    /**
     * Sets the implementation of the command channel to use.
     */
    public void setCommandChannel(CommandDatagramChannel commandChannel) {
        this.commandChannel = commandChannel;
    }

    public ReplayStrategy getReplayStrategy() {
        return replayStrategy;
    }

    /**
     * Sets the strategy used to replay missed datagrams
     */
    public void setReplayStrategy(ReplayStrategy replayStrategy) {
        this.replayStrategy = replayStrategy;
    }

    public int getPort() {
        return port;
    }

    /**
     * Sets the port to connect on
     */
    public void setPort(int port) {
        this.port = port;
    }

    public int getMinmumWireFormatVersion() {
        return minmumWireFormatVersion;
    }

    public void setMinmumWireFormatVersion(int minmumWireFormatVersion) {
        this.minmumWireFormatVersion = minmumWireFormatVersion;
    }

    public OpenWireFormat getWireFormat() {
        return wireFormat;
    }

    public boolean isCheckSequenceNumbers() {
        return checkSequenceNumbers;
    }

    public void setCheckSequenceNumbers(boolean checkSequenceNumbers) {
        this.checkSequenceNumbers = checkSequenceNumbers;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * Creates an address from the given URI
     */
    protected InetSocketAddress createAddress(URI remoteLocation) throws UnknownHostException, IOException {
        String host = resolveHostName(remoteLocation.getHost());
        return new InetSocketAddress(host, remoteLocation.getPort());
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

    protected void doStart() throws Exception {
        commandChannel = createCommandChannel();
        commandChannel.start();

        super.doStart();
    }

    protected CommandChannel createCommandChannel() throws IOException {
        SocketAddress localAddress = createLocalAddress();
        channel = DatagramChannel.open();

        channel = connect(channel, targetAddress);

        DatagramSocket socket = channel.socket();
        bind(socket, localAddress);
        if (port == 0) {
            port = socket.getLocalPort();
        }

        if (bufferPool == null) {
            bufferPool = new DefaultBufferPool();
        }
        return new CommandDatagramChannel(this, channel, wireFormat, bufferPool, datagramSize, targetAddress, createDatagramHeaderMarshaller());
    }

    protected void bind(DatagramSocket socket, SocketAddress localAddress) throws IOException {
        channel.configureBlocking(true);

        if (log.isDebugEnabled()) {
            log.debug("Binding to address: " + localAddress);
        }
        socket.bind(localAddress);
    }

    protected DatagramChannel connect(DatagramChannel channel, SocketAddress targetAddress2) throws IOException {
        // TODO
        // connect to default target address to avoid security checks each time
        // channel = channel.connect(targetAddress);

        return channel;
    }

    protected SocketAddress createLocalAddress() {
        return new InetSocketAddress(port);
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        if (channel != null) {
            channel.close();
        }
    }

    protected DatagramHeaderMarshaller createDatagramHeaderMarshaller() {
        return new DatagramHeaderMarshaller();
    }

    protected String getProtocolName() {
        return "Udp";
    }

    protected String getProtocolUriScheme() {
        return "udp://";
    }

    protected SocketAddress getTargetAddress() {
        return targetAddress;
    }

    protected synchronized int getNextCommandId() {
        return ++lastCommandId;
    }
}
