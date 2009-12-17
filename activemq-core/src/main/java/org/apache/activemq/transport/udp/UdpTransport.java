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
package org.apache.activemq.transport.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportThreadSupport;
import org.apache.activemq.transport.reliable.ExceptionIfDroppedReplayStrategy;
import org.apache.activemq.transport.reliable.ReplayBuffer;
import org.apache.activemq.transport.reliable.ReplayStrategy;
import org.apache.activemq.transport.reliable.Replayer;
import org.apache.activemq.util.IntSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of the {@link Transport} interface using raw UDP
 * 
 * @version $Revision$
 */
public class UdpTransport extends TransportThreadSupport implements Transport, Service, Runnable {
    private static final Log LOG = LogFactory.getLog(UdpTransport.class);

    private static final int MAX_BIND_ATTEMPTS = 50;
    private static final long BIND_ATTEMPT_DELAY = 100;

    private CommandChannel commandChannel;
    private OpenWireFormat wireFormat;
    private ByteBufferPool bufferPool;
    private ReplayStrategy replayStrategy = new ExceptionIfDroppedReplayStrategy();
    private ReplayBuffer replayBuffer;
    private int datagramSize = 4 * 1024;
    private SocketAddress targetAddress;
    private SocketAddress originalTargetAddress;
    private DatagramChannel channel;
    private boolean trace;
    private boolean useLocalHost = true;
    private int port;
    private int minmumWireFormatVersion;
    private String description;
    private IntSequenceGenerator sequenceGenerator;
    private boolean replayEnabled = true;

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

    /**
     * Creates a replayer for working with the reliable transport
     */
    public Replayer createReplayer() throws IOException {
        if (replayEnabled) {
            return getCommandChannel();
        }
        return null;
    }

    /**
     * A one way asynchronous send
     */
    public void oneway(Object command) throws IOException {
        oneway(command, targetAddress);
    }

    /**
     * A one way asynchronous send to a given address
     */
    public void oneway(Object command, SocketAddress address) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending oneway from: " + this + " to target: " + targetAddress + " command: " + command);
        }
        checkStarted();
        commandChannel.write((Command)command, address);
    }

    /**
     * @return pretty print of 'this'
     */
    public String toString() {
        if (description != null) {
            return description + port;
        } else {
            return getProtocolUriScheme() + targetAddress + "@" + port;
        }
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        LOG.trace("Consumer thread starting for: " + toString());
        while (!isStopped()) {
            try {
                Command command = commandChannel.read();
                doConsume(command);
            } catch (AsynchronousCloseException e) {
                // DatagramChannel closed
                try {
                    stop();
                } catch (Exception e2) {
                    LOG.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            } catch (SocketException e) {
                // DatagramSocket closed
                LOG.debug("Socket closed: " + e, e);
                try {
                    stop();
                } catch (Exception e2) {
                    LOG.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            } catch (EOFException e) {
                // DataInputStream closed
                LOG.debug("Socket closed: " + e, e);
                try {
                    stop();
                } catch (Exception e2) {
                    LOG.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
            } catch (Exception e) {
                try {
                    stop();
                } catch (Exception e2) {
                    LOG.warn("Caught in: " + this + " while closing: " + e2 + ". Now Closed", e2);
                }
                if (e instanceof IOException) {
                    onException((IOException)e);
                } else {
                    LOG.error("Caught: " + e, e);
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
            DatagramEndpoint endpoint = (DatagramEndpoint)newTarget;
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

    public int getDatagramSize() {
        return datagramSize;
    }

    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
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

    public CommandChannel getCommandChannel() throws IOException {
        if (commandChannel == null) {
            commandChannel = createCommandChannel();
        }
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

    public IntSequenceGenerator getSequenceGenerator() {
        if (sequenceGenerator == null) {
            sequenceGenerator = new IntSequenceGenerator();
        }
        return sequenceGenerator;
    }

    public void setSequenceGenerator(IntSequenceGenerator sequenceGenerator) {
        this.sequenceGenerator = sequenceGenerator;
    }

    public boolean isReplayEnabled() {
        return replayEnabled;
    }

    /**
     * Sets whether or not replay should be enabled when using the reliable
     * transport. i.e. should we maintain a buffer of messages that can be
     * replayed?
     */
    public void setReplayEnabled(boolean replayEnabled) {
        this.replayEnabled = replayEnabled;
    }

    public ByteBufferPool getBufferPool() {
        if (bufferPool == null) {
            bufferPool = new DefaultBufferPool();
        }
        return bufferPool;
    }

    public void setBufferPool(ByteBufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    public ReplayBuffer getReplayBuffer() {
        return replayBuffer;
    }

    public void setReplayBuffer(ReplayBuffer replayBuffer) throws IOException {
        this.replayBuffer = replayBuffer;
        getCommandChannel().setReplayBuffer(replayBuffer);
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
        getCommandChannel().start();

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

        return createCommandDatagramChannel();
    }

    protected CommandChannel createCommandDatagramChannel() {
        return new CommandDatagramChannel(this, getWireFormat(), getDatagramSize(), getTargetAddress(), createDatagramHeaderMarshaller(), getChannel(), getBufferPool());
    }

    protected void bind(DatagramSocket socket, SocketAddress localAddress) throws IOException {
        channel.configureBlocking(true);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Binding to address: " + localAddress);
        }

        //
        // We have noticed that on some platfoms like linux, after you close
        // down
        // a previously bound socket, it can take a little while before we can
        // bind it again.
        // 
        for (int i = 0; i < MAX_BIND_ATTEMPTS; i++) {
            try {
                socket.bind(localAddress);
                return;
            } catch (BindException e) {
                if (i + 1 == MAX_BIND_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(BIND_ATTEMPT_DELAY);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }

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

    protected DatagramChannel getChannel() {
        return channel;
    }

    protected void setChannel(DatagramChannel channel) {
        this.channel = channel;
    }

    public InetSocketAddress getLocalSocketAddress() {
        if (channel == null) {
            return null;
        } else {
            return (InetSocketAddress)channel.socket().getLocalSocketAddress();
        }
    }

    public String getRemoteAddress() {
        if (targetAddress != null) {
            return "" + targetAddress;
        }
        return null;
    }

    public int getReceiveCounter() {
        if (commandChannel == null) {
            return 0;
        }
        return commandChannel.getReceiveCounter();
    }
}
