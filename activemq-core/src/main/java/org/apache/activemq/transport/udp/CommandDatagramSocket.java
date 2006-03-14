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
import edu.emory.mathcs.backport.java.util.concurrent.Future;

import org.activeio.ByteArrayInputStream;
import org.activeio.ByteArrayOutputStream;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.LastPartialCommand;
import org.apache.activemq.command.PartialCommand;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

/**
 * A strategy for reading datagrams and de-fragmenting them together.
 * 
 * @version $Revision$
 */
public class CommandDatagramSocket implements CommandChannel {

    private static final Log log = LogFactory.getLog(CommandDatagramSocket.class);

    private final UdpTransport transport;
    private final String name;
    private DatagramSocket channel;
    private InetAddress targetAddress;
    private int targetPort;
    private OpenWireFormat wireFormat;
    private int datagramSize = 4 * 1024;
    private DatagramHeaderMarshaller headerMarshaller;

    // reading
    private Object readLock = new Object();

    // writing
    private Object writeLock = new Object();

    public CommandDatagramSocket(UdpTransport transport, DatagramSocket channel, OpenWireFormat wireFormat, int datagramSize, InetAddress targetAddress,
            int targetPort, DatagramHeaderMarshaller headerMarshaller) {
        this.transport = transport;
        this.channel = channel;
        this.wireFormat = wireFormat;
        this.datagramSize = datagramSize;
        this.targetAddress = targetAddress;
        this.targetPort = targetPort;
        this.headerMarshaller = headerMarshaller;
        this.name = transport.toString();
    }

    public String toString() {
        return "CommandChannel#" + name;
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public Command read() throws IOException {
        Command answer = null;
        Endpoint from = null;
        synchronized (readLock) {
            while (true) {
                DatagramPacket datagram = createDatagramPacket();
                channel.receive(datagram);

                // TODO could use a DataInput implementation that talks direct
                // to the byte[] to avoid object allocation
                DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(datagram.getData()));

                from = headerMarshaller.createEndpoint(datagram, dataIn);
                answer = (Command) wireFormat.unmarshal(dataIn);
                break;
            }
        }
        if (answer != null) {
            answer.setFrom(from);

            if (log.isDebugEnabled()) {
                log.debug("Channel: " + name + " about to process: " + answer);
            }
        }
        return answer;
    }

    public void write(Command command, SocketAddress address, Map requestMap, Future future) throws IOException {
        InetSocketAddress ia = (InetSocketAddress) address;
        write(command, ia.getAddress(), ia.getPort(), requestMap, future);
    }

    public void write(Command command, InetAddress address, int port, Map requestMap, Future future) throws IOException {
        synchronized (writeLock) {

            command.setCommandId(transport.getNextCommandId());
            ByteArrayOutputStream writeBuffer = createByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(writeBuffer);
            headerMarshaller.writeHeader(command, dataOut);

            int offset = writeBuffer.size();

            wireFormat.marshal(command, dataOut);

            if (remaining(writeBuffer) >= 0) {
                if (command.isResponseRequired()) {
                    requestMap.put(new Integer(command.getCommandId()), future);
                }
                sendWriteBuffer(address, port, writeBuffer);
            }
            else {
                // lets split the command up into chunks
                byte[] data = writeBuffer.toByteArray();
                boolean lastFragment = false;
                for (int fragment = 0, length = data.length; !lastFragment; fragment++) {
                    writeBuffer.reset();
                    headerMarshaller.writeHeader(command, dataOut);

                    int chunkSize = remaining(writeBuffer);

                    // we need to remove the amount of overhead to write the
                    // partial command

                    // lets write the flags in there
                    BooleanStream bs = null;
                    if (wireFormat.isTightEncodingEnabled()) {
                        bs = new BooleanStream();
                        bs.writeBoolean(true); // the partial data byte[] is
                        // never null
                    }

                    // lets remove the header of the partial command
                    // which is the byte for the type and an int for the size of
                    // the byte[]
                    chunkSize -= 1 // the data type
                    + 4 // the command ID
                    + 4; // the size of the partial data

                    // the boolean flags
                    if (bs != null) {
                        chunkSize -= bs.marshalledSize();
                    }
                    else {
                        chunkSize -= 1;
                    }

                    if (!wireFormat.isSizePrefixDisabled()) {
                        // lets write the size of the command buffer
                        dataOut.writeInt(chunkSize);
                        chunkSize -= 4;
                    }

                    lastFragment = offset + chunkSize >= length;
                    if (chunkSize + offset > length) {
                        chunkSize = length - offset;
                    }

                    dataOut.write(PartialCommand.DATA_STRUCTURE_TYPE);

                    if (bs != null) {
                        bs.marshal(dataOut);
                    }

                    int commandId = command.getCommandId();
                    if (fragment > 0) {
                        commandId = transport.getNextCommandId();
                    }
                    dataOut.writeInt(commandId);
                    if (bs == null) {
                        dataOut.write((byte) 1);
                    }

                    // size of byte array
                    dataOut.writeInt(chunkSize);

                    // now the data
                    dataOut.write(data, offset, chunkSize);

                    offset += chunkSize;
                    sendWriteBuffer(address, port, writeBuffer);
                }

                // now lets write the last partial command
                command = new LastPartialCommand(command.isResponseRequired());
                command.setCommandId(transport.getNextCommandId());

                writeBuffer.reset();
                headerMarshaller.writeHeader(command, dataOut);
                wireFormat.marshal(command, dataOut);

                if (command.isResponseRequired()) {
                    requestMap.put(new Integer(command.getCommandId()), future);
                }
                sendWriteBuffer(address, port, writeBuffer);
            }
        }
    }

    // Properties
    // -------------------------------------------------------------------------

    public int getDatagramSize() {
        return datagramSize;
    }

    /**
     * Sets the default size of a datagram on the network.
     */
    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    public DatagramHeaderMarshaller getHeaderMarshaller() {
        return headerMarshaller;
    }

    public void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller) {
        this.headerMarshaller = headerMarshaller;
    }

    
    public SocketAddress getTargetAddress() {
        return new InetSocketAddress(targetAddress, targetPort);
    }

    public void setTargetAddress(SocketAddress address) {
        if (address instanceof InetSocketAddress) {
            InetSocketAddress ia = (InetSocketAddress) address;
            targetAddress = ia.getAddress();
            targetPort = ia.getPort();
        }
        else {
            throw new IllegalArgumentException("Address must be instance of InetSocketAddress");
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void sendWriteBuffer(InetAddress address, int port, ByteArrayOutputStream writeBuffer) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Channel: " + name + " sending datagram to: " + address);
        }
        byte[] data = writeBuffer.toByteArray();
        DatagramPacket packet = new DatagramPacket(data, 0, data.length, address, port);
        channel.send(packet);
    }

    protected DatagramPacket createDatagramPacket() {
        return new DatagramPacket(new byte[datagramSize], datagramSize);
    }

    protected int remaining(ByteArrayOutputStream buffer) {
        return datagramSize - buffer.size();
    }

    protected ByteArrayOutputStream createByteArrayOutputStream() {
        return new ByteArrayOutputStream(datagramSize);
    }
}
