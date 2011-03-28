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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.LastPartialCommand;
import org.apache.activemq.command.PartialCommand;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.reliable.ReplayBuffer;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A strategy for reading datagrams and de-fragmenting them together.
 * 
 * 
 */
public class CommandDatagramSocket extends CommandChannelSupport {

    private static final Logger LOG = LoggerFactory.getLogger(CommandDatagramSocket.class);

    private DatagramSocket channel;
    private Object readLock = new Object();
    private Object writeLock = new Object();

    private volatile int receiveCounter;

    public CommandDatagramSocket(UdpTransport transport, OpenWireFormat wireFormat, int datagramSize, SocketAddress targetAddress, DatagramHeaderMarshaller headerMarshaller,
                                 DatagramSocket channel) {
        super(transport, wireFormat, datagramSize, targetAddress, headerMarshaller);
        this.channel = channel;
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
                receiveCounter++;
                DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(datagram.getData(), 0, datagram.getLength()));
                
                from = headerMarshaller.createEndpoint(datagram, dataIn);
                answer = (Command)wireFormat.unmarshal(dataIn);
                break;
            }
        }
        if (answer != null) {
            answer.setFrom(from);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Channel: " + name + " about to process: " + answer);
            }
        }
        return answer;
    }

    public void write(Command command, SocketAddress address) throws IOException {
        synchronized (writeLock) {

            ByteArrayOutputStream writeBuffer = createByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(writeBuffer);
            headerMarshaller.writeHeader(command, dataOut);

            int offset = writeBuffer.size();

            wireFormat.marshal(command, dataOut);

            if (remaining(writeBuffer) >= 0) {
                sendWriteBuffer(address, writeBuffer, command.getCommandId());
            } else {
                // lets split the command up into chunks
                byte[] data = writeBuffer.toByteArray();
                boolean lastFragment = false;
                int length = data.length;
                for (int fragment = 0; !lastFragment; fragment++) {
                    writeBuffer = createByteArrayOutputStream();
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

                    // data type + the command ID + size of the partial data
                    chunkSize -= 1 + 4 + 4;

                    // the boolean flags
                    if (bs != null) {
                        chunkSize -= bs.marshalledSize();
                    } else {
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

                    if (lastFragment) {
                        dataOut.write(LastPartialCommand.DATA_STRUCTURE_TYPE);
                    } else {
                        dataOut.write(PartialCommand.DATA_STRUCTURE_TYPE);
                    }

                    if (bs != null) {
                        bs.marshal(dataOut);
                    }

                    int commandId = command.getCommandId();
                    if (fragment > 0) {
                        commandId = sequenceGenerator.getNextSequenceId();
                    }
                    dataOut.writeInt(commandId);
                    if (bs == null) {
                        dataOut.write((byte)1);
                    }

                    // size of byte array
                    dataOut.writeInt(chunkSize);

                    // now the data
                    dataOut.write(data, offset, chunkSize);

                    offset += chunkSize;
                    sendWriteBuffer(address, writeBuffer, commandId);
                }
            }
        }
    }

    public int getDatagramSize() {
        return datagramSize;
    }

    public void setDatagramSize(int datagramSize) {
        this.datagramSize = datagramSize;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void sendWriteBuffer(SocketAddress address, ByteArrayOutputStream writeBuffer, int commandId) throws IOException {
        byte[] data = writeBuffer.toByteArray();
        sendWriteBuffer(commandId, address, data, false);
    }

    protected void sendWriteBuffer(int commandId, SocketAddress address, byte[] data, boolean redelivery) throws IOException {
        // lets put the datagram into the replay buffer first to prevent timing
        // issues
        ReplayBuffer bufferCache = getReplayBuffer();
        if (bufferCache != null && !redelivery) {
            bufferCache.addBuffer(commandId, data);
        }

        if (LOG.isDebugEnabled()) {
            String text = redelivery ? "REDELIVERING" : "sending";
            LOG.debug("Channel: " + name + " " + text + " datagram: " + commandId + " to: " + address);
        }
        DatagramPacket packet = new DatagramPacket(data, 0, data.length, address);
        channel.send(packet);
    }

    public void sendBuffer(int commandId, Object buffer) throws IOException {
        if (buffer != null) {
            byte[] data = (byte[])buffer;
            sendWriteBuffer(commandId, replayAddress, data, true);
        } else {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Request for buffer: " + commandId + " is no longer present");
            }
        }
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

    public int getReceiveCounter() {
        return receiveCounter;
    }
}
