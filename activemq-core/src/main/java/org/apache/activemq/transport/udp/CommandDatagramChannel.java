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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

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
public class CommandDatagramChannel extends CommandChannelSupport {

    private static final Logger LOG = LoggerFactory.getLogger(CommandDatagramChannel.class);

    private DatagramChannel channel;
    private ByteBufferPool bufferPool;

    // reading
    private Object readLock = new Object();
    private ByteBuffer readBuffer;

    // writing
    private Object writeLock = new Object();
    private int defaultMarshalBufferSize = 64 * 1024;
    private volatile int receiveCounter;

    public CommandDatagramChannel(UdpTransport transport, OpenWireFormat wireFormat, int datagramSize, SocketAddress targetAddress, DatagramHeaderMarshaller headerMarshaller,
                                  DatagramChannel channel, ByteBufferPool bufferPool) {
        super(transport, wireFormat, datagramSize, targetAddress, headerMarshaller);
        this.channel = channel;
        this.bufferPool = bufferPool;
    }

    public void start() throws Exception {
        bufferPool.setDefaultSize(datagramSize);
        bufferPool.start();
        readBuffer = bufferPool.borrowBuffer();
    }

    public void stop() throws Exception {
        bufferPool.stop();
    }

    public Command read() throws IOException {
        Command answer = null;
        Endpoint from = null;
        synchronized (readLock) {
            while (true) {
                readBuffer.clear();
                SocketAddress address = channel.receive(readBuffer);

                readBuffer.flip();

                if (readBuffer.limit() == 0) {
                    continue;
                }
                
                receiveCounter++;
                from = headerMarshaller.createEndpoint(readBuffer, address);

                int remaining = readBuffer.remaining();
                byte[] data = new byte[remaining];
                readBuffer.get(data);

                // TODO could use a DataInput implementation that talks direct
                // to
                // the ByteBuffer to avoid object allocation and unnecessary
                // buffering?
                DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(data));
                answer = (Command)wireFormat.unmarshal(dataIn);
                break;
            }
        }
        if (answer != null) {
            answer.setFrom(from);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Channel: " + name + " received from: " + from + " about to process: " + answer);
            }
        }
        return answer;
    }

    public void write(Command command, SocketAddress address) throws IOException {
        synchronized (writeLock) {

            ByteArrayOutputStream largeBuffer = new ByteArrayOutputStream(defaultMarshalBufferSize);
            wireFormat.marshal(command, new DataOutputStream(largeBuffer));
            byte[] data = largeBuffer.toByteArray();
            int size = data.length;

            ByteBuffer writeBuffer = bufferPool.borrowBuffer();
            writeBuffer.clear();
            headerMarshaller.writeHeader(command, writeBuffer);

            if (size > writeBuffer.remaining()) {
                // lets split the command up into chunks
                int offset = 0;
                boolean lastFragment = false;
                int length = data.length;
                for (int fragment = 0; !lastFragment; fragment++) {
                    // write the header
                    if (fragment > 0) {
                        writeBuffer = bufferPool.borrowBuffer();
                        writeBuffer.clear();
                        headerMarshaller.writeHeader(command, writeBuffer);
                    }

                    int chunkSize = writeBuffer.remaining();

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
                        writeBuffer.putInt(chunkSize);
                        chunkSize -= 4;
                    }

                    lastFragment = offset + chunkSize >= length;
                    if (chunkSize + offset > length) {
                        chunkSize = length - offset;
                    }

                    if (lastFragment) {
                        writeBuffer.put(LastPartialCommand.DATA_STRUCTURE_TYPE);
                    } else {
                        writeBuffer.put(PartialCommand.DATA_STRUCTURE_TYPE);
                    }

                    if (bs != null) {
                        bs.marshal(writeBuffer);
                    }

                    int commandId = command.getCommandId();
                    if (fragment > 0) {
                        commandId = sequenceGenerator.getNextSequenceId();
                    }
                    writeBuffer.putInt(commandId);
                    if (bs == null) {
                        writeBuffer.put((byte)1);
                    }

                    // size of byte array
                    writeBuffer.putInt(chunkSize);

                    // now the data
                    writeBuffer.put(data, offset, chunkSize);

                    offset += chunkSize;
                    sendWriteBuffer(commandId, address, writeBuffer, false);
                }
            } else {
                writeBuffer.put(data);
                sendWriteBuffer(command.getCommandId(), address, writeBuffer, false);
            }
        }
    }

    // Properties
    // -------------------------------------------------------------------------

    public ByteBufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * Sets the implementation of the byte buffer pool to use
     */
    public void setBufferPool(ByteBufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void sendWriteBuffer(int commandId, SocketAddress address, ByteBuffer writeBuffer, boolean redelivery) throws IOException {
        // lets put the datagram into the replay buffer first to prevent timing
        // issues
        ReplayBuffer bufferCache = getReplayBuffer();
        if (bufferCache != null && !redelivery) {
            bufferCache.addBuffer(commandId, writeBuffer);
        }

        writeBuffer.flip();

        if (LOG.isDebugEnabled()) {
            String text = redelivery ? "REDELIVERING" : "sending";
            LOG.debug("Channel: " + name + " " + text + " datagram: " + commandId + " to: " + address);
        }
        channel.send(writeBuffer, address);
    }

    public void sendBuffer(int commandId, Object buffer) throws IOException {
        if (buffer != null) {
            ByteBuffer writeBuffer = (ByteBuffer)buffer;
            sendWriteBuffer(commandId, getReplayAddress(), writeBuffer, true);
        } else {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Request for buffer: " + commandId + " is no longer present");
            }
        }
    }

    public int getReceiveCounter() {
        return receiveCounter;
    }

}
