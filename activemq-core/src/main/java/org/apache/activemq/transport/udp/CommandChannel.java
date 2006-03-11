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

import org.activeio.ByteArrayInputStream;
import org.activeio.ByteArrayOutputStream;
import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.LastPartialCommand;
import org.apache.activemq.command.PartialCommand;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * A strategy for reading datagrams and de-fragmenting them together.
 * 
 * @version $Revision$
 */
public class CommandChannel implements Service {

    private static final Log log = LogFactory.getLog(CommandChannel.class);

    private final String name;
    private DatagramChannel channel;
    private OpenWireFormat wireFormat;
    private ByteBufferPool bufferPool;
    private int datagramSize = 4 * 1024;
    private SocketAddress targetAddress;
    private DatagramHeaderMarshaller headerMarshaller;

    // reading
    private Object readLock = new Object();
    private ByteBuffer readBuffer;

    // writing
    private Object writeLock = new Object();
    private ByteBuffer writeBuffer;
    private int defaultMarshalBufferSize = 64 * 1024;

    public CommandChannel(String name, DatagramChannel channel, OpenWireFormat wireFormat, ByteBufferPool bufferPool, int datagramSize,
            SocketAddress targetAddress, DatagramHeaderMarshaller headerMarshaller) {
        this.name = name;
        this.channel = channel;
        this.wireFormat = wireFormat;
        this.bufferPool = bufferPool;
        this.datagramSize = datagramSize;
        this.targetAddress = targetAddress;
        this.headerMarshaller = headerMarshaller;
    }

    public String toString() {
        return "CommandChannel#" + name;
    }

    public void start() throws Exception {
        bufferPool.setDefaultSize(datagramSize);
        bufferPool.start();
        readBuffer = bufferPool.borrowBuffer();
        writeBuffer = bufferPool.borrowBuffer();
    }

    public void stop() throws Exception {
        bufferPool.stop();
    }

    public Command read() throws IOException {
        Command answer = null;
        synchronized (readLock) {
            readBuffer.clear();
            SocketAddress address = channel.receive(readBuffer);
            readBuffer.flip();

            Endpoint from = headerMarshaller.createEndpoint(readBuffer, address);

            int remaining = readBuffer.remaining();
            byte[] data = new byte[remaining];
            readBuffer.get(data);

            // TODO could use a DataInput implementation that talks direct to
            // the ByteBuffer to avoid object allocation and unnecessary
            // buffering?
            DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(data));
            answer = (Command) wireFormat.unmarshal(dataIn);
            answer.setFrom(from);
        }
        if (answer != null) {
            if (log.isDebugEnabled()) {
                log.debug("Channel: " + name + " about to process: " + answer);
            }
        }
        return answer;
    }

    public void write(Command command) throws IOException {
        write(command, targetAddress);
    }

    public void write(Command command, SocketAddress address) throws IOException {
        synchronized (writeLock) {

            ByteArrayOutputStream largeBuffer = new ByteArrayOutputStream(defaultMarshalBufferSize);
            wireFormat.marshal(command, new DataOutputStream(largeBuffer));
            byte[] data = largeBuffer.toByteArray();
            int size = data.length;

            if (size >= datagramSize) {
                // lets split the command up into chunks
                int offset = 0;
                boolean lastFragment = false;
                for (int fragment = 0, length = data.length; !lastFragment; fragment++) {
                    // write the header
                    writeBuffer.clear();
                    headerMarshaller.writeHeader(command, writeBuffer);

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
                        writeBuffer.putInt(chunkSize);
                        chunkSize -= 4;
                    }

                    lastFragment = offset + chunkSize >= length;
                    if (chunkSize + offset > length) {
                        chunkSize = length - offset;
                    }

                    writeBuffer.put(PartialCommand.DATA_STRUCTURE_TYPE);

                    if (bs != null) {
                        bs.marshal(writeBuffer);
                    }

                    writeBuffer.putInt(command.getCommandId());
                    if (bs == null) {
                        writeBuffer.put((byte) 1);
                    }

                    // size of byte array
                    writeBuffer.putInt(chunkSize);

                    // now the data
                    writeBuffer.put(data, offset, chunkSize);

                    offset += chunkSize;
                    sendWriteBuffer(address);
                }
                
                // now lets write the last partial command
                command = new LastPartialCommand(command);
                largeBuffer = new ByteArrayOutputStream(defaultMarshalBufferSize);
                wireFormat.marshal(command, new DataOutputStream(largeBuffer));
                data = largeBuffer.toByteArray();
            }
            
            writeBuffer.clear();
            headerMarshaller.writeHeader(command, writeBuffer);

            writeBuffer.put(data);

            sendWriteBuffer(address);
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

    public ByteBufferPool getBufferPool() {
        return bufferPool;
    }

    /**
     * Sets the implementation of the byte buffer pool to use
     */
    public void setBufferPool(ByteBufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    public DatagramHeaderMarshaller getHeaderMarshaller() {
        return headerMarshaller;
    }

    public void setHeaderMarshaller(DatagramHeaderMarshaller headerMarshaller) {
        this.headerMarshaller = headerMarshaller;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void sendWriteBuffer(SocketAddress address) throws IOException {
        writeBuffer.flip();

        if (log.isDebugEnabled()) {
            log.debug("Channel: " + name + " sending datagram to: " + address);
        }
        channel.send(writeBuffer, address);
    }

}
