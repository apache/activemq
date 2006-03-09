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

import org.apache.activemq.Service;
import org.apache.activemq.command.Command;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.udp.replay.DatagramReplayStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
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
    private DatagramReplayStrategy replayStrategy;
    private SocketAddress targetAddress;
    private DatagramHeaderMarshaller headerMarshaller = new DatagramHeaderMarshaller();
    private final boolean checkSequenceNumbers;

    // reading
    private Object readLock = new Object();
    private ByteBuffer readBuffer;
    private DatagramReadBuffer readStack;
    private SocketAddress lastReadDatagramAddress;

    // writing
    private Object writeLock = new Object();
    private ByteBuffer writeBuffer;
    private BooleanStream bs = new BooleanStream();
    private int largeMessageBufferSize = 128 * 1024;
    private DatagramHeader header = new DatagramHeader();

    public CommandChannel(String name, DatagramChannel channel, OpenWireFormat wireFormat, ByteBufferPool bufferPool, int datagramSize,
            DatagramReplayStrategy replayStrategy, SocketAddress targetAddress, boolean checkSequenceNumbers) {
        this.name = name;
        this.channel = channel;
        this.wireFormat = wireFormat;
        this.bufferPool = bufferPool;
        this.datagramSize = datagramSize;
        this.replayStrategy = replayStrategy;
        this.targetAddress = targetAddress;
        this.checkSequenceNumbers = checkSequenceNumbers;
    }

    public String toString() {
        return "CommandChannel#" + name;
    }

    public void start() throws Exception {
        // wireFormat.setPrefixPacketSize(false);
        wireFormat.setCacheEnabled(false);
        wireFormat.setTightEncodingEnabled(true);

        if (checkSequenceNumbers) {
            readStack = new CommandReadBuffer(name, wireFormat, replayStrategy);
        }
        bufferPool.setDefaultSize(datagramSize);
        bufferPool.start();
        readBuffer = bufferPool.borrowBuffer();
        writeBuffer = bufferPool.borrowBuffer();
    }

    public void stop() throws Exception {
        bufferPool.stop();
    }

    public void read(CommandProcessor processor) throws IOException {
        DatagramHeader header = null;
        Command answer = null;
        lastReadDatagramAddress = null;
        synchronized (readLock) {
            readBuffer.clear();
            lastReadDatagramAddress = channel.receive(readBuffer);
            readBuffer.flip();

            header = headerMarshaller.readHeader(readBuffer);
            header.setFromAddress(lastReadDatagramAddress);

            if (log.isDebugEnabled()) {
                log.debug("Received datagram on: " + name + " from: " + lastReadDatagramAddress + " header: " + header);
            }
            int remaining = readBuffer.remaining();
            int size = header.getDataSize();
            /*
             * if (size > remaining) { throw new IOException("Invalid command
             * size: " + size + " when there are only: " + remaining + " byte(s)
             * remaining"); } else if (size < remaining) { log.warn("Extra bytes
             * in buffer. Expecting: " + size + " but has: " + remaining); }
             */
            if (size != remaining) {
                log.warn("Expecting: " + size + " but has: " + remaining);
            }
            if (header.isPartial()) {
                byte[] data = new byte[size];
                readBuffer.get(data);
                header.setPartialData(data);
            }
            else {
                byte[] data = new byte[remaining];
                readBuffer.get(data);

                // TODO use a DataInput implementation that talks direct to the
                // ByteBuffer
                DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(data));
                Command command = (Command) wireFormat.unmarshal(dataIn);
                // Command command = (Command) wireFormat.doUnmarshal(dataIn);
                header.setCommand(command);
            }

            if (readStack != null) {
                answer = readStack.read(header);
            }
            else {
                answer = header.getCommand();
            }
        }
        if (answer != null) {
            if (log.isDebugEnabled()) {
                log.debug("Channel: " + name + " about to process: " + answer);
            }
            processor.process(answer, header);
        }
    }

    /**
     * Called if a packet is received on a different channel from a remote
     * client
     * 
     * @throws IOException
     */
    public Command onDatagramReceived(DatagramHeader header) throws IOException {
        if (readStack != null) {
            return readStack.read(header);
        }
        else {
            return header.getCommand();
        }
    }

    public void write(Command command) throws IOException {
        write(command, targetAddress);
    }

    public void write(Command command, SocketAddress address) throws IOException {
        synchronized (writeLock) {

            ByteArrayOutputStream largeBuffer = new ByteArrayOutputStream(largeMessageBufferSize);
            wireFormat.marshal(command, new DataOutputStream(largeBuffer));
            byte[] data = largeBuffer.toByteArray();
            int size = data.length;

            if (size < datagramSize) {
                header.incrementCounter();
                header.setPartial(false);
                header.setComplete(true);
                header.setDataSize(size);
                writeBuffer.clear();
                headerMarshaller.writeHeader(header, writeBuffer);

                writeBuffer.put(data);

                sendWriteBuffer(address);
            }
            else {
                header.setPartial(true);
                header.setComplete(false);

                // lets split the command up into chunks

                int offset = 0;
                boolean lastFragment = false;
                for (int fragment = 0, length = data.length; !lastFragment; fragment++) {
                    // write the header
                    writeBuffer.clear();
                    int chunkSize = writeBuffer.capacity() - headerMarshaller.getHeaderSize(header);
                    lastFragment = offset + chunkSize >= length;
                    if (chunkSize + offset > length) {
                        chunkSize = length - offset;
                    }
                    header.incrementCounter();
                    header.setDataSize(chunkSize);
                    header.setComplete(lastFragment);
                    headerMarshaller.writeHeader(header, writeBuffer);

                    // now the data
                    writeBuffer.put(data, offset, chunkSize);
                    offset += chunkSize;
                    sendWriteBuffer(address);
                }
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

    public SocketAddress getLastReadDatagramAddress() {
        synchronized (readLock) {
            return lastReadDatagramAddress;
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void sendWriteBuffer(SocketAddress address) throws IOException {
        writeBuffer.flip();

        if (log.isDebugEnabled()) {
            log.debug("Channel: " + name + " sending datagram to: " + address + " header: " + header);
        }
        channel.send(writeBuffer, address);
    }

}
