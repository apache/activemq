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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;

/**
 * A strategy for reading datagrams and de-fragmenting them together.
 * 
 * @version $Revision$
 */
public class CommandChannel implements Service {

    private static final Log log = LogFactory.getLog(CommandChannel.class);
    
    private ByteChannel channel;
    private OpenWireFormat wireFormat;
    private ByteBufferPool bufferPool;
    private int datagramSize = 4 * 1024;
    private DatagramHeaderMarshaller headerMarshaller = new DatagramHeaderMarshaller();
    
    // reading
    private ByteBuffer readBuffer;
    private DataInputStream dataIn;
    private CommandReadBuffer readStack;
    
    // writing
    private ByteBuffer writeBuffer;
    private BooleanStream bs = new BooleanStream(); 
    private DataOutputStream dataOut;
    private int largeMessageBufferSize = 128 * 1024;
    private DatagramHeader header = new DatagramHeader();


    public CommandChannel(ByteChannel channel, OpenWireFormat wireFormat, ByteBufferPool bufferPool, int datagramSize) {
        this.channel = channel;
        this.wireFormat = wireFormat;
        this.bufferPool = bufferPool;
        this.datagramSize = datagramSize;
    }

    public void start() throws Exception {
        readStack = new CommandReadBuffer(wireFormat);
        bufferPool.setDefaultSize(datagramSize);
        bufferPool.start();
        readBuffer = bufferPool.borrowBuffer();
        writeBuffer = bufferPool.borrowBuffer();
        dataIn = new DataInputStream(Channels.newInputStream(channel));
        dataOut = new DataOutputStream(Channels.newOutputStream(channel));
    }

    public void stop() throws Exception {
        bufferPool.stop();
    }
    
    public synchronized Command read() throws IOException {
        readBuffer.clear();
        int read = channel.read(readBuffer);
        DatagramHeader header = headerMarshaller.readHeader(readBuffer);

        int remaining = readBuffer.remaining();
        int size = header.getDataSize();
        if (size > remaining) {
            throw new IOException("Invalid command size: " + size + " when there are only: " + remaining + " byte(s) remaining");
        }
        else if (size < remaining) {
            log.warn("Extra bytes in buffer. Expecting: " + size + " but has: " + remaining);
        }
        if (header.isPartial()) {
            byte[] data = new byte[size];
            readBuffer.get(data);
            header.setPartialData(data);
        }
        else {
            Command command = (Command) wireFormat.unmarshal(dataIn);
            header.setCommand(command);
        }

        return readStack.read(header);
    }

    public synchronized void write(Command command) throws IOException {
        header.incrementCounter();
        int size = wireFormat.tightMarshalNestedObject1(command, bs);
        if (size < datagramSize ) {
            header.setPartial(false);
            writeBuffer.rewind();
            wireFormat.marshal(command, dataOut);
            dataOut.flush();
            channel.write(writeBuffer);
        }
        else {
            header.setPartial(true);
            header.setComplete(false);
            
            // lets split the command up into chunks
            ByteArrayOutputStream largeBuffer = new ByteArrayOutputStream(largeMessageBufferSize);
            wireFormat.marshal(command, new DataOutputStream(largeBuffer));
            
            byte[] data = largeBuffer.toByteArray();
            int offset = 0;
            boolean lastFragment = false;
            for (int fragment = 0, length = data.length; !lastFragment; fragment++ ) {
                // write the header
                writeBuffer.rewind();
                int chunkSize = writeBuffer.capacity() - headerMarshaller.getHeaderSize(header);
                lastFragment = offset + chunkSize >= length;
                header.setComplete(lastFragment);
                headerMarshaller.writeHeader(header, writeBuffer);

                // now the data
                writeBuffer.put(data, offset, chunkSize);
                offset += chunkSize;
                channel.write(writeBuffer);
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

}
