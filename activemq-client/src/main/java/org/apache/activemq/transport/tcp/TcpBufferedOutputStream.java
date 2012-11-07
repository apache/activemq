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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An optimized buffered outputstream for Tcp
 * 
 * 
 */

public class TcpBufferedOutputStream extends FilterOutputStream implements TimeStampStream {
    private static final int BUFFER_SIZE = 8192;
    private byte[] buffer;
    private int bufferlen;
    private int count;
    private volatile long writeTimestamp = -1;//concurrent reads of this value
    

    /**
     * Constructor
     * 
     * @param out
     */
    public TcpBufferedOutputStream(OutputStream out) {
        this(out, BUFFER_SIZE);
    }

    /**
     * Creates a new buffered output stream to write data to the specified
     * underlying output stream with the specified buffer size.
     * 
     * @param out the underlying output stream.
     * @param size the buffer size.
     * @throws IllegalArgumentException if size <= 0.
     */
    public TcpBufferedOutputStream(OutputStream out, int size) {
        super(out);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buffer = new byte[size];
        bufferlen = size;
    }

    /**
     * write a byte on to the stream
     * 
     * @param b - byte to write
     * @throws IOException
     */
    public void write(int b) throws IOException {
        if ((bufferlen - count) < 1) {
            flush();
        }
        buffer[count++] = (byte)b;
    }

    /**
     * write a byte array to the stream
     * 
     * @param b the byte buffer
     * @param off the offset into the buffer
     * @param len the length of data to write
     * @throws IOException
     */
    public void write(byte b[], int off, int len) throws IOException {
        if (b != null) {
            if ((bufferlen - count) < len) {
                flush();
            }
            if (buffer.length >= len) {
                System.arraycopy(b, off, buffer, count, len);
                count += len;
            } else {
                try {
                    writeTimestamp = System.currentTimeMillis();
                    out.write(b, off, len);
                } finally {
                    writeTimestamp = -1;
                }
            }
        }
    }

    /**
     * flush the data to the output stream This doesn't call flush on the
     * underlying outputstream, because Tcp is particularly efficent at doing
     * this itself ....
     * 
     * @throws IOException
     */
    public void flush() throws IOException {
        if (count > 0 && out != null) {
            try {
                writeTimestamp = System.currentTimeMillis();
                out.write(buffer, 0, count);
            } finally {
            	writeTimestamp = -1;
            }
            count = 0;
        }
    }

    /**
     * close this stream
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        super.close();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.transport.tcp.TimeStampStream#isWriting()
     */
    public boolean isWriting() {
        return writeTimestamp > 0;
    }
    
    /* (non-Javadoc)
     * @see org.apache.activemq.transport.tcp.TimeStampStream#getWriteTimestamp()
     */
    public long getWriteTimestamp() {
    	return writeTimestamp;
    }

}
