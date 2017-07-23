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
package org.apache.activemq.transport.nio;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import org.apache.activemq.transport.tcp.TimeStampStream;
import sun.security.ssl.SSLEngineImpl;

/**
 * An optimized buffered OutputStream for TCP/IP
 */
public class NIOOutputStream extends OutputStream implements TimeStampStream {

    private static final int BUFFER_SIZE = 8196;

    private final WritableByteChannel out;
    private final byte[] buffer;
    private final ByteBuffer byteBuffer;

    private int count;
    private boolean closed;
    private volatile long writeTimestamp = -1; // concurrent reads of this value

    private DataWrappingEngine engine;

    /**
     * Constructor
     *
     * @param out
     *        the channel to write data to.
     */
    public NIOOutputStream(WritableByteChannel out) {
        this(out, BUFFER_SIZE);
    }

    /**
     * Creates a new buffered output stream to write data to the specified
     * underlying output stream with the specified buffer size.
     *
     * @param out
     *        the underlying output stream.
     * @param size
     *        the buffer size.
     *
     * @throws IllegalArgumentException if size <= 0.
     */
    public NIOOutputStream(WritableByteChannel out, int size) {
        this.out = out;
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buffer = new byte[size];
        byteBuffer = ByteBuffer.wrap(buffer);
    }

    /**
     * write a byte on to the stream
     *
     * @param b
     *        byte to write
     *
     * @throws IOException if an error occurs while writing the data.
     */
    @Override
    public void write(int b) throws IOException {
        checkClosed();
        if (availableBufferToWrite() < 1) {
            flush();
        }
        buffer[count++] = (byte) b;
    }

    /**
     * write a byte array to the stream
     *
     * @param b
     *        the byte buffer
     * @param off
     *        the offset into the buffer
     * @param len
     *        the length of data to write
     *
     * @throws IOException if an error occurs while writing the data.
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        checkClosed();
        if (availableBufferToWrite() < len) {
            flush();
        }
        if (buffer.length >= len) {
            System.arraycopy(b, off, buffer, count, len);
            count += len;
        } else {
            write(ByteBuffer.wrap(b, off, len));
        }
    }

    /**
     * flush the data to the output stream This doesn't call flush on the
     * underlying OutputStream, because TCP/IP is particularly efficient at doing
     * this itself ....
     *
     * @throws IOException if an error occurs while writing the data.
     */
    @Override
    public void flush() throws IOException {
        if (count > 0 && out != null) {
            byteBuffer.position(0);
            byteBuffer.limit(count);
            write(byteBuffer);
            count = 0;
        }
    }

    /**
     * close this stream
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        super.close();
        if (engine != null) {
            engine.closeOutbound();
        }
        closed = true;
    }

    /**
     * Checks that the stream has not been closed
     *
     * @throws IOException
     */
    protected void checkClosed() throws IOException {
        if (closed) {
            throw new EOFException("Cannot write to the stream any more it has already been closed");
        }
    }

    /**
     * @return the amount free space in the buffer
     */
    private int availableBufferToWrite() {
        return buffer.length - count;
    }

    protected void write(ByteBuffer data) throws IOException {
        ByteBuffer plain;
        if (engine != null) {
            plain = ByteBuffer.allocate(engine.getPacketBufferSize());
            plain.clear();
            engine.wrap(data, plain);
            plain.flip();
        } else {
            plain = data;
        }

        int remaining = plain.remaining();
        long delay = 1;
        int lastWriteSize = -1;
        try {
            writeTimestamp = System.currentTimeMillis();
            while (remaining > 0) {

                // We may need to do a little bit of sleeping to avoid a busy
                // loop. Slow down if no data was written out..
                if (lastWriteSize == 0) {
                    try {
                        // Use exponential growth to increase sleep time.
                        Thread.sleep(delay);
                        delay *= 2;
                        if (delay > 1000) {
                            delay = 1000;
                        }
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                } else {
                    delay = 1;
                }

                // Since the write is non-blocking, all the data may not have
                // been written.
                lastWriteSize = out.write(plain);

                // if the data buffer was larger than the packet buffer we might
                // need to wrap more packets until we reach the end of data, but only
                // when plain has no more space since we are non-blocking and a write
                // might not have written anything.
                if (engine != null && data.hasRemaining() && !plain.hasRemaining()) {
                    plain.clear();
                    engine.wrap(data, plain);
                    plain.flip();
                }

                remaining = plain.remaining();
            }
        } finally {
            writeTimestamp = -1;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.transport.tcp.TimeStampStream#isWriting()
     */
    @Override
    public boolean isWriting() {
        return writeTimestamp > 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.activemq.transport.tcp.TimeStampStream#getWriteTimestamp()
     */
    @Override
    public long getWriteTimestamp() {
        return writeTimestamp;
    }

    public void setEngine(DataWrappingEngine engine) {
        this.engine = engine;
    }


    //FIXME MOVE OUT FROM HERE
    public interface DataWrappingEngine<T> {

        void closeOutbound();

        void wrap(ByteBuffer data, ByteBuffer plain) throws IOException;

        T unwrap(ByteBuffer inputBuffer, ByteBuffer plain) throws IOException;

        int getPacketBufferSize();
    }

    public static class SSLDataWrappingEngine implements DataWrappingEngine<SSLEngineResult> {
        private final SSLEngine sslEngine;

        public SSLDataWrappingEngine(SSLEngine sslEngine) {
            this.sslEngine = sslEngine;
        }

        @Override
        public void closeOutbound() {
            sslEngine.closeOutbound();
        }

        @Override
        public void wrap(ByteBuffer data, ByteBuffer plain) throws IOException {
            sslEngine.wrap(data, plain);
        }

        @Override
        public int getPacketBufferSize() {
            return sslEngine.getSession().getPacketBufferSize();
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer inputBuffer, ByteBuffer plain) throws IOException {
            return sslEngine.unwrap(inputBuffer, plain);
        }
    }

    public static class SASLDataWrappingEngine implements DataWrappingEngine<Integer> {
        private SaslServer saslServer;
        private SaslClient saslClient;

        public SASLDataWrappingEngine(SaslServer saslServer) {
            this.saslServer = saslServer;
        }
        public SASLDataWrappingEngine(SaslClient saslClient) {
            this.saslClient = saslClient;
        }

        @Override
        public void wrap(ByteBuffer data, ByteBuffer plain) throws IOException {
            byte[] buff = new byte[data.remaining()];
            data.get(buff);

            if (saslServer != null) {
                buff = saslServer.wrap(buff, 0, buff.length);
            } else {
                buff = saslClient.wrap(buff, 0, buff.length);
            }

            plain.putInt(buff.length);
            plain.put(buff);
        }

        @Override
        public Integer unwrap(ByteBuffer inputBuffer, ByteBuffer plain) throws IOException {
            int tokenLenght = inputBuffer.getInt();
            byte[] buff = new byte[tokenLenght];
            inputBuffer.get(buff);

            if (saslServer != null) {
                buff = saslServer.unwrap(buff, 0, buff.length);
            } else {
                buff = saslClient.unwrap(buff, 0, buff.length);
            }

            plain.put(buff);
            return buff.length;
        }

        @Override
        public void closeOutbound() {
            //no-op
        }

        @Override
        public int getPacketBufferSize() {
            return BUFFER_SIZE;
        }
    }

    public static class SASLAuthenticationEngine implements DataWrappingEngine<Integer> {
        private SaslServer saslServer;
        private SaslClient saslClient;

        public SASLAuthenticationEngine(SaslServer saslServer) {
            this.saslServer = saslServer;
        }
        public SASLAuthenticationEngine(SaslClient saslClient) {
            this.saslClient = saslClient;
        }

        @Override
        public void wrap(ByteBuffer data, ByteBuffer plain) throws IOException {
            plain.putInt(data.remaining());
            plain.put(data);
        }

        @Override
        public Integer unwrap(ByteBuffer inputBuffer, ByteBuffer plain) throws IOException {
            plain.putInt(inputBuffer.remaining());
            plain.put(inputBuffer);

            return inputBuffer.remaining();
        }

        @Override
        public void closeOutbound() {
            //no-op
        }

        @Override
        public int getPacketBufferSize() {
            return BUFFER_SIZE;
        }
    }
}
