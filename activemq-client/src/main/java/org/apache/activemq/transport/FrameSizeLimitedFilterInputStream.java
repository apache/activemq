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

package org.apache.activemq.transport;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.apache.activemq.util.MarshallingSupport.ActiveMQUnmarshalEOFException;

/**
 * A filtered style input stream that allows reads up to a given known max frame size
 * before it starts to throw exceptions indicating the reader has exceeded the set
 * limit. This can be used to wrap another stream that contains a protocol frame to
 * be parsed and enforce that decoding of that frame does not cross the boundary set
 * as the max available bytes before error.
 * <p>
 * This is a specialized stream type that may obfuscate the actual state of the underlying
 * stream such as its actual available bytes. The user should be aware of the behavior of
 * this stream when using it to ensure they do not run into unexpected failures. It is possible
 * to configure this stream with a higher available limit than the underlying stream actually
 * has access to but that inconsistency is left as a requirement for the caller to handle.
 */
public class FrameSizeLimitedFilterInputStream extends InputStream {

    private boolean canMark;

    private int maxAvailableBytes;
    private int availableBytes;

    private int markLimit;
    private int markRemaining;

    private InputStream stream;

    /**
     * Create a new uninitialized instance of the filter stream that will fail to
     * read until a stream and a frame size limit is configured.
     */
    public FrameSizeLimitedFilterInputStream() {
        this.maxAvailableBytes = 0;
        this.availableBytes = maxAvailableBytes;
    }

    /**
     * Create a new instance with the given amount of available bytes that should be
     * readable before an exception is thrown indicating that more bytes where requested
     * from the known fixed frame size than is allowed.
     *
     * @param available
     * 		The number of available bytes to allow in a given frame.
     * @param in
     * 		The {@link InputStream} to read from (cannot be null).
     */
    public FrameSizeLimitedFilterInputStream(int available, InputStream in) {
        if (available < 0) {
            throw new IllegalArgumentException("Available bytes needs to be a positive integer but was: " + available);
        }

        this.stream = Objects.requireNonNull(in);
        this.canMark = in.markSupported();
        this.maxAvailableBytes = available;
        this.availableBytes = maxAvailableBytes;
    }

    /**
     * Render the stream unusable until a reset is called that either changes
     * the stream and assigns a new max or simply assigns a new max which
     * assumes that the underlying stream remains readable which is only a
     * subset of stream types such as byte array wrapper variants.
     */
    @Override
    public void close() throws IOException {
        maxAvailableBytes = availableBytes = markLimit = markRemaining = 0;
        canMark = false;
        if (stream != null) {
            stream.close();
        }
    }

    /**
     * Resets the number of available bytes that can be read from the underlying
     * stream. The underlying stream may still throw exceptions if it cannot provide
     * this many bytes. As a result of calling this method any currently set mark
     * is cleared and the stream cannot be reset back to a previously available
     * number of bytes from this point onward.
     * <p>
     * Calling this method on a stream wrapper that has not been initialized will
     * not result in a readable state, the limit remains zero.
     */
    public void resetAvailable() {
        resetAvailable(maxAvailableBytes);
    }

    /**
     * Resets the number of available bytes that can be read from the underlying
     * stream to the new amount. The underlying stream may still throw exceptions
     * if it cannot provide this many bytes. As a result of calling this method
     * any currently set mark is cleared and the stream cannot be reset back to a
     * previously available number of bytes from this point onward.
     *
     * @param available
     * 		The new available number of bytes to allow from this stream wrapper
     */
    public void resetAvailable(int available) {
        resetAvailable(stream, available);
    }

    /**
     * Resets the number of available bytes and assigns a new stream that can be read
     * from the which allows this type to be re-usable across command reads. The underlying
     * stream may still throw exceptions if it cannot provide this many bytes. As a result
     * of calling this method any currently set mark is cleared and the stream cannot be
     * reset back to a previously available number of bytes from this point onward.
     *
     * @param in
     * 		The new input stream to read bytes from (cannot be assigned as null).
     * @param available
     * 		The new available number of bytes to allow from this stream wrapper
     */
   public void resetAvailable(InputStream in, int available) {
       if (available < 0) {
           throw new IllegalArgumentException("Available bytes needs to be a positive integer but was: " + available);
       }

       availableBytes = maxAvailableBytes = available;
       markLimit = markRemaining = 0;
       stream = Objects.requireNonNull(in);
       canMark = stream.markSupported();
   }

    @Override
    public int read() throws IOException {
        Objects.requireNonNull(stream, "The stream wrapper has not been bound to a source input stream");

        validateAvailable(1, availableBytes);

        final int read = stream.read();

        // if -1 then the stream is done
        reduceAvailable(read >= 0 ? 1 : -1);

        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(stream, "The stream wrapper has not been bound to a source input stream");

        // If length is 0, this method is supposed to just return 0 with
        // no bytes being read
        if (len == 0) {
            return 0;
        }

        final int toRead;
        if (availableBytes > 0) {
            // read the smaller of availableBytes or the length
            // this method allows partial reads less than len
            toRead = Math.min(len, availableBytes);
        } else {
            // we have no more remaining but there is data left so trigger error
            toRead = 1;
        }

        validateAvailable(toRead, availableBytes);

        return reduceAvailable(stream.read(b, off, toRead));
    }

    @Override
    public long skip(long amount) throws IOException {
        if (amount < 0) {
            return 0;
        }

        Objects.requireNonNull(stream, "The stream wrapper has not been bound to a source input stream");

        final int safeSkipRange = (int) Math.min(Integer.MAX_VALUE, amount);

        // Max frame size is limited to Integer.MAX_VALUE as we store that value as an integer
        // so don't accept more than that amount which is valid and does allow the caller to
        // skip that full massive frame but will fail on the next stream operation.
        validateAvailable(safeSkipRange, availableBytes);

        return reduceAvailable((int) stream.skip(safeSkipRange));
    }

    @Override
    public int available() throws IOException {
        return availableBytes;
    }

    @Override
    public void mark(int readLimit) {
        if (canMark && readLimit > 0) {
            markLimit = markRemaining = readLimit;
            stream.mark(readLimit);
        }
    }

    @Override
    public void reset() throws IOException {
        if (canMark && markLimit > 0) {
            availableBytes += markLimit - markRemaining;
            markRemaining = markLimit = 0;
            stream.reset();
        }
    }

    @Override
    public boolean markSupported() {
        return canMark;
    }

    private static void validateAvailable(int requested, int available) throws IOException {
        if (requested > available) {
            throw new ActiveMQUnmarshalEOFException(String.format(
                "Cannot read more than the max available %d bytes: requested %d", available, requested));
        }
    }

    private int reduceAvailable(int amount) throws IOException {
        if (amount == -1) {
            return -1; // Underlying says there are no more bytes
        }

        try {
            availableBytes = Math.subtractExact(availableBytes, amount);
        } catch (ArithmeticException e) {
            throw new IOException(e);
        }

        if (markLimit > 0) {
            markRemaining = markRemaining - amount;
            if (markRemaining < 0) {
                markLimit = markRemaining = 0;
            }
        }

        return amount;
    }
}
