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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An optimized buffered input stream for Tcp
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class TcpBufferedInputStream extends FilterInputStream {
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    protected byte internalBuffer[];
    protected int count;
    protected int position;

    public TcpBufferedInputStream(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    public TcpBufferedInputStream(InputStream in, int size) {
        super(in);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        internalBuffer = new byte[size];
    }

    private void fill() throws IOException {
        byte[] buffer = internalBuffer;
        count = position = 0;
        int n = in.read(buffer, position, buffer.length - position);
        if (n > 0)
            count = n + position;
    }

    public int read() throws IOException {
        if (position >= count) {
            fill();
            if (position >= count)
                return -1;
        }
        return internalBuffer[position++] & 0xff;
    }

    private int readStream(byte[] b, int off, int len) throws IOException {
        int avail = count - position;
        if (avail <= 0) {
            if (len >= internalBuffer.length) {
                return in.read(b, off, len);
            }
            fill();
            avail = count - position;
            if (avail <= 0)
                return -1;
        }
        int cnt = (avail < len) ? avail : len;
        System.arraycopy(internalBuffer, position, b, off, cnt);
        position += cnt;
        return cnt;
    }

    public int read(byte b[], int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        int n = 0;
        for (;;) {
            int nread = readStream(b, off + n, len - n);
            if (nread <= 0)
                return (n == 0) ? nread : n;
            n += nread;
            if (n >= len)
                return n;
            // if not closed but no bytes available, return
            InputStream input = in;
            if (input != null && input.available() <= 0)
                return n;
        }
    }

    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        long avail = count - position;
        if (avail <= 0) {
            return in.skip(n);
        }
        long skipped = (avail < n) ? avail : n;
        position += skipped;
        return skipped;
    }

    public int available() throws IOException {
        return in.available() + (count - position);
    }

    public boolean markSupported() {
        return false;
    }

    public void close() throws IOException {
        if (in != null)
            in.close();
    }
}
