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

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * An optimized buffered input stream for Tcp
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class NIOInputStream extends InputStream {

    protected int count;
    protected int position;
    private final ByteBuffer in;

    public NIOInputStream(ByteBuffer in) {
        this.in = in;
    }

    public int read() throws IOException {
        try {
            int rc = in.get() & 0xff;
            return rc;
        } catch (BufferUnderflowException e) {
            return -1;
        }
    }

    public int read(byte b[], int off, int len) throws IOException {
        if (in.hasRemaining()) {
            int rc = Math.min(len, in.remaining());
            in.get(b, off, rc);
            return rc;
        } else {
            return len == 0 ? 0 : -1;
        }
    }

    public long skip(long n) throws IOException {
        int rc = Math.min((int)n, in.remaining());
        in.position(in.position() + rc);
        return rc;
    }

    public int available() throws IOException {
        return in.remaining();
    }

    public boolean markSupported() {
        return false;
    }

    public void close() throws IOException {
    }
}
