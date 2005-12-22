/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activeio.adapter;

import java.io.IOException;
import java.io.OutputStream;

import org.activeio.packet.ByteArrayPacket;
import org.activeio.packet.BytePacket;
import org.activeio.packet.async.AsyncChannel;

/**
 */
public class AsyncChannelToOutputStream extends OutputStream {

    private final AsyncChannel channel;
    private boolean closed;

    /**
     * @param channel
     */
    public AsyncChannelToOutputStream(AsyncChannel channel) {
        this.channel = channel;
    }

    /**
     * @see java.io.OutputStream#write(int)
     */
    public void write(int b) throws IOException {
        channel.write(new BytePacket((byte) b));
    }

    /**
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    public void write(byte[] b, int off, int len) throws IOException {
        channel.write(new ByteArrayPacket(b, off, len));
    }
    
    /**
     * @see java.io.OutputStream#flush()
     */
    public void flush() throws IOException {
        channel.flush();
    }
            
    /**
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        closed=true;
        super.close();
    }
    
    public boolean isClosed() {
        return closed;
    }    
}
