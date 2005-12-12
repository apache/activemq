/**
*
* Copyright 2004 Hiram Chirino
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package org.activeio.adapter;

import java.io.IOException;
import java.io.InputStream;

import org.activeio.InputStreamChannel;

/**
 * Provides an InputStream for a given InputStreamChannel.
 *  
 * @version $Revision$
 */
public class InputStreamChannelToInputStream extends InputStream {
    
    private final InputStreamChannel channel;
    
    /**
     * @param channel
     */
    public InputStreamChannelToInputStream(final InputStreamChannel channel) {
        this.channel = channel;
    }
    
    public int available() throws IOException {
        return channel.available();
    }

    public synchronized void mark(int arg0) {
        channel.mark(arg0);
    }

    public boolean markSupported() {
        return channel.markSupported();
    }

    public int read(byte[] arg0) throws IOException {
        return channel.read(arg0);
    }

    public synchronized void reset() throws IOException {
        channel.reset();
    }

    public long skip(long arg0) throws IOException {
        return channel.skip(arg0);
    }

    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException {
        return channel.read();
    }

    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        return channel.read(b,off,len);
    }
 
    /**
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        channel.dispose();
        super.close();
    }
}
