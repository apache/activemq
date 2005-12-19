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
package org.activeio.packet.async;

import java.io.IOException;

import org.activeio.packet.Packet;


/**
 * A AsyncChannelFilter can be used as a filter between a {@see org.activeio.AsyncChannel}
 * and it's {@see org.activeio.ChannelConsumer}.  Most {@see org.activeio.AsyncChannel}
 * that are not directly accessing the network will extends the AsyncChannelFilter since they act as a
 * filter between the client and the network.  O 
 * 
 * @version $Revision$
 */
public class FilterAsyncChannel implements AsyncChannel, AsyncChannelListener {

    final protected AsyncChannel next;
    protected AsyncChannelListener channelListener;

    public FilterAsyncChannel(AsyncChannel next) {
        this.next = next;
    }

    /**
     */
    public void setAsyncChannelListener(AsyncChannelListener channelListener) {
        this.channelListener = channelListener;
        if (channelListener == null)
            next.setAsyncChannelListener(null);
        else
            next.setAsyncChannelListener(this);
    }

    public void write(Packet packet) throws IOException {
        next.write(packet);
    }

    public void flush() throws IOException {
        next.flush();
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        next.dispose();
    }

    /**
     * @see org.activeio.Service#start()
     * @throws IOException if the next channel has not been set.
     */
    public void start() throws IOException {
        if( next == null )
            throw new IOException("The next channel has not been set.");
        if( channelListener ==null )
            throw new IOException("The UpPacketListener has not been set.");
        next.start();
    }

    /**
     * @see org.activeio.Service#stop()
     */
    public void stop() throws IOException {
        next.stop();
    }

    /**
     * @see org.activeio.packet.async.AsyncChannelListener#onPacket(org.activeio.packet.Packet)
     */
    public void onPacket(Packet packet) {
        channelListener.onPacket(packet);
    }

    /**
     * @see org.activeio.packet.async.AsyncChannelListener#onPacketError(org.activeio.ChannelException)
     */
    public void onPacketError(IOException error) {
        channelListener.onPacketError(error);
    }

    /**
     * @return Returns the next.
     */
    public AsyncChannel getNext() {
        return next;
    }

    /**
     * @return Returns the packetListener.
     */
    public AsyncChannelListener getAsyncChannelListener() {
        return channelListener;
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.getAdapter(target);
    }  
    
    public String toString() {
        return next.toString();
    }
}