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

import org.activeio.AsyncChannel;
import org.activeio.AsyncChannelListener;
import org.activeio.Packet;
import org.activeio.RequestChannel;
import org.activeio.RequestListener;
import org.activeio.packet.EOSPacket;


/**
 * Creates a {@see org.activeio.RequestChannel} out of a {@see org.activeio.AsyncChannel}.
 * Does not support sending requests.  It can only be used to handle requests.  
 * 
 * @version $Revision$
 */
public class AsyncChannelToServerRequestChannel implements RequestChannel, AsyncChannelListener {
    
    private final AsyncChannel next;
    private RequestListener requestListener;
    
    public AsyncChannelToServerRequestChannel(AsyncChannel next) throws IOException {
        this.next = next;
        next.setAsyncChannelListener(this);
    }

    public Packet request(Packet request, long timeout) throws IOException {
        throw new IOException("Operation not supported.");
    }

    public void setRequestListener(RequestListener requestListener) throws IOException {
        this.requestListener = requestListener;      
    }

    public RequestListener getRequestListener() {
        return requestListener;
    }

    public Object getAdapter(Class target) {
        return next.getAdapter(target);
    }

    public void dispose() {
        next.dispose();
    }

    public void start() throws IOException {
        next.start();
    }

    public void stop(long timeout) throws IOException {
        next.stop(timeout);
    }

    public void onPacket(Packet packet) {
        if( packet == EOSPacket.EOS_PACKET ) {
            return;
        }
        try {
            Packet response = requestListener.onRequest(packet);
            next.write(response);
            next.flush();
        } catch (IOException e) {
            requestListener.onRquestError(e);
        }
    }

    public void onPacketError(IOException error) {
        requestListener.onRquestError(error);
    }
}
