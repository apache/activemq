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
package org.apache.activeio.command;

import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelListener;

import java.io.EOFException;
import java.io.IOException;

/**
 * @version $Revision: 1.1 $
 */
public class AsyncChannelToAsyncCommandChannel implements AsyncCommandChannel {
    private AsyncChannel channel;
    private WireFormat wireFormat;

    public AsyncChannelToAsyncCommandChannel(AsyncChannel channel, WireFormat wireFormat) {
        this.channel = channel;
        this.wireFormat = wireFormat;
    }

    public void writeCommand(Object command) throws IOException {
        channel.write(wireFormat.marshal(command));
        channel.flush();
    }

    public Object getAdapter(Class target) {
        return channel.getAdapter(target);
    }

    public void dispose() {
        channel.dispose();
    }

    public void start() throws IOException {
        channel.start();
    }

    public void stop() throws IOException {
        channel.stop();
    }

    public void setCommandListener(final CommandListener listener) {
        channel.setAsyncChannelListener(new AsyncChannelListener() {
            public void onPacket(Packet packet) {
            	if( packet == EOSPacket.EOS_PACKET ) {
            		listener.onError(new EOFException("Peer disconnected."));
            		return;
            	}
                try {
                    Object command = wireFormat.unmarshal(packet);
                    listener.onCommand(command);
                }
                catch (IOException e) {
                    listener.onError(e);
                }
            }

            public void onPacketError(IOException error) {
                listener.onError(error);
            }
        });
    }
}
