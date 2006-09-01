/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.activeio;

import java.io.EOFException;
import java.io.IOException;

import org.apache.activeio.command.AsyncCommandChannel;
import org.apache.activeio.command.CommandListener;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelListener;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

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
    
    public Packet toPacket(ByteSequence sequence) {
    	return new ByteArrayPacket(new org.apache.activeio.packet.ByteSequence(sequence.data, sequence.offset, sequence.length));
    }
    
    public ByteSequence toByteSequence(Packet packet) {
    	org.apache.activeio.packet.ByteSequence sequence = packet.asByteSequence();
    	return new ByteSequence(sequence.getData(), sequence.getOffset(), sequence.getLength());
    }

    public void writeCommand(Object command) throws IOException {
    	ByteSequence sequence = wireFormat.marshal(command);
        channel.write(toPacket(sequence));
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
                    Object command = wireFormat.unmarshal(toByteSequence(packet));
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
