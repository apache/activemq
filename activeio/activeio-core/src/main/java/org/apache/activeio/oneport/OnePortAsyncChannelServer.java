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
package org.apache.activeio.oneport;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.activeio.AcceptListener;
import org.apache.activeio.Channel;
import org.apache.activeio.adapter.AsyncToSyncChannel;
import org.apache.activeio.adapter.SyncToAsyncChannel;
import org.apache.activeio.packet.AppendedPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.async.AsyncChannel;
import org.apache.activeio.packet.async.AsyncChannelListener;
import org.apache.activeio.packet.async.AsyncChannelServer;
import org.apache.activeio.packet.async.FilterAsyncChannel;
import org.apache.activeio.packet.async.FilterAsyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.filter.PushbackSyncChannel;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * Allows multiple protocols share a single ChannelServer.  All protocols sharing the server 
 * must have a distinct magic number at the beginning of the client's request.
 * 
 * TODO: handle the case where a client opens a connection but sends no data down the stream.  We need
 * to timeout that client.
 * 
 * @version $Revision$
 */
final public class OnePortAsyncChannelServer extends FilterAsyncChannelServer {
    
    /**
     * The OnePortAsyncChannelServer listens for incoming connection
     * from a normal AsyncChannelServer.  This s the listner used 
     * to receive the accepted channels.
     */
    final private class OnePortAcceptListener implements AcceptListener {
        
        public void onAccept(Channel channel) {
            try {
                AsyncChannel asyncChannel = SyncToAsyncChannel.adapt(channel);
                ProtocolInspectingAsyncChannel inspector = new ProtocolInspectingAsyncChannel(asyncChannel);
                inspector.start();                
            } catch (IOException e) {
                onAcceptError(e);
            }                
        }
        
        public void onAcceptError(IOException error) {
            dispose();
        }
    }

    /**
     * This channel filter sniffs the first few bytes of the byte stream 
     * to see if a ProtocolRecognizer recognizes the protocol.  If it does not
     * it just closes the channel, otherwise the associated SubPortAsyncChannelServer
     * is notified that it accepted a channel.
     *
     */
    final private class ProtocolInspectingAsyncChannel extends FilterAsyncChannel {
        private Packet buffer;

        public ProtocolInspectingAsyncChannel(AsyncChannel next) throws IOException {
            super(next);
            setAsyncChannelListener(new AsyncChannelListener() {
                public void onPacket(Packet packet) {
                    if (buffer == null) {
                        buffer = packet;
                    } else {
                        buffer = AppendedPacket.join(buffer, packet);
                    }
                    findMagicNumber();
                }

                public void onPacketError(IOException error) {
                    dispose();
                }
            });
        }

        private void findMagicNumber() {
            for (Iterator iter = recognizerMap.keySet().iterator(); iter.hasNext();) {
                ProtocolRecognizer recognizer = (ProtocolRecognizer) iter.next();
                if (recognizer.recognizes(buffer.duplicate())) {

                    if( UnknownRecognizer.UNKNOWN_RECOGNIZER == recognizer ) {
                        // Dispose the channel.. don't know what to do with it.
                        dispose();
                    }
                    
                    SubPortAsyncChannelServer onePort = (SubPortAsyncChannelServer) recognizerMap.get(recognizer);
                    if( onePort == null ) {
                        // Dispose the channel.. don't know what to do with it.
                        dispose();
                    }

                    // Once the magic number is found:
                    // Stop the channel so that a decision can be taken on what to
                    // do with the
                    // channel. When the channel is restarted, the buffered up
                    // packets wiil get
                    // delivered.
                    try {
                        stop();
                        setAsyncChannelListener(null);
                    } catch (IOException e) {                        
                        getAsyncChannelListener().onPacketError(e);
                    }
                    
                    Channel channel = getNext();
                    channel = AsyncToSyncChannel.adapt(channel);
                    channel = new PushbackSyncChannel((SyncChannel) channel, buffer);
                    channel = SyncToAsyncChannel.adapt(channel);
                    
                    onePort.onAccept(channel);
                    break;
                }
            }
        }
    }    

    /**
     * Clients bind against the OnePortAsyncChannelServer and get 
     * SubPortAsyncChannelServer which can be used to accept connections.
     */
    final private class SubPortAsyncChannelServer implements AsyncChannelServer {
        
        private final ProtocolRecognizer recognizer;
        private AcceptListener acceptListener;
        private boolean started;
        
        /**
         * @param recognizer
         */
        public SubPortAsyncChannelServer(ProtocolRecognizer recognizer) {
            this.recognizer = recognizer;
        }

        public void setAcceptListener(AcceptListener acceptListener) {
            this.acceptListener = acceptListener;
        }
        
        public URI getBindURI() {
            return next.getBindURI();
        }
        
        public URI getConnectURI() {
            return next.getConnectURI();
        }
        
        public void dispose() {
            started = false;
            recognizerMap.remove(recognizer);
        }
        
        public void start() throws IOException {
            started = true;
        }
        public void stop() throws IOException {
            started = false;
        }
        
        void onAccept(Channel channel) {
            if( started && acceptListener!=null ) {
                acceptListener.onAccept(channel);
            } else {
                // Dispose the channel.. don't know what to do with it.
                channel.dispose();
            }
        }
        
        public Object getAdapter(Class target) {
            if( target.isAssignableFrom(getClass()) ) {
                return this;
            }
            return OnePortAsyncChannelServer.this.getAdapter(target);
        }    
        
    }
    
    
    private final ConcurrentHashMap recognizerMap = new ConcurrentHashMap();

    public OnePortAsyncChannelServer(AsyncChannelServer server) throws IOException {
        super(server);
        super.setAcceptListener(new OnePortAcceptListener());
    }
    
    public void setAcceptListener(AcceptListener acceptListener) {
        throw new IllegalAccessError("Not supported");
    }    
    
    public AsyncChannelServer bindAsyncChannel(ProtocolRecognizer recognizer) throws IOException {
        
        if( recognizerMap.contains(recognizer) ) 
            throw new IOException("That recognizer is allredy bound.");
        
        SubPortAsyncChannelServer server = new SubPortAsyncChannelServer(recognizer);
        Object old = recognizerMap.put(recognizer, server);
        return server;
    }
    
    
}
