/** 
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activeio.filter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.activeio.AcceptListener;
import org.activeio.AsyncChannelServer;
import org.activeio.Channel;
import org.activeio.ChannelRequestTestSupport;
import org.activeio.ChannelServer;
import org.activeio.RequestChannel;
import org.activeio.RequestListener;
import org.activeio.SyncChannelFactory;
import org.activeio.adapter.AsyncChannelToClientRequestChannel;
import org.activeio.adapter.AsyncChannelToServerRequestChannel;
import org.activeio.adapter.SyncToAsyncChannel;
import org.activeio.adapter.SyncToAsyncChannelServer;
import org.activeio.net.SocketMetadata;
import org.activeio.net.SocketSyncChannelFactory;

/**
 */
public class PacketAggregatingChannelFilterTest extends ChannelRequestTestSupport {
    
    private URI serverURI;

    protected ChannelServer createChannelServer(final RequestListener requestListener) throws IOException, URISyntaxException {

        SyncChannelFactory factory = new SocketSyncChannelFactory();
        
        AsyncChannelServer server = new SyncToAsyncChannelServer(factory.bindSyncChannel(new URI("tcp://localhost:0")));
        
        server.setAcceptListener(new AcceptListener() {
            public void onAccept(Channel channel) {
            	
                RequestChannel requestChannel = null;
                try {
                		((SocketMetadata)channel.getAdapter(SocketMetadata.class)).setTcpNoDelay(true);
                    requestChannel = 
                        new AsyncChannelToServerRequestChannel(
                            new PacketAggregatingAsyncChannel(
                                    SyncToAsyncChannel.adapt(channel)));

                    requestChannel.setRequestListener(requestListener);
                    requestChannel.start();

                } catch (IOException e) {
                    if (requestChannel != null)
                        requestChannel.dispose();
                    else
                        channel.dispose();
                }
            }

            public void onAcceptError(IOException error) {
                error.printStackTrace();
            }
        });
        serverURI = server.getConnectURI();
        return server;
    }
    
    /**
     * @return
     * @throws IOException
     */
    protected RequestChannel createClientRequestChannel() throws IOException {
        SyncChannelFactory factory = new SocketSyncChannelFactory();
        PacketAggregatingSyncChannel channel = new PacketAggregatingSyncChannel(factory.openSyncChannel(serverURI));
		((SocketMetadata)channel.getAdapter(SocketMetadata.class)).setTcpNoDelay(true);
        return new AsyncChannelToClientRequestChannel(channel);
    }
    
}
