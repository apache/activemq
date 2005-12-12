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
package org.activeio;

import org.activeio.packet.ByteArrayPacket;

import java.io.IOException;
import java.net.URISyntaxException;

import junit.framework.TestCase;

/**
 */
abstract public class ChannelRequestTestSupport extends TestCase implements RequestListener {

    private ChannelServer server;

    public void test() throws IOException {
        final RequestChannel channel = createClientRequestChannel();
        try {
            channel.start();
            sendRequest(channel, 1001);
            sendRequest(channel, 1002);
            sendRequest(channel, 1003);
            sendRequest(channel, 1004);
            sendRequest(channel, 1005);
        } finally {
            channel.dispose();
        }
    }
    
    private void sendRequest(final RequestChannel channel, int packetSize) throws IOException {
        Packet request = new ByteArrayPacket(fill(new byte[packetSize],(byte)1));
        Packet response = channel.request(request, 1000 * 30*1000);
        assertNotNull(response);
        assertEquals(packetSize, response.remaining());
    }

    private byte[] fill(byte[] bs, byte b) {
        for (int i = 0; i < bs.length; i++) {
            bs[i] = b;            
        }
        return bs;
    }

    public Packet onRequest(Packet request) {
        return new ByteArrayPacket(fill(new byte[request.remaining()],(byte)2));
    }

    public void onRquestError(IOException error) {
        error.printStackTrace();
    }

    protected void setUp() throws Exception {
        server = createChannelServer(this);
        server.start();
    }
    
    protected void tearDown() throws Exception {
        server.dispose();
    }

    abstract protected RequestChannel createClientRequestChannel() throws IOException;
    abstract protected ChannelServer createChannelServer(final RequestListener requestListener) throws IOException, URISyntaxException;}
