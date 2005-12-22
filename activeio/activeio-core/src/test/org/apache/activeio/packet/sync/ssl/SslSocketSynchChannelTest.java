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
package org.apache.activeio.packet.sync.ssl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activeio.Channel;
import org.apache.activeio.ChannelServer;
import org.apache.activeio.packet.sync.SyncChannelTestSupport;
import org.apache.activeio.packet.sync.ssl.SslSocketSyncChannelFactory;

/**
 * @version $Revision$
 */
public class SslSocketSynchChannelTest extends SyncChannelTestSupport {

    static {
        System.setProperty("javax.net.ssl.trustStore", "src/test/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");        
        System.setProperty("javax.net.ssl.keyStore", "src/test/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");        
        //System.setProperty("javax.net.debug", "ssl,handshake,data,trustmanager");        
    }
    
    SslSocketSyncChannelFactory factory = new SslSocketSyncChannelFactory();
    
    protected Channel openChannel(URI connectURI) throws IOException {
        return factory.openSyncChannel(connectURI);
    }

    protected ChannelServer bindChannel() throws IOException, URISyntaxException {
        return factory.bindSyncChannel(new URI("ssl://localhost:0"));
    }
}
