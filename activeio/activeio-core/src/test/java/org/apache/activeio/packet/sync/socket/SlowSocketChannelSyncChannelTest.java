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
package org.apache.activeio.packet.sync.socket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activeio.Channel;
import org.apache.activeio.ChannelServer;
import org.apache.activeio.packet.sync.SlowWriteSyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelTestSupport;
import org.apache.activeio.packet.sync.nio.NIOSyncChannelFactory;

/**
 * @version $Revision$
 */
public class SlowSocketChannelSyncChannelTest extends SyncChannelTestSupport {

    SlowWriteSyncChannelFactory factory = new SlowWriteSyncChannelFactory(new NIOSyncChannelFactory(true), 1, 100);

    protected Channel openChannel(URI connectURI) throws IOException {
        return factory.openSyncChannel(connectURI);
    }

    protected ChannelServer bindChannel() throws IOException, URISyntaxException {
        return factory.bindSyncChannel(new URI("tcp://localhost:0"));
    }

    /**
     * Reduce the number if iterations since we are running slower than normal.
     */
    protected int getTestIterations() {
        return 25;
    }
}
