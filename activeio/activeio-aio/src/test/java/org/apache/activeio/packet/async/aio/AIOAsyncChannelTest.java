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
package org.apache.activeio.packet.async.aio;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activeio.Channel;
import org.apache.activeio.ChannelServer;
import org.apache.activeio.packet.async.aio.AIOAsyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelTestSupport;

/**
 * @version $Revision$
 */
public class AIOAsyncChannelTest extends SyncChannelTestSupport {

    static boolean disabled = System.getProperty("disable.aio.tests", "false").equals("true");    
    AIOAsyncChannelFactory factory =  new AIOAsyncChannelFactory();

    protected Channel openChannel(URI connectURI) throws IOException {
        return factory.openAsyncChannel(connectURI);
    }

    protected ChannelServer bindChannel() throws IOException, URISyntaxException {
        return factory.bindAsyncChannel(new URI("tcp://localhost:0"));
    }

    protected boolean isDisabled() {
        return disabled;
    }
}
