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
package org.activeio.packet.sync.ssl;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.activeio.packet.sync.socket.SocketSyncChannelFactory;

/**
 * A SslSynchChannelFactory creates {@see org.activeio.net.TcpSynchChannel}
 * and {@see org.activeio.net.TcpSynchChannelServer} objects that use SSL.
 * 
 * @version $Revision$
 */
public class SslSocketSyncChannelFactory extends SocketSyncChannelFactory {

    public SslSocketSyncChannelFactory() {
        super(SSLSocketFactory.getDefault(), SSLServerSocketFactory.getDefault());
    }
}
