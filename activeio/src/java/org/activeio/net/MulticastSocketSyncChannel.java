/**
 *
 * Copyright 2005 (C) The original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activeio.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;


/**
 * @version $Revision$
 */
final public class MulticastSocketSyncChannel extends DatagramSocketSyncChannel {

    private final InetAddress groupAddress;


    protected MulticastSocketSyncChannel(MulticastSocket socket, InetAddress groupAddress) throws IOException {
        super(socket);
        this.groupAddress = groupAddress;
    }

    public void start() throws IOException {
        ((MulticastSocket) getSocket()).joinGroup(groupAddress);
    }

    public void stop(long timeout) throws IOException {
        ((MulticastSocket) getSocket()).leaveGroup(groupAddress);
    }

    public String toString() {
        return "MulticastSocket Connection: " + getSocket().getLocalSocketAddress() + " -> " + getSocket().getRemoteSocketAddress();
    }
}