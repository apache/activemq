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
package org.apache.activemq.transport.udp;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A logical server side transport instance for a remote client which works with
 * the {@link UdpTransportServer}
 * 
 * @version $Revision$
 */
public class UdpTransportServerClient extends TransportSupport {
    private static final Log log = LogFactory.getLog(UdpTransportServerClient.class);

    private UdpTransportServer server;
    private SocketAddress address;
    private List queue = Collections.synchronizedList(new LinkedList());

    public UdpTransportServerClient(UdpTransportServer server, SocketAddress address) {
        this.server = server;
        this.address = address;
    }

    public String toString() {
        return "UdpClient@" + address;
    }

    public void oneway(Command command) throws IOException {
        checkStarted(command);
        server.sendOutboundCommand(command, address);
    }

    protected void doStart() throws Exception {
        for (Iterator iter = queue.iterator(); iter.hasNext();) {
            Command command = (Command) iter.next();
            doConsume(command);
            iter.remove();
        }
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        queue.clear();
    }

}
