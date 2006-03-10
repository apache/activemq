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
package org.apache.activemq.transport.multicast;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.udp.UdpTransport;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * A multicast based transport.
 * 
 * @version $Revision$
 */
public class MulticastTransport extends UdpTransport {

    public MulticastTransport(OpenWireFormat wireFormat, int port) throws UnknownHostException, IOException {
        super(wireFormat, port);
    }

    public MulticastTransport(OpenWireFormat wireFormat, SocketAddress socketAddress) throws IOException {
        super(wireFormat, socketAddress);
    }

    public MulticastTransport(OpenWireFormat wireFormat, URI remoteLocation) throws UnknownHostException, IOException {
        super(wireFormat, remoteLocation);
    }

    public MulticastTransport(OpenWireFormat wireFormat) throws IOException {
        super(wireFormat);
    }

    protected String getProtocolName() {
        return "Multicast";
    }

    protected String getProtocolUriScheme() {
        return "multicast://";
    }
}
