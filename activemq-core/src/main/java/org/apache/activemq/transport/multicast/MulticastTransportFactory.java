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

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.activeio.command.WireFormat;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.udp.UdpTransportFactory;

/**
 * A factory of multicast transport classes
 * 
 * @version $Revision$
 */
public class MulticastTransportFactory extends UdpTransportFactory {

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        OpenWireFormat wireFormat = asOpenWireFormat(wf);
        return new MulticastTransport(wireFormat, location);
    }
}
