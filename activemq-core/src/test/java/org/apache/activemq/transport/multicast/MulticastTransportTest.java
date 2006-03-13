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
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.udp.UdpTransport;
import org.apache.activemq.transport.udp.UdpTransportTest;

import java.net.URI;

/**
 *
 * @version $Revision$
 */
public class MulticastTransportTest extends UdpTransportTest {
    
    private String multicastURI = "multicast://224.1.2.3:6255";
    

    protected Transport createProducer() throws Exception {
        System.out.println("Producer using URI: " + multicastURI);
        
        // we are not using the TransportFactory as this assumes that
        // transports talk to a server using a WireFormat Negotiation step
        // rather than talking directly to each other
        
        OpenWireFormat wireFormat = createWireFormat();
        MulticastTransport transport = new MulticastTransport(wireFormat, new URI(multicastURI));
        return new CommandJoiner(transport, wireFormat);
    }

    protected Transport createConsumer() throws Exception {
        OpenWireFormat wireFormat = createWireFormat();
        MulticastTransport transport = new MulticastTransport(wireFormat, new URI(multicastURI));
        return new CommandJoiner(transport, wireFormat);
    }


}
