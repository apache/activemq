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

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IntSequenceGenerator;

import java.net.URI;

/**
 * 
 * @version $Revision$
 */
public class UdpTransportTest extends UdpTestSupport {

    protected int consumerPort = 9123;
    protected String producerURI = "udp://localhost:" + consumerPort;

    protected Transport createProducer() throws Exception {
        System.out.println("Producer using URI: " + producerURI);
        
        // we are not using the TransportFactory as this assumes that
        // UDP transports talk to a server using a WireFormat Negotiation step
        // rather than talking directly to each other
        
        OpenWireFormat wireFormat = createWireFormat();
        UdpTransport transport = new UdpTransport(wireFormat, new URI(producerURI));
        transport.setSequenceGenerator(new IntSequenceGenerator());
        return new CommandJoiner(transport, wireFormat);
    }

    protected Transport createConsumer() throws Exception {
        System.out.println("Consumer on port: " + consumerPort);
        OpenWireFormat wireFormat = createWireFormat();
        UdpTransport transport = new UdpTransport(wireFormat, consumerPort);
        transport.setSequenceGenerator(new IntSequenceGenerator());
        return new CommandJoiner(transport, wireFormat);
    }

    protected OpenWireFormat createWireFormat() {
        return new OpenWireFormat();
    }

}
