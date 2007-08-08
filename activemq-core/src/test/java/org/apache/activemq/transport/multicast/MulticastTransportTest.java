/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.multicast;

import java.net.URI;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.udp.UdpTransportTest;
import org.apache.activemq.util.IntSequenceGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @version $Revision$
 */
public class MulticastTransportTest extends UdpTransportTest {
    
    protected static final Log log = LogFactory.getLog(MulticastTransportTest.class);

    private String multicastURI = "multicast://224.1.2.3:6255";
    

    protected Transport createProducer() throws Exception {
        log.info("Producer using URI: " + multicastURI);
        
        // we are not using the TransportFactory as this assumes that
        // transports talk to a server using a WireFormat Negotiation step
        // rather than talking directly to each other
        
        OpenWireFormat wireFormat = createWireFormat();
        MulticastTransport transport = new MulticastTransport(wireFormat, new URI(multicastURI));
        transport.setLoopBackMode(false);
        transport.setSequenceGenerator(new IntSequenceGenerator());
        return new CommandJoiner(transport, wireFormat);
    }

    protected Transport createConsumer() throws Exception {
        OpenWireFormat wireFormat = createWireFormat();
        MulticastTransport transport = new MulticastTransport(wireFormat, new URI(multicastURI));
        transport.setLoopBackMode(false);
        transport.setSequenceGenerator(new IntSequenceGenerator());
        return new CommandJoiner(transport, wireFormat);
    }


}
