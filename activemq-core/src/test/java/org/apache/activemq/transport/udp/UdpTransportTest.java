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

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;

import java.net.URI;

/**
 * 
 * @version $Revision$
 */
public class UdpTransportTest extends UdpTestSupport {

    protected String producerURI = "udp://localhost:8830";
    protected String consumerURI = "udp://localhost:8831?port=8830";

    protected Transport createProducer() throws Exception {
        System.out.println("Producer using URI: " + producerURI);
        return TransportFactory.connect(new URI(producerURI));
    }

    protected Transport createConsumer() throws Exception {
        System.out.println("Consumer using URI: " + consumerURI);
        return TransportFactory.connect(new URI(consumerURI));
    }

}
