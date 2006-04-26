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

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

import java.net.URI;

/**
 * 
 * @version $Revision$
 */
public class UdpTransportUsingServerTest extends UdpTestSupport {

    protected int consumerPort = 9123;
    protected String producerURI = "udp://localhost:" + consumerPort;
    protected String serverURI = producerURI;

    public void testRequestResponse() throws Exception {
        ConsumerInfo expected = new ConsumerInfo();
        expected.setSelector("Edam");
        expected.setResponseRequired(true);
        log.info("About to send: " + expected);
        Response response = producer.request(expected, 2000);

        log.info("Received: " + response);
        assertNotNull("Received a response", response);
        assertTrue("Should not be an exception", !response.isException());
    }

    protected Transport createProducer() throws Exception {
        log.info("Producer using URI: " + producerURI);
        URI uri = new URI(producerURI);
        return TransportFactory.connect(uri);
    }

    protected TransportServer createServer() throws Exception {
        return TransportFactory.bind("byBroker", new URI(serverURI));
    }

    protected Transport createConsumer() throws Exception {
        return null;
    }
}
