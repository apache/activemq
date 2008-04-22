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
package org.apache.activemq.transport.udp;

import java.net.URI;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.WireformatNegociationTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @version $Revision$
 */
public class UdpTransportUsingServerTest extends UdpTestSupport {
    private static final Log LOG = LogFactory.getLog(UdpTransportUsingServerTest.class);

    protected int consumerPort = 9123;
    protected String producerURI = "udp://localhost:" + consumerPort;
    protected String serverURI = producerURI;

    public void testRequestResponse() throws Exception {
        ConsumerInfo expected = new ConsumerInfo();
        expected.setSelector("Edam");
        expected.setResponseRequired(true);
        LOG.info("About to send: " + expected);
        Response response = (Response) producer.request(expected, 2000);

        LOG.info("Received: " + response);
        assertNotNull("Received a response", response);
        assertTrue("Should not be an exception", !response.isException());
    }

    protected Transport createProducer() throws Exception {
        LOG.info("Producer using URI: " + producerURI);
        URI uri = new URI(producerURI);
        return TransportFactory.connect(uri);
    }

    protected TransportServer createServer() throws Exception {
        return TransportFactory.bind(new URI(serverURI));
    }

    protected Transport createConsumer() throws Exception {
        return null;
    }
}
