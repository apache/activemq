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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompTelnetTest extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompTelnetTest.class);

    private BrokerService broker;

    @Override
    protected void setUp() throws Exception {

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.addConnector("stomp://localhost:0");
        broker.addConnector("stomp+nio://localhost:0");

        broker.start();
        broker.waitUntilStarted();
    }

    public void testCRLF() throws Exception {

        for (TransportConnector connector : broker.getTransportConnectors()) {
            LOG.info("try: " + connector.getConnectUri());

            StompConnection stompConnection = new StompConnection();
            stompConnection.open(createSocket(connector.getConnectUri()));
            String frame = "CONNECT\r\n\r\n" + Stomp.NULL;
            stompConnection.sendFrame(frame);

            frame = stompConnection.receiveFrame();
            LOG.info("response from: " + connector.getConnectUri() + ", " + frame);
            assertTrue(frame.startsWith("CONNECTED"));
            stompConnection.close();
        }
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        return new Socket("127.0.0.1", connectUri.getPort());
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

}
