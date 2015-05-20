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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.TransportConnector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompTelnetTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompTelnetTest.class);

    @Test(timeout = 60000)
    public void testCRLF() throws Exception {

        for (TransportConnector connector : brokerService.getTransportConnectors()) {
            LOG.info("try: " + connector.getConnectUri());
            int port = connector.getConnectUri().getPort();

            StompConnection stompConnection = new StompConnection();
            stompConnection.open(createSocket(port));
            String frame = "CONNECT\r\n\r\n" + Stomp.NULL;
            stompConnection.sendFrame(frame);

            frame = stompConnection.receiveFrame();
            LOG.info("response from: " + connector.getConnectUri() + ", " + frame);
            assertTrue(frame.startsWith("CONNECTED"));
            stompConnection.close();
        }
    }

    @Test(timeout = 60000)
    public void testCRLF11() throws Exception {

        for (TransportConnector connector : brokerService.getTransportConnectors()) {
            LOG.info("try: " + connector.getConnectUri());
            int port = connector.getConnectUri().getPort();

            StompConnection stompConnection = new StompConnection();
            stompConnection.open(createSocket(port));
            String frame = "CONNECT\r\naccept-version:1.1\r\n\r\n" + Stomp.NULL;
            stompConnection.sendFrame(frame);

            frame = stompConnection.receiveFrame();
            LOG.info("response from: " + connector.getConnectUri() + ", " + frame);
            assertTrue(frame.startsWith("CONNECTED"));
            stompConnection.close();
        }
    }

    @Override
    protected BrokerPlugin configureAuthentication() throws Exception {
        return null;
    }

    @Override
    protected BrokerPlugin configureAuthorization() throws Exception {
        return null;
    }

    protected Socket createSocket(int port) throws IOException {
        return new Socket("127.0.0.1", port);
    }

    @Override
    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }
}
