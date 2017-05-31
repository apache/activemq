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

package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WSTransportLinkStealingTest {

    private static final Logger LOG = LoggerFactory.getLogger(WSTransportLinkStealingTest.class);

    private BrokerService broker;

    protected BrokerService createBroker(boolean deleteMessages) throws Exception {
        BrokerService broker = BrokerFactory.createBroker(
                new URI("broker:()/localhost?persistent=false&useJmx=false"));

        SpringSslContext context = new SpringSslContext();
        context.setKeyStore("src/test/resources/server.keystore");
        context.setKeyStoreKeyPassword("password");
        context.setTrustStore("src/test/resources/client.keystore");
        context.setTrustStorePassword("password");
        context.afterPropertiesSet();
        broker.setSslContext(context);

        broker.addConnector(getWSConnectorURI()).setName("ws+mqtt");
        broker.setDeleteAllMessagesOnStartup(deleteMessages);
        broker.start();
        broker.waitUntilStarted();

        return broker;
    }

    protected String getWSConnectorURI() {
        return "ws://127.0.0.1:61623?allowLinkStealing=true&websocket.maxTextMessageSize=99999&transport.maxIdleTime=1001";
    }

    protected void stopBroker() {
        try {
            if (broker != null) {
                broker.stop();
                broker.waitUntilStopped();
                broker = null;
            }
        } catch (Exception e) {
            LOG.warn("Error during Broker stop");
        }
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker(true);
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    @Test
    public void testBrokerStart() throws Exception {
        assertTrue(broker.isStarted());

        TransportConnector connector = broker.getTransportConnectorByName("ws+mqtt");
        assertNotNull("Should have an WS transport", connector);
        assertTrue(connector.isAllowLinkStealing());
    }
}
