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
package org.apache.activemq.network;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class NetworkConnectionsTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkConnectionsTest.class);

    private static final String LOCAL_BROKER_TRANSPORT_URI = "tcp://localhost:61616";
    private static final String REMOTE_BROKER_TRANSPORT_URI = "tcp://localhost:61617";
    private static final String DESTINATION_NAME = "TEST.RECONNECT";

    private BrokerService localBroker;
    private BrokerService remoteBroker;

    @Test
    public void testIsStarted() throws Exception {
        LOG.info("testIsStarted is starting...");

        LOG.info("Adding network connector...");
        NetworkConnector nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName("NC1");

        LOG.info("Starting network connector...");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Stopping network connector...");
        nc.stop();

        while (nc.isStopping()) {
            LOG.info("... still stopping ...");
            Thread.sleep(100);
        }

        assertTrue(nc.isStopped());
        assertFalse(nc.isStarted());

        LOG.info("Starting network connector...");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Stopping network connector...");
        nc.stop();

        while (nc.isStopping()) {
            LOG.info("... still stopping ...");
            Thread.sleep(100);
        }

        assertTrue(nc.isStopped());
        assertFalse(nc.isStarted());
    }

    @Test
    public void testNetworkConnectionRestart() throws Exception {
        LOG.info("testNetworkConnectionRestart is starting...");

        LOG.info("Adding network connector...");
        NetworkConnector nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName("NC1");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Setting up Message Producer and Consumer");
        ActiveMQQueue destination = new ActiveMQQueue(DESTINATION_NAME);

        ActiveMQConnectionFactory localFactory = new ActiveMQConnectionFactory(LOCAL_BROKER_TRANSPORT_URI);
        Connection localConnection = localFactory.createConnection();
        localConnection.start();
        Session localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer localProducer = localSession.createProducer(destination);

        ActiveMQConnectionFactory remoteFactory = new ActiveMQConnectionFactory(REMOTE_BROKER_TRANSPORT_URI);
        Connection remoteConnection = remoteFactory.createConnection();
        remoteConnection.start();
        Session remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer remoteConsumer = remoteSession.createConsumer(destination);

        Message message = localSession.createTextMessage("test");
        localProducer.send(message);

        LOG.info("Testing initial network connection...");
        message = remoteConsumer.receive(10000);
        assertNotNull(message);

        LOG.info("Stopping network connection...");
        nc.stop();
        assertFalse(nc.isStarted());

        LOG.info("Sending 2nd message...");
        message = localSession.createTextMessage("test stop");
        localProducer.send(message);

        message = remoteConsumer.receive(1000);
        assertNull("Message should not have been delivered since NetworkConnector was stopped", message);

        LOG.info("(Re)starting network connection...");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Wait for 2nd message to get forwarded and received...");
        message = remoteConsumer.receive(10000);
        assertNotNull("Should have received 2nd message", message);
    }

    @Test
    public void testNetworkConnectionReAddURI() throws Exception {
        LOG.info("testNetworkConnectionReAddURI is starting...");

        LOG.info("Adding network connector 'NC1'...");
        NetworkConnector nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName("NC1");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Looking up network connector by name...");
        NetworkConnector nc1 = localBroker.getNetworkConnectorByName("NC1");
        assertNotNull("Should find network connector 'NC1'", nc1);
        assertTrue(nc1.isStarted());
        assertEquals(nc, nc1);

        LOG.info("Setting up producer and consumer...");
        ActiveMQQueue destination = new ActiveMQQueue(DESTINATION_NAME);

        ActiveMQConnectionFactory localFactory = new ActiveMQConnectionFactory(LOCAL_BROKER_TRANSPORT_URI);
        Connection localConnection = localFactory.createConnection();
        localConnection.start();
        Session localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer localProducer = localSession.createProducer(destination);

        ActiveMQConnectionFactory remoteFactory = new ActiveMQConnectionFactory(REMOTE_BROKER_TRANSPORT_URI);
        Connection remoteConnection = remoteFactory.createConnection();
        remoteConnection.start();
        Session remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer remoteConsumer = remoteSession.createConsumer(destination);

        Message message = localSession.createTextMessage("test");
        localProducer.send(message);

        LOG.info("Testing initial network connection...");
        message = remoteConsumer.receive(10000);
        assertNotNull(message);

        LOG.info("Stopping network connector 'NC1'...");
        nc.stop();
        assertFalse(nc.isStarted());

        LOG.info("Removing network connector...");
        assertTrue(localBroker.removeNetworkConnector(nc));

        nc1 = localBroker.getNetworkConnectorByName("NC1");
        assertNull("Should not find network connector 'NC1'", nc1);

        LOG.info("Re-adding network connector 'NC2'...");
        nc = localBroker.addNetworkConnector("static:(" + REMOTE_BROKER_TRANSPORT_URI + ")");
        nc.setName("NC2");
        nc.start();
        assertTrue(nc.isStarted());

        LOG.info("Looking up network connector by name...");
        NetworkConnector nc2 = localBroker.getNetworkConnectorByName("NC2");
        assertNotNull(nc2);
        assertTrue(nc2.isStarted());
        assertEquals(nc, nc2);

        LOG.info("Testing re-added network connection...");
        message = localSession.createTextMessage("test");
        localProducer.send(message);

        message = remoteConsumer.receive(10000);
        assertNotNull(message);

        LOG.info("Stopping network connector...");
        nc.stop();
        assertFalse(nc.isStarted());

        LOG.info("Removing network connection 'NC2'");
        assertTrue(localBroker.removeNetworkConnector(nc));

        nc2 = localBroker.getNetworkConnectorByName("NC2");
        assertNull("Should not find network connector 'NC2'", nc2);
    }

    @Override
    protected void setUp() throws Exception {
        LOG.info("Setting up LocalBroker");
        localBroker = new BrokerService();
        localBroker.setBrokerName("LocalBroker");
        localBroker.setUseJmx(false);
        localBroker.setPersistent(false);
        localBroker.setTransportConnectorURIs(new String[]{LOCAL_BROKER_TRANSPORT_URI});
        localBroker.start();
        localBroker.waitUntilStarted();

        LOG.info("Setting up RemoteBroker");
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("RemoteBroker");
        remoteBroker.setUseJmx(false);
        remoteBroker.setPersistent(false);
        remoteBroker.setTransportConnectorURIs(new String[]{REMOTE_BROKER_TRANSPORT_URI});
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
    }

    @Override
    protected void tearDown() throws Exception {
        if (localBroker.isStarted()) {
            LOG.info("Stopping LocalBroker");
            localBroker.stop();
            localBroker.waitUntilStopped();
            localBroker = null;
        }

        if (remoteBroker.isStarted()) {
            LOG.info("Stopping RemoteBroker");
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
            remoteBroker = null;
        }
    }
}