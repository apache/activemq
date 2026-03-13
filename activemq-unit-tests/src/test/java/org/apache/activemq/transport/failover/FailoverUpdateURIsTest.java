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
package org.apache.activemq.transport.failover;

import java.io.File;
import java.io.FileOutputStream;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverUpdateURIsTest extends TestCase {

    private static final String QUEUE_NAME = "test.failoverupdateuris";
    private static final Logger LOG = LoggerFactory.getLogger(FailoverUpdateURIsTest.class);

    Connection connection = null;
    BrokerService bs1 = null;
    BrokerService bs2 = null;

    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (bs1 != null) {
            bs1.stop();
        }
        if (bs2 != null) {
            bs2.stop();
        }
    }

    public void testUpdateURIsViaFile() throws Exception {

        final String targetDir = "target/" + getName();
        new File(targetDir).mkdir();
        final File updateFile = new File(targetDir + "/updateURIsFile.txt");
        LOG.info("updateFile:" + updateFile);
        LOG.info("updateFileUri:" + updateFile.toURI());
        LOG.info("updateFileAbsoluteFile:" + updateFile.getAbsoluteFile());
        LOG.info("updateFileAbsoluteFileUri:" + updateFile.getAbsoluteFile().toURI());

        bs1 = createBroker("bs1", "tcp://localhost:0");
        bs1.start();
        bs1.waitUntilStarted();
        final String firstTcpUri = bs1.getTransportConnectors().get(0).getConnectUri().toString();

        FileOutputStream out = new FileOutputStream(updateFile);
        out.write(firstTcpUri.getBytes());
        out.close();

        // no failover uri's to start with, must be read from file...
        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:()?updateURIsURL=file:///" + updateFile.getAbsoluteFile());
        connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue theQueue = session.createQueue(QUEUE_NAME);
        final MessageProducer producer = session.createProducer(theQueue);
        final MessageConsumer consumer = session.createConsumer(theQueue);
        Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);

        bs1.stop();
        bs1.waitUntilStopped();
        bs1 = null;

        bs2 = createBroker("bs2", "tcp://localhost:0");
        bs2.start();
        bs2.waitUntilStarted();
        final String secondTcpUri = bs2.getTransportConnectors().get(0).getConnectUri().toString();

        // add the transport uri for broker number 2
        out = new FileOutputStream(updateFile, true);
        out.write(",".getBytes());
        out.write(secondTcpUri.getBytes());
        out.close();

        producer.send(message);
        msg = consumer.receive(2000);
        assertNotNull(msg);
    }

    private BrokerService createBroker(final String name, final String tcpUri) throws Exception {
        final BrokerService bs = new BrokerService();
        bs.setBrokerName(name);
        bs.setUseJmx(false);
        bs.setPersistent(false);
        bs.addConnector(tcpUri);
        return bs;
    }

    public void testAutoUpdateURIs() throws Exception {

        bs1 = new BrokerService();
        bs1.setUseJmx(false);
        final TransportConnector transportConnector = bs1.addConnector("tcp://localhost:0");
        transportConnector.setUpdateClusterClients(true);
        bs1.start();
        bs1.waitUntilStarted();
        final String firstTcpUri = bs1.getTransportConnectors().get(0).getConnectUri().toString();

        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + firstTcpUri + ")");
        connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue theQueue = session.createQueue(QUEUE_NAME);
        final MessageProducer producer = session.createProducer(theQueue);
        final MessageConsumer consumer = session.createConsumer(theQueue);
        final Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(4000);
        assertNotNull(msg);

        bs2 = createBroker("bs2", "tcp://localhost:0");
        final NetworkConnector networkConnector = bs2.addNetworkConnector("static:(" + firstTcpUri + ")");
        networkConnector.setDuplex(true);
        bs2.start();
        LOG.info("started brokerService 2");
        bs2.waitUntilStarted();

        assertTrue("bs2 bridge started in time", Wait.waitFor(() ->
            !bs2.getNetworkConnectors().get(0).activeBridges().isEmpty(), 10000, 200));

        LOG.info("stopping brokerService 1");
        bs1.stop();
        bs1.waitUntilStopped();
        bs1 = null;

        producer.send(message);
        msg = consumer.receive(4000);
        assertNotNull(msg);
    }
}
