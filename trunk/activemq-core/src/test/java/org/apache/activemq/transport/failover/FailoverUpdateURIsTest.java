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
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.log4j.Logger;

public class FailoverUpdateURIsTest extends TestCase {

    private static final String QUEUE_NAME = "test.failoverupdateuris";
    private static final Logger LOG = Logger.getLogger(FailoverUpdateURIsTest.class);

    String firstTcpUri = "tcp://localhost:61616";
    String secondTcpUri = "tcp://localhost:61626";
    Connection connection = null;
    BrokerService bs2 = null;

    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (bs2 != null) {
            bs2.stop();
        }
    }

    public void testUpdateURIsViaFile() throws Exception {

        String targetDir = "target/" + getName();
        new File(targetDir).mkdir();
        File updateFile = new File(targetDir + "/updateURIsFile.txt");
        LOG.info(updateFile);
        LOG.info(updateFile.toURI());
        LOG.info(updateFile.getAbsoluteFile());
        LOG.info(updateFile.getAbsoluteFile().toURI());
        FileOutputStream out = new FileOutputStream(updateFile);
        out.write(firstTcpUri.getBytes());
        out.close();

        BrokerService bs1 = createBroker("bs1", firstTcpUri);
        bs1.start();

        // no failover uri's to start with, must be read from file...
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:()?updateURIsURL=file:///" + updateFile.getAbsoluteFile());
        connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = session.createQueue(QUEUE_NAME);
        MessageProducer producer = session.createProducer(theQueue);
        MessageConsumer consumer = session.createConsumer(theQueue);
        Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);

        bs1.stop();
        bs1.waitUntilStopped();

        bs2 = createBroker("bs2", secondTcpUri);
        bs2.start();

        // add the transport uri for broker number 2
        out = new FileOutputStream(updateFile, true);
        out.write(",".getBytes());
        out.write(secondTcpUri.toString().getBytes());
        out.close();

        producer.send(message);
        msg = consumer.receive(2000);
        assertNotNull(msg);
    }

    private BrokerService createBroker(String name, String tcpUri) throws Exception {
        BrokerService bs = new BrokerService();
        bs.setBrokerName(name);
        bs.setUseJmx(false);
        bs.setPersistent(false);
        bs.addConnector(tcpUri);
        return bs;
    }

    public void testAutoUpdateURIs() throws Exception {

        BrokerService bs1 = new BrokerService();
        bs1.setUseJmx(false);
        TransportConnector transportConnector = bs1.addConnector(firstTcpUri);
        transportConnector.setUpdateClusterClients(true);
        bs1.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + firstTcpUri + ")");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = session.createQueue(QUEUE_NAME);
        MessageProducer producer = session.createProducer(theQueue);
        MessageConsumer consumer = session.createConsumer(theQueue);
        Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(4000);
        assertNotNull(msg);

        bs2 = createBroker("bs2", secondTcpUri);
        NetworkConnector networkConnector = bs2.addNetworkConnector("static:(" + firstTcpUri + ")");
        networkConnector.setDuplex(true);
        bs2.start();
        LOG.info("started brokerService 2");
        bs2.waitUntilStarted();

        TimeUnit.SECONDS.sleep(4);

        LOG.info("stopping brokerService 1");
        bs1.stop();
        bs1.waitUntilStopped();

        producer.send(message);
        msg = consumer.receive(4000);
        assertNotNull(msg);
    }
}
