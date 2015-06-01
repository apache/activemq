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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.net.ServerSocketFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.leveldb.LevelDBStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AMQ5816Test {

    private static BrokerService brokerService;

    @Rule public TestName name = new TestName();

    private File dataDirFile;
    private String connectionURI;

    @Before
    public void setUp() throws Exception {

        dataDirFile = new File("target/" + name.getMethodName());

        brokerService = new BrokerService();
        brokerService.setBrokerName("LevelDBBroker");
        brokerService.setPersistent(true);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setDataDirectoryFile(dataDirFile);

        TransportConnector connector = brokerService.addConnector("http://0.0.0.0:" + getFreePort());

        LevelDBStore persistenceFactory = new LevelDBStore();
        persistenceFactory.setDirectory(dataDirFile);
        brokerService.setPersistenceAdapter(persistenceFactory);
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = connector.getPublishableConnectString();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testSendPersistentMessage() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionURI);
        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(session.createTextMessage());

        assertNotNull(consumer.receive(5000));
    }

    protected int getFreePort() {
        int port = 8161;
        ServerSocket ss = null;

        try {
            ss = ServerSocketFactory.getDefault().createServerSocket(0);
            port = ss.getLocalPort();
        } catch (IOException e) { // ignore
        } finally {
            try {
                if (ss != null ) {
                    ss.close();
                }
            } catch (IOException e) { // ignore
            }
        }

        return port;
    }
}
