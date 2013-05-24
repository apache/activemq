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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AMQ4554Test extends TestCase {

    private final Logger LOG = LoggerFactory.getLogger(AMQ4554Test.class);

    private String connectionURI;
    private BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        connectionURI = broker.addConnector("tcp://0.0.0.0:0?maximumConnections=1").getPublishableConnectString();
        broker.setPersistent(false);
        broker.start();
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        super.tearDown();
    }

    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
     */
    public AMQ4554Test(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AMQ4554Test.class);
    }

    public void testMSXProducerTXID() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionURI);
        Connection connection = factory.createConnection();
        connection.start();

        Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = producerSession.createProducer(producerSession.createQueue("myQueue"));
        TextMessage producerMessage = producerSession.createTextMessage("Test Message");
        producer.send(producerMessage);
        producer.close();
        producerSession.commit();
        producerSession.close();

        Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createQueue("myQueue"));
        Message consumerMessage = consumer.receive(1000);
        try {
            String txId = consumerMessage.getStringProperty("JMSXProducerTXID");
            assertNotNull(txId);
        } catch(Exception e) {
            LOG.info("Caught Exception that was not expected:", e);
            fail("Should not throw");
        }
        consumer.close();
        consumerSession.commit();
        consumerSession.close();
        connection.close();
    }

}
