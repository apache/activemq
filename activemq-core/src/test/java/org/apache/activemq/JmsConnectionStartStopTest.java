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
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * @version $Revision: 1.3 $
 */
public class JmsConnectionStartStopTest extends TestSupport {

    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
        .getLog(JmsConnectionStartStopTest.class);

    private Connection startedConnection;
    private Connection stoppedConnection;

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {

        LOG.info(getClass().getClassLoader().getResource("log4j.properties"));

        ActiveMQConnectionFactory factory = createConnectionFactory();
        startedConnection = factory.createConnection();
        startedConnection.start();
        stoppedConnection = factory.createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        stoppedConnection.close();
        startedConnection.close();
    }

    /**
     * Tests if the consumer receives the messages that were sent before the
     * connection was started.
     * 
     * @throws JMSException
     */
    public void testStoppedConsumerHoldsMessagesTillStarted() throws JMSException {
        Session startedSession = startedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session stoppedSession = stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Setup the consumers.
        Topic topic = startedSession.createTopic("test");
        MessageConsumer startedConsumer = startedSession.createConsumer(topic);
        MessageConsumer stoppedConsumer = stoppedSession.createConsumer(topic);

        // Send the message.
        MessageProducer producer = startedSession.createProducer(topic);
        TextMessage message = startedSession.createTextMessage("Hello");
        producer.send(message);

        // Test the assertions.
        Message m = startedConsumer.receive(1000);
        assertNotNull(m);

        m = stoppedConsumer.receive(1000);
        assertNull(m);

        stoppedConnection.start();
        m = stoppedConsumer.receive(5000);
        assertNotNull(m);

        startedSession.close();
        stoppedSession.close();
    }

    /**
     * Tests if the consumer is able to receive messages eveb when the
     * connecction restarts multiple times.
     * 
     * @throws Exception
     */
    public void testMultipleConnectionStops() throws Exception {
        testStoppedConsumerHoldsMessagesTillStarted();
        stoppedConnection.stop();
        testStoppedConsumerHoldsMessagesTillStarted();
        stoppedConnection.stop();
        testStoppedConsumerHoldsMessagesTillStarted();
    }
}
