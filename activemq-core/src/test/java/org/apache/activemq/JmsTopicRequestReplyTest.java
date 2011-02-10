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

import java.util.List;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;

import org.apache.activemq.test.TestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.3 $
 */
public class JmsTopicRequestReplyTest extends TestSupport implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicRequestReplyTest.class);

    protected boolean useAsyncConsume;
    private Connection serverConnection;
    private Connection clientConnection;
    private MessageProducer replyProducer;
    private Session serverSession;
    private Destination requestDestination;
    private List<JMSException> failures = new Vector<JMSException>();
    private boolean dynamicallyCreateProducer;
    private String clientSideClientID;

    public void testSendAndReceive() throws Exception {
        clientConnection = createConnection();
        clientConnection.setClientID("ClientConnection:" + getSubject());

        Session session = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        clientConnection.start();

        Destination replyDestination = createTemporaryDestination(session);

        // lets test the destination
        clientSideClientID = clientConnection.getClientID();

        // TODO
        // String value = ActiveMQDestination.getClientId((ActiveMQDestination)
        // replyDestination);
        // assertEquals("clientID from the temporary destination must be the
        // same", clientSideClientID, value);
        LOG.info("Both the clientID and destination clientID match properly: " + clientSideClientID);

        /* build queues */
        MessageProducer requestProducer = session.createProducer(requestDestination);
        MessageConsumer replyConsumer = session.createConsumer(replyDestination);

        /* build requestmessage */
        TextMessage requestMessage = session.createTextMessage("Olivier");
        requestMessage.setJMSReplyTo(replyDestination);
        requestProducer.send(requestMessage);

        LOG.info("Sent request.");
        LOG.info(requestMessage.toString());

        Message msg = replyConsumer.receive(5000);

        if (msg instanceof TextMessage) {
            TextMessage replyMessage = (TextMessage)msg;
            LOG.info("Received reply.");
            LOG.info(replyMessage.toString());
            assertEquals("Wrong message content", "Hello: Olivier", replyMessage.getText());
        } else {
            fail("Should have received a reply by now");
        }
        replyConsumer.close();
        deleteTemporaryDestination(replyDestination);

        assertEquals("Should not have had any failures: " + failures, 0, failures.size());
    }

    public void testSendAndReceiveWithDynamicallyCreatedProducer() throws Exception {
        dynamicallyCreateProducer = true;
        testSendAndReceive();
    }

    /**
     * Use the asynchronous subscription mechanism
     */
    public void onMessage(Message message) {
        try {
            TextMessage requestMessage = (TextMessage)message;

            LOG.info("Received request.");
            LOG.info(requestMessage.toString());

            Destination replyDestination = requestMessage.getJMSReplyTo();

            // TODO
            // String value =
            // ActiveMQDestination.getClientId((ActiveMQDestination)
            // replyDestination);
            // assertEquals("clientID from the temporary destination must be the
            // same", clientSideClientID, value);

            TextMessage replyMessage = serverSession.createTextMessage("Hello: " + requestMessage.getText());

            replyMessage.setJMSCorrelationID(requestMessage.getJMSMessageID());

            if (dynamicallyCreateProducer) {
                replyProducer = serverSession.createProducer(replyDestination);
                replyProducer.send(replyMessage);
            } else {
                replyProducer.send(replyDestination, replyMessage);
            }

            LOG.info("Sent reply.");
            LOG.info(replyMessage.toString());
        } catch (JMSException e) {
            onException(e);
        }
    }

    /**
     * Use the synchronous subscription mechanism
     */
    protected void syncConsumeLoop(MessageConsumer requestConsumer) {
        try {
            Message message = requestConsumer.receive(5000);
            if (message != null) {
                onMessage(message);
            } else {
                LOG.error("No message received");
            }
        } catch (JMSException e) {
            onException(e);
        }
    }

    protected void setUp() throws Exception {
        super.setUp();

        serverConnection = createConnection();
        serverConnection.setClientID("serverConnection:" + getSubject());
        serverSession = serverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        replyProducer = serverSession.createProducer(null);

        requestDestination = createDestination(serverSession);

        /* build queues */
        final MessageConsumer requestConsumer = serverSession.createConsumer(requestDestination);
        if (useAsyncConsume) {
            requestConsumer.setMessageListener(this);
        } else {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    syncConsumeLoop(requestConsumer);
                }
            });
            thread.start();
        }
        serverConnection.start();
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        serverConnection.close();
        clientConnection.stop();
        clientConnection.close();
    }

    protected void onException(JMSException e) {
        LOG.info("Caught: " + e);
        e.printStackTrace();
        failures.add(e);
    }

    protected Destination createDestination(Session session) throws JMSException {
        if (topic) {
            return session.createTopic(getSubject());
        }
        return session.createQueue(getSubject());
    }

    protected Destination createTemporaryDestination(Session session) throws JMSException {
        if (topic) {
            return session.createTemporaryTopic();
        }
        return session.createTemporaryQueue();
    }
    
    protected void deleteTemporaryDestination(Destination dest) throws JMSException {
        if (topic) {
            ((TemporaryTopic)dest).delete();
        } else {
            ((TemporaryQueue)dest).delete();
        }
    }

}
