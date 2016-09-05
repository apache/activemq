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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSClientRequestResponseTest extends AmqpTestSupport implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(JMSClientRequestResponseTest.class);

    private Connection requestorConnection;
    private Destination requestDestination;
    private Session requestorSession;

    private Connection responderConnection;
    private MessageProducer responseProducer;
    private Session responderSession;
    private Destination replyDestination;

    private final List<JMSException> failures = new Vector<JMSException>();
    private boolean dynamicallyCreateProducer;
    private final boolean useAsyncConsumer = true;
    private Thread syncThread;

    @Override
    @After
    public void tearDown() throws Exception {
        if (requestorConnection != null) {
            try {
                requestorConnection.close();
            } catch (Exception e) {}
        }
        if (responderConnection != null) {
            try {
                responderConnection.close();
            } catch (Exception e) {}
        }

        if (syncThread != null) {
            syncThread.join(5000);
        }

        super.tearDown();
    }

    private void doSetupConnections(boolean topic) throws Exception {
        responderConnection = createConnection(name.getMethodName() + "-responder");
        responderSession = responderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (topic) {
            requestDestination = responderSession.createTopic(name.getMethodName());
        } else {
            requestDestination = responderSession.createQueue(name.getMethodName());
        }
        responseProducer = responderSession.createProducer(null);

        final MessageConsumer requestConsumer = responderSession.createConsumer(requestDestination);
        if (useAsyncConsumer) {
            requestConsumer.setMessageListener(this);
        } else {
            syncThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    syncConsumeLoop(requestConsumer);
                }
            });
            syncThread.start();
        }
        responderConnection.start();

        requestorConnection = createConnection(name.getMethodName() + "-requestor");
        requestorSession = requestorConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (topic) {
            replyDestination = requestorSession.createTemporaryTopic();
        } else {
            replyDestination = requestorSession.createTemporaryQueue();
        }
        requestorConnection.start();
    }

    @Test(timeout=60000)
    public void testRequestResponseToTempQueue() throws Exception {
        doSetupConnections(false);
        doTestRequestResponse();
    }

    @Test(timeout=60000)
    public void testRequestResponseToTempTopic() throws Exception {
        doSetupConnections(true);
        doTestRequestResponse();
    }

    private void doTestRequestResponse() throws Exception {

        MessageProducer requestProducer = requestorSession.createProducer(requestDestination);
        MessageConsumer replyConsumer = requestorSession.createConsumer(replyDestination);

        TextMessage requestMessage = requestorSession.createTextMessage("SomeRequest");
        requestMessage.setJMSReplyTo(replyDestination);
        requestProducer.send(requestMessage);

        LOG.info("Sent request to destination: {}", requestDestination.toString());

        Message msg = replyConsumer.receive(10000);

        if (msg instanceof TextMessage) {
            TextMessage replyMessage = (TextMessage)msg;
            LOG.info("Received reply.");
            LOG.info(replyMessage.toString());
            assertTrue("Wrong message content", replyMessage.getText().startsWith("response"));
        } else {
            fail("Should have received a reply by now");
        }
        replyConsumer.close();

        assertEquals("Should not have had any failures: " + failures, 0, failures.size());
    }

    private Connection createConnection(String clientId) throws JMSException {
        return JMSClientContext.INSTANCE.createConnection(amqpURI, "admin", "password", clientId);
    }

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

    @Override
    public void onMessage(Message message) {
        try {
            TextMessage requestMessage = (TextMessage)message;

            LOG.info("Received request.");
            LOG.info(requestMessage.toString());

            Destination replyDestination = requestMessage.getJMSReplyTo();
            if (replyDestination instanceof Topic) {
                LOG.info("Reply destination is: {}", ((Topic)replyDestination).getTopicName());
            } else {
                LOG.info("Reply destination is: {}", ((Queue)replyDestination).getQueueName());
            }

            TextMessage replyMessage = responderSession.createTextMessage("response for: " + requestMessage.getText());
            replyMessage.setJMSCorrelationID(requestMessage.getJMSMessageID());

            if (dynamicallyCreateProducer) {
                responseProducer = responderSession.createProducer(replyDestination);
                responseProducer.send(replyMessage);
            } else {
                responseProducer.send(replyDestination, replyMessage);
            }

            LOG.info("Sent reply.");
            LOG.info(replyMessage.toString());
        } catch (JMSException e) {
            onException(e);
        }
    }

    protected void onException(JMSException e) {
        LOG.info("Caught: " + e);
        e.printStackTrace();
        failures.add(e);
    }
}
