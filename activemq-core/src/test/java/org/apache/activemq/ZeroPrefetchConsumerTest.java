/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.spring.SpringConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @version $Revision$
 */
public class ZeroPrefetchConsumerTest extends EmbeddedBrokerTestSupport {

    private static final Log log = LogFactory.getLog(ZeroPrefetchConsumerTest.class);

    protected Connection connection;
    protected Queue queue;

    public void testCannotUseMessageListener() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);

        MessageListener listener = new SpringConsumer();
        try {
            consumer.setMessageListener(listener);
            fail("Should have thrown JMSException as we cannot use MessageListener with zero prefetch");
        } catch (JMSException e) {
            log.info("Received expected exception : " + e);
        }
    }

    public void testPullConsumerWorks() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello World!"));

        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive(5000);
        assertNotNull("Should have received a message!", answer);
        // check if method will return at all and will return a null
        answer = consumer.receive(1);
        assertNull("Should have not received a message!", answer);
        answer = consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testIdleConsumer() throws Exception {
        doTestIdleConsumer(false);
    }

    public void testIdleConsumerTranscated() throws Exception {
        doTestIdleConsumer(true);
    }

    private void doTestIdleConsumer(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        // noinspection UNUSED_SYMBOL
        MessageConsumer idleConsumer = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        if (transacted) {
            session.commit();
        }
        // this call would return null if prefetchSize > 0
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testRecvRecvCommit() throws Exception {
        doTestRecvRecvCommit(false);
    }

    public void testRecvRecvCommitTranscated() throws Exception {
        doTestRecvRecvCommit(true);
    }

    private void doTestRecvRecvCommit(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        answer = (TextMessage)consumer.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        super.setUp();

        connection = createConnection();
        connection.start();
        queue = createQueue();
    }

    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    protected Queue createQueue() {
        return new ActiveMQQueue(getDestinationString() + "?consumer.prefetchSize=0");
    }

}
