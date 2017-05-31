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

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.test.JmsTopicSendReceiveTest;

/**
 * 
 */
public class JmsQueueWildcardSendReceiveTest extends JmsTopicSendReceiveTest {

    private String destination1String = "TEST.ONE.ONE";
    private String destination2String = "TEST.ONE.ONE.ONE";
    private String destination3String = "TEST.ONE.TWO";
    private String destination4String = "TEST.TWO.ONE";

    /**
     * Sets a test to have a queue destination and non-persistent delivery mode.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        topic = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        super.setUp();
    }

    /**
     * Returns the consumer subject.
     * 
     * @return String - consumer subject
     * @see org.apache.activemq.test.TestSupport#getConsumerSubject()
     */
    protected String getConsumerSubject() {
        return "FOO.>";
    }

    /**
     * Returns the producer subject.
     * 
     * @return String - producer subject
     * @see org.apache.activemq.test.TestSupport#getProducerSubject()
     */
    protected String getProducerSubject() {
        return "FOO.BAR.HUMBUG";
    }

    public void testReceiveWildcardQueueEndAsterisk() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQDestination destination1 = (ActiveMQDestination)session.createQueue(destination1String);
        ActiveMQDestination destination3 = (ActiveMQDestination)session.createQueue(destination3String);

        Message m = null;
        MessageConsumer consumer = null;
        String text = null;

        sendMessage(session, destination1, destination1String);
        sendMessage(session, destination3, destination3String);
        ActiveMQDestination destination6 = (ActiveMQDestination)session.createQueue("TEST.ONE.*");
        consumer = session.createConsumer(destination6);
        m = consumer.receive(1000);
        assertNotNull(m);
        text = ((TextMessage)m).getText();
        if (!(text.equals(destination1String) || text.equals(destination3String))) {
            fail("unexpected message:" + text);
        }
        m = consumer.receive(1000);
        assertNotNull(m);
        text = ((TextMessage)m).getText();
        if (!(text.equals(destination1String) || text.equals(destination3String))) {
            fail("unexpected message:" + text);
        }
        assertNull(consumer.receiveNoWait());
    }

    public void testReceiveWildcardQueueEndGreaterThan() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQDestination destination1 = (ActiveMQDestination)session.createQueue(destination1String);
        ActiveMQDestination destination2 = (ActiveMQDestination)session.createQueue(destination2String);
        ActiveMQDestination destination3 = (ActiveMQDestination)session.createQueue(destination3String);

        Message m = null;
        MessageConsumer consumer = null;
        String text = null;

        sendMessage(session, destination1, destination1String);
        sendMessage(session, destination2, destination2String);
        sendMessage(session, destination3, destination3String);
        ActiveMQDestination destination7 = (ActiveMQDestination)session.createQueue("TEST.ONE.>");
        consumer = session.createConsumer(destination7);
        m = consumer.receive(1000);
        assertNotNull(m);
        text = ((TextMessage)m).getText();
        if (!(text.equals(destination1String) || text.equals(destination2String) || text.equals(destination3String))) {
            fail("unexpected message:" + text);
        }
        m = consumer.receive(1000);
        assertNotNull(m);
        if (!(text.equals(destination1String) || text.equals(destination2String) || text.equals(destination3String))) {
            fail("unexpected message:" + text);
        }
        m = consumer.receive(1000);
        assertNotNull(m);
        if (!(text.equals(destination1String) || text.equals(destination2String) || text.equals(destination3String))) {
            fail("unexpected message:" + text);
        }
        assertNull(consumer.receiveNoWait());
    }

    public void testReceiveWildcardQueueMidAsterisk() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQDestination destination1 = (ActiveMQDestination)session.createQueue(destination1String);
        ActiveMQDestination destination4 = (ActiveMQDestination)session.createQueue(destination4String);

        Message m = null;
        MessageConsumer consumer = null;
        String text = null;

        sendMessage(session, destination1, destination1String);
        sendMessage(session, destination4, destination4String);
        ActiveMQDestination destination8 = (ActiveMQDestination)session.createQueue("TEST.*.ONE");
        consumer = session.createConsumer(destination8);
        m = consumer.receive(1000);
        assertNotNull(m);
        text = ((TextMessage)m).getText();
        if (!(text.equals(destination1String) || text.equals(destination4String))) {
            fail("unexpected message:" + text);
        }
        m = consumer.receive(1000);
        assertNotNull(m);
        text = ((TextMessage)m).getText();
        if (!(text.equals(destination1String) || text.equals(destination4String))) {
            fail("unexpected message:" + text);
        }
        assertNull(consumer.receiveNoWait());

    }

    private void sendMessage(Session session, Destination destination, String text) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage(text));
        producer.close();
    }
}
