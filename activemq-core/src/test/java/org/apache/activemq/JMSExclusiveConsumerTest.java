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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.command.ActiveMQQueue;

/**
 * Test cases used to test the JMS message exclusive consumers.
 * 
 * @version $Revision$
 */
public class JMSExclusiveConsumerTest extends JmsTestSupport {

    public static Test suite() {
        return suite(JMSExclusiveConsumerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public int deliveryMode;

    public void initCombosForTestRoundRobinDispatchOnNonExclusive() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    /**
     * Shows that by default messages are round robined across a set of
     * consumers.
     * 
     * @throws Exception
     */
    public void testRoundRobinDispatchOnNonExclusive() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        MessageConsumer consumer1 = session.createConsumer(destination);
        MessageConsumer consumer2 = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));

        Message m;
        m = consumer2.receive(1000);
        assertNotNull(m);

        m = consumer1.receive(1000);
        assertNotNull(m);

        assertNull(consumer1.receiveNoWait());
        assertNull(consumer2.receiveNoWait());
    }

    public void initCombosForTestDispatchExclusive() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    /**
     * Shows that if the "?consumer.exclusive=true" option is added to
     * destination, then all messages are routed to 1 consumer.
     * 
     * @throws Exception
     */
    public void testDispatchExclusive() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST?consumer.exclusive=true");
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        MessageConsumer consumer1 = session.createConsumer(destination);
        MessageConsumer consumer2 = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        producer.send(session.createTextMessage("3nd"));

        Message m;
        m = consumer2.receive(1000);
        if (m != null) {
            // Consumer 2 should get all the messages.
            for (int i = 0; i < 2; i++) {
                m = consumer2.receive(1000);
                assertNotNull(m);
            }
        } else {
            // Consumer 1 should get all the messages.
            for (int i = 0; i < 3; i++) {
                m = consumer1.receive(1000);
                assertNotNull(m);
            }
        }

        assertNull(consumer1.receiveNoWait());
        assertNull(consumer2.receiveNoWait());
    }

    public void testMixExclusiveWithNonExclusive() throws Exception {
        ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.FOO?consumer.exclusive=true");
        ActiveMQQueue nonExclusiveQueue = new ActiveMQQueue("TEST.FOO?consumer.exclusive=false");

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer nonExCon = session.createConsumer(nonExclusiveQueue);
        MessageConsumer exCon = session.createConsumer(exclusiveQueue);

        MessageProducer prod = session.createProducer(exclusiveQueue);
        prod.send(session.createMessage());
        prod.send(session.createMessage());
        prod.send(session.createMessage());

        Message m;
        for (int i = 0; i < 3; i++) {
            m = exCon.receive(1000);
            assertNotNull(m);
            m = nonExCon.receive(1000);
            assertNull(m);
        }
    }
}
