/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;

import org.activemq.command.ActiveMQQueue;
import org.activemq.command.RedeliveryPolicy;

/**
 * Test cases used to test the JMS message exclusive consumers.
 * 
 * @version $Revision$
 */
public class RedeliveryPolicyTest extends JmsTestSupport {

    public static Test suite() {
        return suite(RedeliveryPolicyTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    /**
     * @throws Throwable
     */
    public void testExponentialRedeliveryPolicyDelaysDeliveryOnRollback() throws Throwable {

        // Receive a message with the JMS API
        RedeliveryPolicy policy = connection.getRedeliveryPolicy();
        policy.setInitialRedeliveryDelay(500);
        policy.setBackOffMultiplier((short) 2);
        policy.setUseExponentialBackOff(true);
        
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        session.commit();
        
        TextMessage m;
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();

        // Show re-delivery delay is incrementing.
        m = (TextMessage)consumer.receive(100);
        assertNull(m);
        m = (TextMessage)consumer.receive(500);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();
        
        // Show re-delivery delay is incrementing exponentially
        m = (TextMessage)consumer.receive(100);
        assertNull(m);
        m = (TextMessage)consumer.receive(500);
        assertNull(m);
        m = (TextMessage)consumer.receive(500);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        
    }


    /**
     * @throws Throwable
     */
    public void testNornalRedeliveryPolicyDelaysDeliveryOnRollback() throws Throwable {

        // Receive a message with the JMS API
        RedeliveryPolicy policy = connection.getRedeliveryPolicy();
        policy.setInitialRedeliveryDelay(500);
        
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        session.commit();
        
        TextMessage m;
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();

        // Show re-delivery delay is incrementing.
        m = (TextMessage)consumer.receive(100);
        assertNull(m);
        m = (TextMessage)consumer.receive(500);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();
        
        // The message gets redelivered after 500 ms every time since
        // we are not using exponential backoff.
        m = (TextMessage)consumer.receive(100);
        assertNull(m);
        m = (TextMessage)consumer.receive(500);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        
    }

    /**
     * @throws Throwable
     */
    public void testDLQHandling() throws Throwable {

        // Receive a message with the JMS API
        RedeliveryPolicy policy = connection.getRedeliveryPolicy();
        policy.setInitialRedeliveryDelay(100);
        policy.setUseExponentialBackOff(false);
        policy.setMaximumRedeliveries(2);
        
        connection.start();
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        
        MessageConsumer consumer = session.createConsumer(destination);
        MessageConsumer dlqConsumer = session.createConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));

        // Send the messages
        producer.send(session.createTextMessage("1st"));
        producer.send(session.createTextMessage("2nd"));
        session.commit();
        
        TextMessage m;
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();

        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();
        
        // The last rollback should cause the 1st message to get sent to the DLQ 
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("2nd", m.getText());        
        session.commit();
        
        // We should be able to get the message off the DLQ now.
        m = (TextMessage)dlqConsumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.commit();
        
    }
}
