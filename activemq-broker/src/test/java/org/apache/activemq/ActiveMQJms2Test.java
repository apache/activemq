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
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActiveMQJms2Test {

    protected static ActiveMQConnectionFactory activemqConnectionFactory = null;

    protected Connection connection = null;
    protected Session session = null;
    protected MessageProducer messageProducer = null;

    @BeforeClass
    public static void setUpClass() {
        activemqConnectionFactory = new ActiveMQConnectionFactory("vm://localhost?marshal=false&broker.persistent=false"); 	
    }

    @AfterClass
    public static void tearDownClass() {
        activemqConnectionFactory = null;
    }

    @Before
    public void setUp() throws JMSException {
        connection = activemqConnectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        messageProducer = session.createProducer(session.createQueue("AMQ.JMS2.TEST"));
    }

    @After
    public void tearDown() {
        if(messageProducer != null) {
            try { messageProducer.close(); } catch (Exception e) { } finally { messageProducer = null; }
        }

        if(session != null) {
            try { session.close(); } catch (Exception e) { } finally { session = null; }
        }

        if(connection != null) {
            try { connection.close(); } catch (Exception e) { } finally { connection = null; }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionFactoryCreateContext() {
        activemqConnectionFactory.createContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionFactoryCreateContextSession() {
        activemqConnectionFactory.createContext(Session.AUTO_ACKNOWLEDGE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionFactoryCreateContextUserPass() {
        activemqConnectionFactory.createContext("admin", "admin");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionFactoryCreateContextUserPassSession() {
        activemqConnectionFactory.createContext("admin", "admin", Session.AUTO_ACKNOWLEDGE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionSharedConnectionConsumer() throws JMSException {
        connection.createSharedConnectionConsumer(null, null, null, null, 10);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionSharedDurableConnectionConsumer() throws JMSException {
        connection.createSharedDurableConnectionConsumer(null, null, null, null, 10);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionAckMode() throws JMSException {
        connection.createSession(Session.AUTO_ACKNOWLEDGE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionDurableConsumer() throws JMSException {
        session.createDurableConsumer(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionDurableConsumerSelectorNoLocal() throws JMSException {
        session.createDurableConsumer(null, null, null, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedConsumer() throws JMSException {
        session.createSharedConsumer(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedConsumerSelector() throws JMSException {
        session.createSharedConsumer(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedDurableConsumer() throws JMSException {
        session.createSharedDurableConsumer(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedDurableConsumerSelector() throws JMSException {
        session.createSharedDurableConsumer(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerDeliveryDelayGet() throws JMSException {
        messageProducer.getDeliveryDelay();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerDeliveryDelaySet() throws JMSException {
        messageProducer.setDeliveryDelay(1000l);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendMessageCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue("AMQ.TEST"), null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendMessageQoSParamsCompletionListener() throws JMSException {
         messageProducer.send(null, 1, 4, 0l, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendDestinationMessageCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue("AMQ.TEST"), null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendDestinationMessageQosParamsCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue("AMQ.TEST"), null, 1, 4, 0l, null);
    }

}
