/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.camel.CamelContext;
import org.apache.camel.Handler;
import org.apache.camel.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit38.AbstractJUnit38SpringContextTests;

/**
 * 
 */
@ContextConfiguration
public class CamelRedeliveryTest extends AbstractJUnit38SpringContextTests {
    private static final transient Logger LOG = LoggerFactory.getLogger(CamelRedeliveryTest.class);

    @Autowired
    protected CamelContext camelContext;

    public void testRedeliveryViaCamel() throws Exception {
        
        
        ActiveMQConnectionFactory factory = applicationContext.getBean("connectionFactory", ActiveMQConnectionFactory.class);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        
        // send message to dlq immediately
        RedeliveryPolicy policy = connection.getRedeliveryPolicy();
        policy.setMaximumRedeliveries(0);        
        connection.start();
        
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        ActiveMQQueue destination = new ActiveMQQueue("camelRedeliveryQ");
        MessageProducer producer = session.createProducer(destination);
        
        MessageConsumer consumer = session.createConsumer(destination);
        // Send the messages
        producer.send(session.createTextMessage("1st"));
        session.commit();
        LOG.info("sent 1st message");
        
        TextMessage m;
        m = (TextMessage)consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", m.getText());        
        session.rollback();

        LOG.info("received and rolledback 1st message: " + m);
        m = (TextMessage)consumer.receive(1);
        assertNull("no immediate redelivery", m);
 
        m = (TextMessage)consumer.receive(20000);
        LOG.info("received redelivery on second wait attempt, message: " + m);
        
        assertNotNull("got redelivery on second attempt", m);
        assertEquals("text matches original", "1st", m.getText());
        
        // came from camel
        assertTrue("redelivery marker header set, so came from camel", m.getBooleanProperty("CamelRedeliveryMarker"));
    }

    public static class DestinationExtractor {

        @RecipientList @Handler
        public String routeTo(ActiveMQMessage body) throws Exception {
            ActiveMQDestination originalDestination = body.getOriginalDestination();
            return "activemq:" + originalDestination.getPhysicalName() + "?explicitQosEnabled=true&messageConverter=#messageConverter";
        }
    }
}
