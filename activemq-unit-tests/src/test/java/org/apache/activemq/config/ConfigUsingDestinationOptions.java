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
package org.apache.activemq.config;

import javax.jms.Connection;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.command.ActiveMQQueue;

public class ConfigUsingDestinationOptions extends TestCase {
    public void testValidSelectorConfig() throws JMSException {
        ActiveMQQueue queue = new ActiveMQQueue("TEST.FOO?consumer.selector=test=1");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection conn = factory.createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer cons;
        // JMS selector should be priority
        cons = (ActiveMQMessageConsumer) sess.createConsumer(queue, "test=2");
        assertEquals("test=2", cons.getMessageSelector());

        // Test setting using JMS destinations
        cons = (ActiveMQMessageConsumer) sess.createConsumer(queue);
        assertEquals("test=1", cons.getMessageSelector());
    }

    public void testInvalidSelectorConfig() throws JMSException {
        ActiveMQQueue queue = new ActiveMQQueue("TEST.FOO?consumer.selector=test||1");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection conn = factory.createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer cons;
        // JMS selector should be priority
        try {
            cons = (ActiveMQMessageConsumer) sess.createConsumer(queue, "test||1");
            fail("Selector should be invalid" + cons);
        } catch (InvalidSelectorException e) {

        }

        // Test setting using JMS destinations
        try {
            cons = (ActiveMQMessageConsumer) sess.createConsumer(queue);
            fail("Selector should be invalid" + cons);
        } catch (InvalidSelectorException e) {

        }
    }
}
