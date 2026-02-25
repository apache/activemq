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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQXAContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class ActiveMQJMS2XAContextTest extends ActiveMQJMS2XATestBase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // [AMQ-8325] We override ackMode for unit tests. Actual XA features is tested elsewhere
        activemqXAConnectionFactory.setXaAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testConnectionFactoryCreateXAContext() {
        try(JMSContext jmsContext = activemqXAConnectionFactory.createXAContext()) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQXAContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            recvMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionFactoryCreateContextUserPass() {
        try(JMSContext jmsContext = activemqXAConnectionFactory.createXAContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQXAContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            recvMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }
}
