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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQMessageProducerSupport;
import org.junit.Test;

public class ActiveMQJMS2ProducerTest extends ActiveMQJMS2TestBase {

    private static final String PROPERTY_NAME_VALID="ValidName";
    private static final String PROPERTY_VALUE_VALID="valid value";

    private static final Set<String> PROPERTY_NAMES_INVALID = Set.of("contains+plus+sign", "123startswithnumber", "contains blank space", "has-dash", "with&amp");
    private static final Set<Object> PROPERTY_VALUES_INVALID = Set.of(new HashSet<String>(), new HashMap<Integer, String>(), ZonedDateTime.now());

    @Test
    public void testInvalidPropertyNameNullEmpty() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            jmsContext.start();

            boolean caught = false;
            try {
                jmsProducer.setProperty(null, PROPERTY_VALUE_VALID);
            } catch (Exception e) {
                assertEquals("Invalid JMS property name must not be null or empty", e.getMessage());
                caught = true;
            }
            assertTrue(caught);
            caught = false;

            try {
                jmsProducer.setProperty("", PROPERTY_VALUE_VALID);
            } catch (Exception e) {
                assertEquals("Invalid JMS property name must not be null or empty", e.getMessage());
                caught = true;
            }
            assertTrue(caught);
            caught = false;
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidPropertyNameKeyword() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            jmsContext.start();

            for(String propertyName : ActiveMQMessageProducerSupport.JMS_PROPERTY_NAMES_DISALLOWED) {
                boolean caught = false;
                try {
                    jmsProducer.setProperty(propertyName, PROPERTY_VALUE_VALID);
                } catch (Exception e) {
                    assertEquals("Invalid JMS property: " + propertyName + " name is in disallowed list", e.getMessage());
                    caught = true;
                }
                assertTrue("Expected exception for propertyName: " + propertyName, caught);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidPropertyNameInvalidJavaIdentifier() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            jmsContext.start();

            for(String propertyName : PROPERTY_NAMES_INVALID) {
                boolean caught = false;
                try {
                    jmsProducer.setProperty(propertyName, PROPERTY_VALUE_VALID);
                } catch (Exception e) {
                    assertTrue(e.getMessage().startsWith("Invalid JMS property: " + propertyName + " name"));
                    assertTrue(e.getMessage().contains("invalid character"));
                    caught = true;
                }
                assertTrue("Expected exception for propertyName: " + propertyName, caught);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidPropertyNameInvalidValueClassType() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            jmsContext.start();

            for(Object propertyValue : PROPERTY_VALUES_INVALID) {
                boolean caught = false;
                try {
                    jmsProducer.setProperty(PROPERTY_NAME_VALID, propertyValue);
                } catch (Exception e) {
                    assertEquals("Invalid JMS property: " + PROPERTY_NAME_VALID + " value class: " + propertyValue.getClass().getName() + " is not permitted by specification", e.getMessage());
                    caught = true;
                }
                assertTrue("Expected exception for propertyValue: " + propertyValue, caught);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDisableTimestamp() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            JMSProducer jmsProducer1 = jmsContext.createProducer();
            jmsProducer1.setDisableMessageTimestamp(true);
            jmsContext.start();

            String textBody = "Test-" + methodNameDestinationName;
            jmsProducer1.send(destination, textBody);
            verifyTimestamp(jmsContext, destination, textBody, true);

            // Re-enable timestatmp
            jmsProducer1.setDisableMessageTimestamp(false);
            jmsProducer1.send(destination, textBody);
            verifyTimestamp(jmsContext, destination, textBody, false);

            // Create second producer from same Context
            JMSProducer jmsProducer2 = jmsContext.createProducer();
            jmsProducer2.setDisableMessageTimestamp(true);
            jmsProducer2.send(destination, textBody);
            verifyTimestamp(jmsContext, destination, textBody, true);

            // Re-confirm jmsProducer1 setting remains
            jmsProducer1.send(destination, textBody);
            verifyTimestamp(jmsContext, destination, textBody, false);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    protected static void verifyTimestamp(JMSContext jmsContext, Destination destination, String expectedTextBody, boolean expectTimestampZero) throws JMSException {
        try(JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
            javax.jms.Message message = jmsConsumer.receive(1000l);
            assertNotNull(message);
            assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
            assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());

            if(expectTimestampZero) {
                assertEquals(Long.valueOf(0), Long.valueOf(message.getJMSTimestamp()));
            } else {
                assertNotEquals(Long.valueOf(0), Long.valueOf(message.getJMSTimestamp()));
            }
        }
    }
}
