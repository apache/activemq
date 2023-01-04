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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

public class ActiveMQJMS2TestSupport {

    protected static final Set<String> PROPERTY_NAMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("JMS2_BOOLEAN_MIN", "JMS2_BOOLEAN_MAX", "JMS2_BYTE_MIN", "JMS2_BYTE_MAX",
                                                        "JMS2_DOUBLE_MIN", "JMS2_DOUBLE_MAX", "JMS2_INT_MIN", "JMS2_INT_MAX", "JMS2_FLOAT_MIN", "JMS2_FLOAT_MAX", "JMS2_LONG_MIN",
                                                        "JMS2_LONG_MAX", "JMS2_SHORT_MIN", "JMS2_SHORT_MAX", "JMS2_STRING_VAL")));

    private ActiveMQJMS2TestSupport() {}

    protected static Destination generateDestination(JMSContext jmsContext, String destinationType, String destinationName) throws JMSException {
        Destination destination = null;
        switch(destinationType) {
        case "queue":
            destination = jmsContext.createQueue(destinationName);
            break;
        case "topic":
            destination = jmsContext.createTopic(destinationName);
            break;
        case "temp-queue":
            destination = jmsContext.createTemporaryQueue();
            break;
        case "temp-topic":
            destination = jmsContext.createTemporaryTopic();
            break;
        default:
            fail("Unsupported destinationType:" + destinationType);
        }
       assertNotNull(destination);
       return destination;
    }

    protected static Message generateMessage(JMSContext jmsContext, String messageType, String payload) throws JMSException {
        assertNotNull(messageType);
        Message tmpMessage = null;
        switch(messageType) {
        case "bytes":
            BytesMessage bytesMessage = jmsContext.createBytesMessage();
            bytesMessage.writeBytes(payload.getBytes(StandardCharsets.UTF_8));
            tmpMessage = bytesMessage;
            break;
        case "map":
            MapMessage mapMessage = jmsContext.createMapMessage();
            mapMessage.setString("payload", payload);
            tmpMessage = mapMessage;
            break;
        case "object":
            tmpMessage = jmsContext.createObjectMessage(new ActiveMQJMS2TestObjectMessagePayload(payload));
            break;
        case "stream":
            StreamMessage streamMessage = jmsContext.createStreamMessage();
            streamMessage.writeString(payload);
            tmpMessage = streamMessage;
            break;
        case "text":
            tmpMessage = jmsContext.createTextMessage(payload);
            break;
        default:
            fail("Unsupported messageType:" + messageType);
        }
        return tmpMessage;
    }

    protected static void populateJMSHeaders(javax.jms.Message message, String correlationID, Destination replyTo, String jmsType) throws JMSException {
        assertNotNull(message);
        message.setJMSCorrelationID(null);
        message.setJMSReplyTo(null);
        message.setJMSType(null);
    }

    protected static String sendMessage(JMSContext jmsContext, Destination destination, Message message) throws JMSException {
        MessageData messageData = new MessageData();
        messageData.setMessage(message);
        return sendMessage(jmsContext, destination, messageData);
    }

    protected static String sendMessage(JMSContext jmsContext, Destination destination, MessageData messageData) throws JMSException {
        assertNotNull(jmsContext);
        assertNotNull(messageData);
        assertNotNull(messageData.getMessage());

        JMSProducer jmsProducer = jmsContext.createProducer();

        if(messageData.getDeliveryDelay() != null) {
            jmsProducer.setDeliveryDelay(messageData.getDeliveryDelay());
        }
        if(messageData.getDeliveryMode() != null) {
            jmsProducer.setDeliveryMode(messageData.getDeliveryMode());
        } 
        if(messageData.getDisableMessageID() != null) {
            jmsProducer.setDisableMessageID(messageData.getDisableMessageID());
        }
        if(messageData.getDisableMessageTimestamp() != null) {
            jmsProducer.setDisableMessageTimestamp(messageData.getDisableMessageTimestamp());
        }
        if(messageData.getCorrelationID() != null) {
            jmsProducer.setJMSCorrelationID(messageData.getCorrelationID());
        }
        if(messageData.getReplyTo() != null) {
            jmsProducer.setJMSReplyTo(messageData.getReplyTo());
        }
        if(messageData.getJmsType() != null) {
            jmsProducer.setJMSType(messageData.getJmsType());
        }
        if(messageData.getPriority() != null) {
            jmsProducer.setPriority(messageData.getPriority());
        }
        if(messageData.getTimeToLive() != null) {
            jmsProducer.setTimeToLive(messageData.getTimeToLive());
        }
        populateJMSProperties(jmsProducer);
        validateJMSProperties(jmsProducer);
        jmsProducer.send(destination, messageData.getMessage());

        return messageData.getMessage().getJMSMessageID();
    }

    protected static void browseMessage(JMSContext jmsContext, String testDestinationName, String expectedTextBody, boolean expectFound) throws JMSException {
        assertNotNull(jmsContext);
        try(QueueBrowser queueBrowser = jmsContext.createBrowser(jmsContext.createQueue(testDestinationName))) {
            Enumeration<?> messageEnumeration = queueBrowser.getEnumeration();
            assertNotNull(messageEnumeration);
            
            boolean found = false; 
            while(!found && messageEnumeration.hasMoreElements()) {
                javax.jms.Message message = (Message)messageEnumeration.nextElement();
                assertNotNull(message);
                assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
                assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
                found = true;
            }
            assertEquals(expectFound, found);
        }
    }

    protected static void validateMessageData(javax.jms.Message message, MessageData messageData) throws JMSException {
        assertNotNull(message);
        assertNotNull(messageData.getMessageType());
        assertNotNull(messageData.getMessagePayload());
        validateJMSHeaders(message, messageData);
        validateJMSProperties(message);

        switch(messageData.getMessageType()) {
        case "bytes":
            assertTrue(message instanceof BytesMessage);
            BytesMessage bytesMessage = BytesMessage.class.cast(message);
            byte[] payload = new byte[(int)bytesMessage.getBodyLength()];
            bytesMessage.readBytes(payload);
            assertEquals(messageData.getMessagePayload(), new String(payload, StandardCharsets.UTF_8));
            break;
        case "map":
            assertTrue(message instanceof MapMessage);
            MapMessage mapMessage = MapMessage.class.cast(message);
            String mapPayload = mapMessage.getString("payload");
            assertEquals(messageData.getMessagePayload(), mapPayload);
            break;
        case "object":
            assertTrue(message instanceof ObjectMessage);
            ObjectMessage objectMessage = ObjectMessage.class.cast(message);
            Object tmpObject = objectMessage.getObject();
            assertNotNull(tmpObject);
            assertTrue(tmpObject instanceof ActiveMQJMS2TestObjectMessagePayload);
            assertEquals(messageData.getMessagePayload(), ActiveMQJMS2TestObjectMessagePayload.class.cast(tmpObject).getPayload());
            break;
        case "stream":
            assertTrue(message instanceof StreamMessage);
            StreamMessage streamMessage = StreamMessage.class.cast(message);
            assertEquals(messageData.getMessagePayload(), streamMessage.readString());
            break;
        case "text":
            assertTrue(message instanceof TextMessage);
            assertEquals(messageData.getMessagePayload(), TextMessage.class.cast(message).getText());
            break;
        default:
            fail("Unsupported messageType:" + messageData.getMessageType());
        }
    }

    private static void validateJMSHeaders(javax.jms.Message message, MessageData messageData) throws JMSException {
        assertNotNull(message);
        assertEquals(messageData.getCorrelationID(), message.getJMSCorrelationID());
        if(messageData.getDeliveryMode() != null) {
            assertEquals(messageData.getDeliveryMode(), Integer.valueOf(message.getJMSDeliveryMode()));
        }
        if(messageData.getDeliveryTime() != null) {
            assertEquals(messageData.getDeliveryTime(), Long.valueOf(message.getJMSDeliveryTime()));
        }
        if(messageData.getExpiration() != null) {
            assertEquals(messageData.getExpiration(), Long.valueOf(message.getJMSExpiration()));
        }
        if(messageData.getMessageID() != null) {
            assertEquals(messageData.getMessageID(), message.getJMSMessageID());
        }
        if(messageData.getPriority() != null) {
            assertEquals(messageData.getPriority(), Integer.valueOf(message.getJMSPriority()));
        }
        assertEquals(messageData.getReplyTo(), message.getJMSReplyTo());
        if(messageData.getTimestamp() != null) {
            assertEquals(messageData.getTimestamp(), Long.valueOf(message.getJMSTimestamp()));
        }
        if(Boolean.TRUE.equals(messageData.getDisableMessageTimestamp())) {
            assertEquals(Long.valueOf(0l), Long.valueOf(message.getJMSTimestamp()));
        }
        assertEquals(messageData.getJmsType(), message.getJMSType());
    }

    private static void populateJMSProperties(JMSProducer jmsProducer) throws JMSException {
        jmsProducer.setProperty("JMS2_BOOLEAN_MIN", false);
        jmsProducer.setProperty("JMS2_BOOLEAN_MAX", true);
        jmsProducer.setProperty("JMS2_BYTE_MIN", Byte.MIN_VALUE);
        jmsProducer.setProperty("JMS2_BYTE_MAX", Byte.MAX_VALUE);
        jmsProducer.setProperty("JMS2_DOUBLE_MIN", Double.MIN_VALUE);
        jmsProducer.setProperty("JMS2_DOUBLE_MAX", Double.MAX_VALUE);
        jmsProducer.setProperty("JMS2_INT_MIN", Integer.MIN_VALUE);
        jmsProducer.setProperty("JMS2_INT_MAX", Integer.MAX_VALUE);
        jmsProducer.setProperty("JMS2_FLOAT_MIN", Float.MIN_VALUE);
        jmsProducer.setProperty("JMS2_FLOAT_MAX", Float.MAX_VALUE);
        jmsProducer.setProperty("JMS2_LONG_MIN", Long.MIN_VALUE);
        jmsProducer.setProperty("JMS2_LONG_MAX", Long.MAX_VALUE);
        jmsProducer.setProperty("JMS2_SHORT_MIN", Short.MIN_VALUE);
        jmsProducer.setProperty("JMS2_SHORT_MAX", Short.MAX_VALUE);
        jmsProducer.setProperty("JMS2_STRING_VAL", "Hello World");
    }

    private static void validateJMSProperties(JMSProducer jmsProducer) throws JMSException {
        assertNotNull(jmsProducer);
        assertNotNull(jmsProducer.getPropertyNames());
        assertEquals(Integer.valueOf(PROPERTY_NAMES.size()), Integer.valueOf(jmsProducer.getPropertyNames().size()));
        for(String propertyName : PROPERTY_NAMES) {
            assertTrue(jmsProducer.propertyExists(propertyName));
        }
        assertEquals(Boolean.FALSE, Boolean.valueOf(jmsProducer.getBooleanProperty("JMS2_BOOLEAN_MIN")));
        assertEquals(Boolean.TRUE, Boolean.valueOf(jmsProducer.getBooleanProperty("JMS2_BOOLEAN_MAX")));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), Byte.valueOf(jmsProducer.getByteProperty("JMS2_BYTE_MIN")));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), Byte.valueOf(jmsProducer.getByteProperty("JMS2_BYTE_MAX")));
        assertEquals(Double.valueOf(Double.MIN_VALUE), Double.valueOf(jmsProducer.getDoubleProperty("JMS2_DOUBLE_MIN")));
        assertEquals(Double.valueOf(Double.MAX_VALUE), Double.valueOf(jmsProducer.getDoubleProperty("JMS2_DOUBLE_MAX")));
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), Integer.valueOf(jmsProducer.getIntProperty("JMS2_INT_MIN")));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), Integer.valueOf(jmsProducer.getIntProperty("JMS2_INT_MAX")));
        assertEquals(Float.valueOf(Float.MIN_VALUE), Float.valueOf(jmsProducer.getFloatProperty("JMS2_FLOAT_MIN")));
        assertEquals(Float.valueOf(Float.MAX_VALUE), Float.valueOf(jmsProducer.getFloatProperty("JMS2_FLOAT_MAX")));
        assertEquals(Long.valueOf(Long.MIN_VALUE), Long.valueOf(jmsProducer.getLongProperty("JMS2_LONG_MIN")));
        assertEquals(Long.valueOf(Long.MAX_VALUE), Long.valueOf(jmsProducer.getLongProperty("JMS2_LONG_MAX")));
        assertEquals(Short.valueOf(Short.MIN_VALUE), Short.valueOf(jmsProducer.getShortProperty("JMS2_SHORT_MIN")));
        assertEquals(Short.valueOf(Short.MAX_VALUE), Short.valueOf(jmsProducer.getShortProperty("JMS2_SHORT_MAX")));
        assertEquals("Hello World", jmsProducer.getStringProperty("JMS2_STRING_VAL"));
    }

    private static void validateJMSProperties(javax.jms.Message message) throws JMSException {
        assertNotNull(message);
        assertEquals(Boolean.FALSE, Boolean.valueOf(message.getBooleanProperty("JMS2_BOOLEAN_MIN")));
        assertEquals(Boolean.TRUE, Boolean.valueOf(message.getBooleanProperty("JMS2_BOOLEAN_MAX")));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), Byte.valueOf(message.getByteProperty("JMS2_BYTE_MIN")));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), Byte.valueOf(message.getByteProperty("JMS2_BYTE_MAX")));
        assertEquals(Double.valueOf(Double.MIN_VALUE), Double.valueOf(message.getDoubleProperty("JMS2_DOUBLE_MIN")));
        assertEquals(Double.valueOf(Double.MAX_VALUE), Double.valueOf(message.getDoubleProperty("JMS2_DOUBLE_MAX")));
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), Integer.valueOf(message.getIntProperty("JMS2_INT_MIN")));
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), Integer.valueOf(message.getIntProperty("JMS2_INT_MAX")));
        assertEquals(Float.valueOf(Float.MIN_VALUE), Float.valueOf(message.getFloatProperty("JMS2_FLOAT_MIN")));
        assertEquals(Float.valueOf(Float.MAX_VALUE), Float.valueOf(message.getFloatProperty("JMS2_FLOAT_MAX")));
        assertEquals(Long.valueOf(Long.MIN_VALUE), Long.valueOf(message.getLongProperty("JMS2_LONG_MIN")));
        assertEquals(Long.valueOf(Long.MAX_VALUE), Long.valueOf(message.getLongProperty("JMS2_LONG_MAX")));
        assertEquals(Short.valueOf(Short.MIN_VALUE), Short.valueOf(message.getShortProperty("JMS2_SHORT_MIN")));
        assertEquals(Short.valueOf(Short.MAX_VALUE), Short.valueOf(message.getShortProperty("JMS2_SHORT_MAX")));
        assertEquals("Hello World", message.getStringProperty("JMS2_STRING_VAL"));
    }
}
