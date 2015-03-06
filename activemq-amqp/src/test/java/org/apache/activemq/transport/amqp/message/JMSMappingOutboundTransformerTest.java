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
package org.apache.activemq.transport.amqp.message;

import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.JMS_DEST_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.JMS_REPLY_TO_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.QUEUE_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TEMP_QUEUE_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TEMP_TOPIC_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TOPIC_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

public class JMSMappingOutboundTransformerTest {

    @Test
    public void testConvertMessageWithTextMessageCreatesAmqpValueStringBody() throws Exception {
        String contentString = "myTextMessageContent";
        TextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn(contentString);
        JMSVendor mockVendor = createMockVendor();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);

        Message amqp = transformer.convert(mockTextMessage);

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
    }

    // ======= JMSDestination Handling =========

    @Test
    public void testConvertMessageWithJMSDestinationNull() throws Exception {
        doTestConvertMessageWithJMSDestination(null, null);
    }

    @Test
    public void testConvertMessageWithJMSDestinationQueue() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSDestination(mockDest, QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryQueue() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSDestination(mockDest, TEMP_QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTopic() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSDestination(mockDest, TOPIC_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryTopic() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSDestination(mockDest, TEMP_TOPIC_TYPE);
    }

    private void doTestConvertMessageWithJMSDestination(Destination jmsDestination, Object expectedAnnotationValue) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getJMSDestination()).thenReturn(jmsDestination);
        JMSVendor mockVendor = createMockVendor();
        String toAddress = "someToAddress";
        if (jmsDestination != null) {
            Mockito.when(mockVendor.toAddress(Mockito.any(Destination.class))).thenReturn(toAddress);
        }

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);

        Message amqp = transformer.convert(mockTextMessage);

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(JMS_DEST_TYPE_MSG_ANNOTATION);
            assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
        } else if (expectedAnnotationValue != null) {
            fail("Expected annotation value, but there were no annotations");
        }

        if (jmsDestination != null) {
            assertEquals("Unexpected 'to' address", toAddress, amqp.getAddress());
        }
    }

    // ======= JMSReplyTo Handling =========

    @Test
    public void testConvertMessageWithJMSReplyToNull() throws Exception {
        doTestConvertMessageWithJMSReplyTo(null, null);
    }

    @Test
    public void testConvertMessageWithJMSReplyToQueue() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryQueue() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, TEMP_QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTopic() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, TOPIC_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryTopic() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, TEMP_TOPIC_TYPE);
    }

    private void doTestConvertMessageWithJMSReplyTo(Destination jmsReplyTo, Object expectedAnnotationValue) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getJMSReplyTo()).thenReturn(jmsReplyTo);
        JMSVendor mockVendor = createMockVendor();
        String replyToAddress = "someReplyToAddress";
        if (jmsReplyTo != null) {
            Mockito.when(mockVendor.toAddress(Mockito.any(Destination.class))).thenReturn(replyToAddress);
        }

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);

        Message amqp = transformer.convert(mockTextMessage);

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(JMS_REPLY_TO_TYPE_MSG_ANNOTATION);
            assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
        } else if (expectedAnnotationValue != null) {
            fail("Expected annotation value, but there were no annotations");
        }

        if (jmsReplyTo != null) {
            assertEquals("Unexpected 'reply-to' address", replyToAddress, amqp.getReplyTo());
        }
    }

    // ======= Utility Methods =========

    private TextMessage createMockTextMessage() throws Exception {
        TextMessage mockTextMessage = Mockito.mock(TextMessage.class);
        Mockito.when(mockTextMessage.getPropertyNames()).thenReturn(Collections.enumeration(Collections.emptySet()));

        return mockTextMessage;
    }

    private JMSVendor createMockVendor() {
        JMSVendor mockVendor = Mockito.mock(JMSVendor.class);

        return mockVendor;
    }
}
