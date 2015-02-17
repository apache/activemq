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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    @Test
    public void testDefaultsTolStringDestinationTypeAnnotationValues() {
        JMSVendor mockVendor = createMockVendor();
        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);

        assertFalse("Expected the older string style annotation values to be used by default", transformer.isUseByteDestinationTypeAnnotations());
    }

    @Test
    public void testSetGetIsUseByteDestinationTypeAnnotations() {
        JMSVendor mockVendor = createMockVendor();
        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);

        assertFalse(transformer.isUseByteDestinationTypeAnnotations());
        transformer.setUseByteDestinationTypeAnnotations(true);
        assertTrue(transformer.isUseByteDestinationTypeAnnotations());
    }

    // ======= JMSDestination Handling =========

    // --- String type annotation ---
    @Test
    public void testConvertMessageWithJMSDestinationNull() throws Exception {
        doTestConvertMessageWithJMSDestination(null, null, false);
    }

    @Test
    public void testConvertMessageWithJMSDestinationQueue() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSDestination(mockDest, "queue", false);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryQueue() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSDestination(mockDest, "temporary,queue", false);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTopic() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSDestination(mockDest, "topic", false);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryTopic() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSDestination(mockDest, "temporary,topic", false);
    }

    // --- byte type annotation ---

    @Test
    public void testConvertMessageWithJMSDestinationNullUsingByteAnnotation() throws Exception {
        doTestConvertMessageWithJMSDestination(null, null, true);
    }

    @Test
    public void testConvertMessageWithJMSDestinationQueueUsingByteAnnotation() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSDestination(mockDest, JMSVendor.QUEUE_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryQueueUsingByteAnnotation() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSDestination(mockDest, JMSVendor.TEMP_QUEUE_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTopicUsingByteAnnotation() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSDestination(mockDest, JMSVendor.TOPIC_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryTopicUsingByteAnnotation() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSDestination(mockDest, JMSVendor.TEMP_TOPIC_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSDestinationUnkownUsingByteAnnotation() throws Exception {
        Destination mockDest = Mockito.mock(Destination.class);

        doTestConvertMessageWithJMSDestination(mockDest, JMSVendor.QUEUE_TYPE, true);
    }

    private void doTestConvertMessageWithJMSDestination(Destination jmsDestination, Object expectedAnnotationValue, boolean byteType) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getJMSDestination()).thenReturn(jmsDestination);
        JMSVendor mockVendor = createMockVendor();
        String toAddress = "someToAddress";
        if (jmsDestination != null) {
            Mockito.when(mockVendor.toAddress(Mockito.any(Destination.class))).thenReturn(toAddress);
        }

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);
        if (byteType) {
            transformer.setUseByteDestinationTypeAnnotations(true);
        }

        Message amqp = transformer.convert(mockTextMessage);

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(Symbol.valueOf("x-opt-to-type"));
            assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
        } else if (expectedAnnotationValue != null) {
            fail("Expected annotation value, but there were no annotations");
        }

        if (jmsDestination != null) {
            assertEquals("Unexpected 'to' address", toAddress, amqp.getAddress());
        }
    }

    // ======= JMSReplyTo Handling =========

    // --- String type annotation ---
    @Test
    public void testConvertMessageWithJMSReplyToNull() throws Exception {
        doTestConvertMessageWithJMSReplyTo(null, null, false);
    }

    @Test
    public void testConvertMessageWithJMSReplyToQueue() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, "queue", false);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryQueue() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, "temporary,queue", false);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTopic() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, "topic", false);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryTopic() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, "temporary,topic", false);
    }

    // --- byte type annotation ---
    @Test
    public void testConvertMessageWithJMSReplyToNullUsingByteAnnotation() throws Exception {
        doTestConvertMessageWithJMSReplyTo(null, null, true);
    }

    @Test
    public void testConvertMessageWithJMSReplyToQueueUsingByteAnnotation() throws Exception {
        Queue mockDest = Mockito.mock(Queue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, JMSVendor.QUEUE_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryQueueUsingByteAnnotation() throws Exception {
        TemporaryQueue mockDest = Mockito.mock(TemporaryQueue.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, JMSVendor.TEMP_QUEUE_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTopicUsingByteAnnotation() throws Exception {
        Topic mockDest = Mockito.mock(Topic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, JMSVendor.TOPIC_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryTopicUsingByteAnnotation() throws Exception {
        TemporaryTopic mockDest = Mockito.mock(TemporaryTopic.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, JMSVendor.TEMP_TOPIC_TYPE, true);
    }

    @Test
    public void testConvertMessageWithJMSReplyToUnkownUsingByteAnnotation() throws Exception {
        Destination mockDest = Mockito.mock(Destination.class);

        doTestConvertMessageWithJMSReplyTo(mockDest, JMSVendor.QUEUE_TYPE, true);
    }

    private void doTestConvertMessageWithJMSReplyTo(Destination jmsReplyTo, Object expectedAnnotationValue, boolean byteType) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getJMSReplyTo()).thenReturn(jmsReplyTo);
        JMSVendor mockVendor = createMockVendor();
        String replyToAddress = "someReplyToAddress";
        if (jmsReplyTo != null) {
            Mockito.when(mockVendor.toAddress(Mockito.any(Destination.class))).thenReturn(replyToAddress);
        }

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer(mockVendor);
        if (byteType) {
            transformer.setUseByteDestinationTypeAnnotations(true);
        }

        Message amqp = transformer.convert(mockTextMessage);

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(Symbol.valueOf("x-opt-reply-type"));
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
