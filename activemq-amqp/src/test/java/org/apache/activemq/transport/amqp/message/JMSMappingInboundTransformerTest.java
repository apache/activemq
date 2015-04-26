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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
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

public class JMSMappingInboundTransformerTest {

    @Test
    public void testTransformMessageWithAmqpValueStringCreatesTextMessage() throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        JMSVendor mockVendor = createMockVendor(mockTextMessage);
        JMSMappingInboundTransformer transformer = new JMSMappingInboundTransformer(mockVendor);

        String contentString = "myTextMessageContent";
        Message amqp = Message.Factory.create();
        amqp.setBody(new AmqpValue(contentString));

        EncodedMessage em = encodeMessage(amqp);

        javax.jms.Message jmsMessage = transformer.transform(em);

        assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);
        Mockito.verify(mockTextMessage).setText(contentString);
        assertSame("Expected provided mock message, got a different one", mockTextMessage, jmsMessage);
    }

    // ======= JMSDestination Handling =========

    @Test
    public void testTransformWithNoToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithToTypeDestinationTypeAnnotationTestImpl(null, Destination.class);
    }

    @Test
    public void testTransformWithQueueStringToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithToTypeDestinationTypeAnnotationTestImpl("queue", Queue.class);
    }

    @Test
    public void testTransformWithTemporaryQueueStringToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithToTypeDestinationTypeAnnotationTestImpl("queue,temporary", TemporaryQueue.class);
    }

    @Test
    public void testTransformWithTopicStringToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithToTypeDestinationTypeAnnotationTestImpl("topic", Topic.class);
    }

    @Test
    public void testTransformWithTemporaryTopicStringToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithToTypeDestinationTypeAnnotationTestImpl("topic,temporary", TemporaryTopic.class);
    }

    private void doTransformWithToTypeDestinationTypeAnnotationTestImpl(Object toTypeAnnotationValue, Class<? extends Destination> expectedClass) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        JMSVendor mockVendor = createMockVendor(mockTextMessage);
        JMSMappingInboundTransformer transformer = new JMSMappingInboundTransformer(mockVendor);

        String toAddress = "toAddress";
        Message amqp = Message.Factory.create();
        amqp.setBody(new AmqpValue("myTextMessageContent"));
        amqp.setAddress(toAddress);
        if (toTypeAnnotationValue != null) {
            Map<Symbol, Object> map = new HashMap<Symbol, Object>();
            map.put(Symbol.valueOf("x-opt-to-type"), toTypeAnnotationValue);
            MessageAnnotations ma = new MessageAnnotations(map);
            amqp.setMessageAnnotations(ma);
        }

        EncodedMessage em = encodeMessage(amqp);

        javax.jms.Message jmsMessage = transformer.transform(em);
        assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);

        // Verify that createDestination was called with the provided 'to'
        // address and 'Destination' class
        // TODO - No need to really test this bit ?
        // Mockito.verify(mockVendor).createDestination(toAddress, expectedClass);
    }

    // ======= JMSReplyTo Handling =========

    @Test
    public void testTransformWithNoReplyToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl(null, Destination.class);
    }

    @Test
    public void testTransformWithQueueStringReplyToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("queue", Queue.class);
    }

    @Test
    public void testTransformWithTemporaryQueueStringReplyToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("queue,temporary", TemporaryQueue.class);
    }

    @Test
    public void testTransformWithTopicStringReplyToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("topic", Topic.class);
    }

    @Test
    public void testTransformWithTemporaryTopicStringReplyToTypeDestinationTypeAnnotation() throws Exception {
        doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("topic,temporary", TemporaryTopic.class);
    }

    private void doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl(Object replyToTypeAnnotationValue, Class<? extends Destination> expectedClass) throws Exception {
        TextMessage mockTextMessage = createMockTextMessage();
        JMSVendor mockVendor = createMockVendor(mockTextMessage);
        JMSMappingInboundTransformer transformer = new JMSMappingInboundTransformer(mockVendor);

        String replyToAddress = "replyToAddress";
        Message amqp = Message.Factory.create();
        amqp.setBody(new AmqpValue("myTextMessageContent"));
        amqp.setReplyTo(replyToAddress);
        if (replyToTypeAnnotationValue != null) {
            Map<Symbol, Object> map = new HashMap<Symbol, Object>();
            map.put(Symbol.valueOf("x-opt-reply-type"), replyToTypeAnnotationValue);
            MessageAnnotations ma = new MessageAnnotations(map);
            amqp.setMessageAnnotations(ma);
        }

        EncodedMessage em = encodeMessage(amqp);

        javax.jms.Message jmsMessage = transformer.transform(em);
        assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);

        // Verify that createDestination was called with the provided 'replyTo'
        // address and 'Destination' class
        // TODO - No need to really test this bit ?
        // Mockito.verify(mockVendor).createDestination(replyToAddress, expectedClass);
    }

    // ======= Utility Methods =========

    private TextMessage createMockTextMessage() {
        TextMessage mockTextMessage = Mockito.mock(TextMessage.class);

        return mockTextMessage;
    }

    private JMSVendor createMockVendor(TextMessage mockTextMessage) {
        JMSVendor mockVendor = Mockito.mock(JMSVendor.class);
        Mockito.when(mockVendor.createTextMessage()).thenReturn(mockTextMessage);

        return mockVendor;
    }

    private EncodedMessage encodeMessage(Message message) {
        byte[] encodeBuffer = new byte[1024 * 8];
        int encodedSize;
        while (true) {
            try {
                encodedSize = message.encode(encodeBuffer, 0, encodeBuffer.length);
                break;
            } catch (java.nio.BufferOverflowException e) {
                encodeBuffer = new byte[encodeBuffer.length * 2];
            }
        }

        long messageFormat = 0;
        return new EncodedMessage(messageFormat, encodeBuffer, 0, encodedSize);
    }
}
