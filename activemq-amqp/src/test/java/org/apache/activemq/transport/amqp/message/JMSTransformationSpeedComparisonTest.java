/*
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
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.amqp.JMSInteroperabilityTest;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.codec.CompositeWritableBuffer;
import org.apache.qpid.proton.codec.DroppingWritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some simple performance tests for the Message Transformers.
 */
@Ignore("Turn on to profile.")
@RunWith(Parameterized.class)
public class JMSTransformationSpeedComparisonTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSInteroperabilityTest.class);

    @Rule
    public TestName test = new TestName();

    private final String transformer;

    private final int WARM_CYCLES = 50;
    private final int PROFILE_CYCLES = 1000000;

    public JMSTransformationSpeedComparisonTest(String transformer) {
        this.transformer = transformer;
    }

    @Parameters(name="Transformer->{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"jms"},
                {"native"},
                {"raw"},
            });
    }

    private InboundTransformer getInboundTransformer() {
        switch (transformer) {
            case "raw":
                return new AMQPRawInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            case "native":
                return new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            default:
                return new JMSMappingInboundTransformer(ActiveMQJMSVendor.INSTANCE);
        }
    }

    private OutboundTransformer getOutboundTransformer() {
        switch (transformer) {
            case "raw":
            case "native":
                return new AMQPNativeOutboundTransformer(ActiveMQJMSVendor.INSTANCE);
            default:
                return new JMSMappingOutboundTransformer(ActiveMQJMSVendor.INSTANCE);
        }
    }

    @Test
    public void testBodyOnlyMessage() throws Exception {

        Message message = Proton.message();

        message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

        EncodedMessage encoded = encode(message);
        InboundTransformer inboundTransformer = getInboundTransformer();
        OutboundTransformer outboundTransformer = getOutboundTransformer();

        // Warm up
        for (int i = 0; i < WARM_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }

        long totalDuration = 0;

        long startTime = System.nanoTime();
        for (int i = 0; i < PROFILE_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }
        totalDuration += System.nanoTime() - startTime;

        LOG.info("[{}] Total time for {} cycles of transforms = {} ms  -> [{}]",
            transformer, PROFILE_CYCLES, TimeUnit.NANOSECONDS.toMillis(totalDuration), test.getMethodName());
    }

    @Test
    public void testMessageWithNoPropertiesOrAnnotations() throws Exception {

        Message message = Proton.message();

        message.setAddress("queue://test-queue");
        message.setDeliveryCount(1);
        message.setCreationTime(System.currentTimeMillis());
        message.setContentType("text/plain");
        message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

        EncodedMessage encoded = encode(message);
        InboundTransformer inboundTransformer = getInboundTransformer();
        OutboundTransformer outboundTransformer = getOutboundTransformer();

        // Warm up
        for (int i = 0; i < WARM_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }

        long totalDuration = 0;

        long startTime = System.nanoTime();
        for (int i = 0; i < PROFILE_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }
        totalDuration += System.nanoTime() - startTime;

        LOG.info("[{}] Total time for {} cycles of transforms = {} ms  -> [{}]",
            transformer, PROFILE_CYCLES, TimeUnit.NANOSECONDS.toMillis(totalDuration), test.getMethodName());
    }

    @Test
    public void testTypicalQpidJMSMessage() throws Exception {

        EncodedMessage encoded = encode(createTypicalQpidJMSMessage());
        InboundTransformer inboundTransformer = getInboundTransformer();
        OutboundTransformer outboundTransformer = getOutboundTransformer();

        // Warm up
        for (int i = 0; i < WARM_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }

        long totalDuration = 0;

        long startTime = System.nanoTime();
        for (int i = 0; i < PROFILE_CYCLES; ++i) {
            ActiveMQMessage intermediate = (ActiveMQMessage) inboundTransformer.transform(encoded);
            intermediate.onSend();
            outboundTransformer.transform(intermediate);
        }
        totalDuration += System.nanoTime() - startTime;

        LOG.info("[{}] Total time for {} cycles of transforms = {} ms  -> [{}]",
            transformer, PROFILE_CYCLES, TimeUnit.NANOSECONDS.toMillis(totalDuration), test.getMethodName());
    }

    @Test
    public void testTypicalQpidJMSMessageInBoundOnly() throws Exception {

        EncodedMessage encoded = encode(createTypicalQpidJMSMessage());
        InboundTransformer inboundTransformer = getInboundTransformer();

        // Warm up
        for (int i = 0; i < WARM_CYCLES; ++i) {
            inboundTransformer.transform(encoded);
        }

        long totalDuration = 0;

        long startTime = System.nanoTime();
        for (int i = 0; i < PROFILE_CYCLES; ++i) {
            inboundTransformer.transform(encoded);
        }

        totalDuration += System.nanoTime() - startTime;

        LOG.info("[{}] Total time for {} cycles of transforms = {} ms  -> [{}]",
            transformer, PROFILE_CYCLES, TimeUnit.NANOSECONDS.toMillis(totalDuration), test.getMethodName());
    }

    @Test
    public void testTypicalQpidJMSMessageOutBoundOnly() throws Exception {

        EncodedMessage encoded = encode(createTypicalQpidJMSMessage());
        InboundTransformer inboundTransformer = getInboundTransformer();
        OutboundTransformer outboundTransformer = getOutboundTransformer();

        ActiveMQMessage outbound = (ActiveMQMessage) inboundTransformer.transform(encoded);
        outbound.onSend();

        // Warm up
        for (int i = 0; i < WARM_CYCLES; ++i) {
            outboundTransformer.transform(outbound);
        }

        long totalDuration = 0;

        long startTime = System.nanoTime();
        for (int i = 0; i < PROFILE_CYCLES; ++i) {
            outboundTransformer.transform(outbound);
        }

        totalDuration += System.nanoTime() - startTime;

        LOG.info("[{}] Total time for {} cycles of transforms = {} ms  -> [{}]",
            transformer, PROFILE_CYCLES, TimeUnit.NANOSECONDS.toMillis(totalDuration), test.getMethodName());
    }

    @Ignore
    @Test
    public void testEncodeDecodeIsWorking() throws Exception {
        Message incomingMessage = createTypicalQpidJMSMessage();
        EncodedMessage encoded = encode(incomingMessage);
        InboundTransformer inboundTransformer = getInboundTransformer();
        OutboundTransformer outboundTransformer = getOutboundTransformer();

        ActiveMQMessage outbound = (ActiveMQMessage) inboundTransformer.transform(encoded);
        outbound.onSend();
        Message outboudMessage = outboundTransformer.transform(outbound).decode();

        // Test that message details are equal
        assertEquals(incomingMessage.getAddress(), outboudMessage.getAddress());
        assertEquals(incomingMessage.getDeliveryCount(), outboudMessage.getDeliveryCount());
        assertEquals(incomingMessage.getCreationTime(), outboudMessage.getCreationTime());
        assertEquals(incomingMessage.getContentType(), outboudMessage.getContentType());

        // Test Message annotations
        ApplicationProperties incomingApplicationProperties = incomingMessage.getApplicationProperties();
        ApplicationProperties outgoingApplicationProperties = outboudMessage.getApplicationProperties();

        assertEquals(incomingApplicationProperties.getValue(), outgoingApplicationProperties.getValue());

        // Test Message properties
        MessageAnnotations incomingMessageAnnotations = incomingMessage.getMessageAnnotations();
        MessageAnnotations outgoingMessageAnnotations = outboudMessage.getMessageAnnotations();

        assertEquals(incomingMessageAnnotations.getValue(), outgoingMessageAnnotations.getValue());

        // Test that bodies are equal
        assertTrue(incomingMessage.getBody() instanceof AmqpValue);
        assertTrue(outboudMessage.getBody() instanceof AmqpValue);

        AmqpValue incomingBody = (AmqpValue) incomingMessage.getBody();
        AmqpValue outgoingBody = (AmqpValue) outboudMessage.getBody();

        assertTrue(incomingBody.getValue() instanceof String);
        assertTrue(outgoingBody.getValue() instanceof String);

        assertEquals(incomingBody.getValue(), outgoingBody.getValue());
    }

    private Message createTypicalQpidJMSMessage() {
        Map<String, Object> applicationProperties = new HashMap<String, Object>();
        Map<Symbol, Object> messageAnnotations = new HashMap<Symbol, Object>();

        applicationProperties.put("property-1", "string");
        applicationProperties.put("property-2", 512);
        applicationProperties.put("property-3", true);

        messageAnnotations.put(Symbol.valueOf("x-opt-jms-msg-type"), 0);
        messageAnnotations.put(Symbol.valueOf("x-opt-jms-dest"), 0);

        Message message = Proton.message();

        message.setAddress("queue://test-queue");
        message.setDeliveryCount(1);
        message.setApplicationProperties(new ApplicationProperties(applicationProperties));
        message.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
        message.setCreationTime(System.currentTimeMillis());
        message.setContentType("text/plain");
        message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

        return message;
    }

    private EncodedMessage encode(Message message) {
        ProtonJMessage amqp = (ProtonJMessage) message;

        ByteBuffer buffer = ByteBuffer.wrap(new byte[1024 * 4]);
        final DroppingWritableBuffer overflow = new DroppingWritableBuffer();
        int c = amqp.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
        if (overflow.position() > 0) {
            buffer = ByteBuffer.wrap(new byte[1024 * 4 + overflow.position()]);
            c = amqp.encode(new WritableBuffer.ByteBufferWrapper(buffer));
        }

        return new EncodedMessage(1, buffer.array(), 0, c);
    }
}
