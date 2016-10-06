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

import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_DATA;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_NULL;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.JMS_DEST_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.JMS_REPLY_TO_TYPE_MSG_ANNOTATION;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.QUEUE_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TEMP_QUEUE_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TEMP_TOPIC_TYPE;
import static org.apache.activemq.transport.amqp.message.JMSMappingOutboundTransformer.TOPIC_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

public class JMSMappingOutboundTransformerTest {

    private final UUID TEST_OBJECT_VALUE = UUID.fromString("fee14b62-09e0-4ac6-a4c3-4206c630d844");
    private final String TEST_ADDRESS = "queue://testAddress";

    //----- no-body Message type tests ---------------------------------------//

    @Test
    public void testConvertMessageToAmqpMessageWithNoBody() throws Exception {
        ActiveMQMessage outbound = createMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNull(amqp.getBody());
    }

    @Test
    public void testConvertTextMessageToAmqpMessageWithNoBodyOriginalEncodingWasNull() throws Exception {
        ActiveMQTextMessage outbound = createTextMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_NULL);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNull(amqp.getBody());
    }

    //----- BytesMessage type tests ---------------------------------------//

    @Test
    public void testConvertEmptyBytesMessageToAmqpMessageWithDataBody() throws Exception {
        ActiveMQBytesMessage outbound = createBytesMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(0, ((Data) amqp.getBody()).getValue().getLength());
    }

    @Test
    public void testConvertUncompressedBytesMessageToAmqpMessageWithDataBody() throws Exception {
        byte[] expectedPayload = new byte[] { 8, 16, 24, 32 };
        ActiveMQBytesMessage outbound = createBytesMessage();
        outbound.writeBytes(expectedPayload);
        outbound.storeContent();
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(4, ((Data) amqp.getBody()).getValue().getLength());

        Binary amqpData = ((Data) amqp.getBody()).getValue();
        Binary inputData = new Binary(expectedPayload);

        assertTrue(inputData.equals(amqpData));
    }

    @Test
    public void testConvertCompressedBytesMessageToAmqpMessageWithDataBody() throws Exception {
        byte[] expectedPayload = new byte[] { 8, 16, 24, 32 };
        ActiveMQBytesMessage outbound = createBytesMessage(true);
        outbound.writeBytes(expectedPayload);
        outbound.storeContent();
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(4, ((Data) amqp.getBody()).getValue().getLength());

        Binary amqpData = ((Data) amqp.getBody()).getValue();
        Binary inputData = new Binary(expectedPayload);

        assertTrue(inputData.equals(amqpData));
    }

    @Test
    public void testConvertEmptyBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQBytesMessage outbound = createBytesMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(0, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
    }

    @Test
    public void testConvertUncompressedBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        byte[] expectedPayload = new byte[] { 8, 16, 24, 32 };
        ActiveMQBytesMessage outbound = createBytesMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.writeBytes(expectedPayload);
        outbound.storeContent();
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(4, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

        Binary amqpData = (Binary) ((AmqpValue) amqp.getBody()).getValue();
        Binary inputData = new Binary(expectedPayload);

        assertTrue(inputData.equals(amqpData));
    }

    @Test
    public void testConvertCompressedBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        byte[] expectedPayload = new byte[] { 8, 16, 24, 32 };
        ActiveMQBytesMessage outbound = createBytesMessage(true);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.writeBytes(expectedPayload);
        outbound.storeContent();
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
        assertEquals(4, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

        Binary amqpData = (Binary) ((AmqpValue) amqp.getBody()).getValue();
        Binary inputData = new Binary(expectedPayload);

        assertTrue(inputData.equals(amqpData));
    }

    //----- MapMessage type tests --------------------------------------------//

    @Test
    public void testConvertMapMessageToAmqpMessageWithNoBody() throws Exception {
        ActiveMQMapMessage outbound = createMapMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);
    }

    @Test
    public void testConvertMapMessageToAmqpMessageWithByteArrayValueInBody() throws Exception {
        final byte[] byteArray = new byte[] { 1, 2, 3, 4, 5 };

        ActiveMQMapMessage outbound = createMapMessage();
        outbound.setBytes("bytes", byteArray);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

        @SuppressWarnings("unchecked")
        Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

        assertEquals(1, amqpMap.size());
        Binary readByteArray = (Binary) amqpMap.get("bytes");
        assertNotNull(readByteArray);
    }

    @Test
    public void testConvertMapMessageToAmqpMessage() throws Exception {
        ActiveMQMapMessage outbound = createMapMessage();
        outbound.setString("property-1", "string");
        outbound.setInt("property-2", 1);
        outbound.setBoolean("property-3", true);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

        @SuppressWarnings("unchecked")
        Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

        assertEquals(3, amqpMap.size());
        assertTrue("string".equals(amqpMap.get("property-1")));
    }

    @Test
    public void testConvertCompressedMapMessageToAmqpMessage() throws Exception {
        ActiveMQMapMessage outbound = createMapMessage(true);
        outbound.setString("property-1", "string");
        outbound.setInt("property-2", 1);
        outbound.setBoolean("property-3", true);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

        @SuppressWarnings("unchecked")
        Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

        assertEquals(3, amqpMap.size());
        assertTrue("string".equals(amqpMap.get("property-1")));
    }

    //----- StreamMessage type tests -----------------------------------------//

    @Test
    public void testConvertStreamMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQStreamMessage outbound = createStreamMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof List);
    }

    @Test
    public void testConvertStreamMessageToAmqpMessageWithAmqpSequencey() throws Exception {
        ActiveMQStreamMessage outbound = createStreamMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpSequence);
        assertTrue(((AmqpSequence) amqp.getBody()).getValue() instanceof List);
    }

    @Test
    public void testConvertCompressedStreamMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQStreamMessage outbound = createStreamMessage(true);
        outbound.writeBoolean(false);
        outbound.writeString("test");
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> amqpList = (List<Object>) ((AmqpValue) amqp.getBody()).getValue();

        assertEquals(2, amqpList.size());
    }

    @Test
    public void testConvertCompressedStreamMessageToAmqpMessageWithAmqpSequencey() throws Exception {
        ActiveMQStreamMessage outbound = createStreamMessage(true);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
        outbound.writeBoolean(false);
        outbound.writeString("test");
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpSequence);
        assertTrue(((AmqpSequence) amqp.getBody()).getValue() instanceof List);

        @SuppressWarnings("unchecked")
        List<Object> amqpList = ((AmqpSequence) amqp.getBody()).getValue();

        assertEquals(2, amqpList.size());
    }

    //----- ObjectMessage type tests -----------------------------------------//

    @Test
    public void testConvertEmptyObjectMessageToAmqpMessageWithDataBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertEquals(0, ((Data) amqp.getBody()).getValue().getLength());
    }

    @Test
    public void testConvertEmptyObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertEquals(0, ((Data) amqp.getBody()).getValue().getLength());
    }

    @Test
    public void testConvertEmptyObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage();
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue)amqp.getBody()).getValue() instanceof Binary);
        assertEquals(0, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
    }

    @Test
    public void testConvertObjectMessageToAmqpMessageWithDataBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

        Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    @Test
    public void testConvertObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

        Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    @Test
    public void testConvertObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue)amqp.getBody()).getValue() instanceof Binary);
        assertFalse(0 == ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

        Object value = deserialize(((Binary) ((AmqpValue) amqp.getBody()).getValue()).getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    @Test
    public void testConvertCompressedObjectMessageToAmqpMessageWithDataBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

        Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    @Test
    public void testConvertCompressedObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

        Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    @Test
    public void testConvertCompressedObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
        ActiveMQObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertTrue(((AmqpValue)amqp.getBody()).getValue() instanceof Binary);
        assertFalse(0 == ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

        Object value = deserialize(((Binary) ((AmqpValue) amqp.getBody()).getValue()).getArray());
        assertNotNull(value);
        assertTrue(value instanceof UUID);
    }

    //----- TextMessage type tests -------------------------------------------//

    @Test
    public void testConvertTextMessageToAmqpMessageWithNoBody() throws Exception {
        ActiveMQTextMessage outbound = createTextMessage();
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertNull(((AmqpValue) amqp.getBody()).getValue());
    }

    @Test
    public void testConvertTextMessageCreatesBodyUsingOriginalEncodingWithDataSection() throws Exception {
        String contentString = "myTextMessageContent";
        ActiveMQTextMessage outbound = createTextMessage(contentString);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

        Binary data = ((Data) amqp.getBody()).getValue();
        String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
        assertEquals(contentString, contents);
    }

    @Test
    public void testConvertTextMessageContentNotStoredCreatesBodyUsingOriginalEncodingWithDataSection() throws Exception {
        String contentString = "myTextMessageContent";
        ActiveMQTextMessage outbound = createTextMessage(contentString);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

        Binary data = ((Data) amqp.getBody()).getValue();
        String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
        assertEquals(contentString, contents);
    }

    @Test
    public void testConvertTextMessageCreatesAmqpValueStringBody() throws Exception {
        String contentString = "myTextMessageContent";
        ActiveMQTextMessage outbound = createTextMessage(contentString);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
    }

    @Test
    public void testConvertTextMessageContentNotStoredCreatesAmqpValueStringBody() throws Exception {
        String contentString = "myTextMessageContent";
        ActiveMQTextMessage outbound = createTextMessage(contentString);
        outbound.onSend();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof AmqpValue);
        assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
    }

    @Test
    public void testConvertCompressedTextMessageCreatesDataSectionBody() throws Exception {
        String contentString = "myTextMessageContent";
        ActiveMQTextMessage outbound = createTextMessage(contentString, true);
        outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
        outbound.onSend();
        outbound.storeContent();

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(outbound);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        assertNotNull(amqp.getBody());
        assertTrue(amqp.getBody() instanceof Data);
        assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

        Binary data = ((Data) amqp.getBody()).getValue();
        String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
        assertEquals(contentString, contents);
    }

    //----- Test JMSDestination Handling -------------------------------------//

    @Test
    public void testConvertMessageWithJMSDestinationNull() throws Exception {
        doTestConvertMessageWithJMSDestination(null, null);
    }

    @Test
    public void testConvertMessageWithJMSDestinationQueue() throws Exception {
        doTestConvertMessageWithJMSDestination(createMockDestination(QUEUE_TYPE), QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryQueue() throws Exception {
        doTestConvertMessageWithJMSDestination(createMockDestination(TEMP_QUEUE_TYPE), TEMP_QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTopic() throws Exception {
        doTestConvertMessageWithJMSDestination(createMockDestination(TOPIC_TYPE), TOPIC_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSDestinationTemporaryTopic() throws Exception {
        doTestConvertMessageWithJMSDestination(createMockDestination(TEMP_TOPIC_TYPE), TEMP_TOPIC_TYPE);
    }

    private void doTestConvertMessageWithJMSDestination(ActiveMQDestination jmsDestination, Object expectedAnnotationValue) throws Exception {
        ActiveMQTextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getDestination()).thenReturn(jmsDestination);

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(mockTextMessage);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(JMS_DEST_TYPE_MSG_ANNOTATION);
            assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
        } else if (expectedAnnotationValue != null) {
            fail("Expected annotation value, but there were no annotations");
        }

        if (jmsDestination != null) {
            assertEquals("Unexpected 'to' address", jmsDestination.getQualifiedName(), amqp.getAddress());
        }
    }

    //----- Test JMSReplyTo Handling -----------------------------------------//

    @Test
    public void testConvertMessageWithJMSReplyToNull() throws Exception {
        doTestConvertMessageWithJMSReplyTo(null, null);
    }

    @Test
    public void testConvertMessageWithJMSReplyToQueue() throws Exception {
        doTestConvertMessageWithJMSReplyTo(createMockDestination(QUEUE_TYPE), QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryQueue() throws Exception {
        doTestConvertMessageWithJMSReplyTo(createMockDestination(TEMP_QUEUE_TYPE), TEMP_QUEUE_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTopic() throws Exception {
        doTestConvertMessageWithJMSReplyTo(createMockDestination(TOPIC_TYPE), TOPIC_TYPE);
    }

    @Test
    public void testConvertMessageWithJMSReplyToTemporaryTopic() throws Exception {
        doTestConvertMessageWithJMSReplyTo(createMockDestination(TEMP_TOPIC_TYPE), TEMP_TOPIC_TYPE);
    }

    private void doTestConvertMessageWithJMSReplyTo(ActiveMQDestination jmsReplyTo, Object expectedAnnotationValue) throws Exception {
        ActiveMQTextMessage mockTextMessage = createMockTextMessage();
        Mockito.when(mockTextMessage.getText()).thenReturn("myTextMessageContent");
        Mockito.when(mockTextMessage.getReplyTo()).thenReturn(jmsReplyTo);

        JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

        EncodedMessage encoded = transformer.transform(mockTextMessage);
        assertNotNull(encoded);

        Message amqp = encoded.decode();

        MessageAnnotations ma = amqp.getMessageAnnotations();
        Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
        if (maMap != null) {
            Object actualValue = maMap.get(JMS_REPLY_TO_TYPE_MSG_ANNOTATION);
            assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
        } else if (expectedAnnotationValue != null) {
            fail("Expected annotation value, but there were no annotations");
        }

        if (jmsReplyTo != null) {
            assertEquals("Unexpected 'reply-to' address", jmsReplyTo.getQualifiedName(), amqp.getReplyTo());
        }
    }

    //----- Utility Methods used for this Test -------------------------------//

    private ActiveMQTextMessage createMockTextMessage() throws Exception {
        ActiveMQTextMessage mockTextMessage = Mockito.mock(ActiveMQTextMessage.class);
        Mockito.when(mockTextMessage.getPropertyNames()).thenReturn(Collections.enumeration(Collections.emptySet()));
        Mockito.when(mockTextMessage.getPriority()).thenReturn((byte) Message.DEFAULT_PRIORITY);

        return mockTextMessage;
    }

    private ActiveMQDestination createMockDestination(byte destType) {
        ActiveMQDestination mockDestination = null;
        switch (destType) {
            case QUEUE_TYPE:
                mockDestination = Mockito.mock(ActiveMQQueue.class);
                Mockito.when(mockDestination.getQualifiedName()).thenReturn("queue://" + TEST_ADDRESS);
                Mockito.when(mockDestination.isQueue()).thenReturn(true);
                break;
            case TOPIC_TYPE:
                mockDestination = Mockito.mock(ActiveMQTopic.class);
                Mockito.when(mockDestination.getQualifiedName()).thenReturn("topic://" + TEST_ADDRESS);
                Mockito.when(mockDestination.isTopic()).thenReturn(true);
                break;
            case TEMP_QUEUE_TYPE:
                mockDestination = Mockito.mock(ActiveMQTempQueue.class);
                Mockito.when(mockDestination.getQualifiedName()).thenReturn("tempQueue://" + TEST_ADDRESS);
                Mockito.when(mockDestination.isQueue()).thenReturn(true);
                Mockito.when(mockDestination.isTemporary()).thenReturn(true);
                break;
            case TEMP_TOPIC_TYPE:
                mockDestination = Mockito.mock(ActiveMQTempTopic.class);
                Mockito.when(mockDestination.getQualifiedName()).thenReturn("tempTopic://" + TEST_ADDRESS);
                Mockito.when(mockDestination.isTopic()).thenReturn(true);
                Mockito.when(mockDestination.isTemporary()).thenReturn(true);
                break;
            default:
                throw new IllegalArgumentException("Invliad Destination Type given/");
        }

        return mockDestination;
    }

    private ActiveMQMessage createMessage() {
        return new ActiveMQMessage();
    }

    private ActiveMQBytesMessage createBytesMessage() {
        return createBytesMessage(false);
    }

    private ActiveMQBytesMessage createBytesMessage(boolean compression) {
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();

        if (compression) {
            ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);
            Mockito.when(connection.isUseCompression()).thenReturn(true);
            message.setConnection(connection);
        }

        return message;
    }

    private ActiveMQMapMessage createMapMessage() {
        return createMapMessage(false);
    }

    private ActiveMQMapMessage createMapMessage(boolean compression) {
        ActiveMQMapMessage message = new ActiveMQMapMessage();

        if (compression) {
            ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);
            Mockito.when(connection.isUseCompression()).thenReturn(true);
            message.setConnection(connection);
        }

        return message;
    }

    private ActiveMQStreamMessage createStreamMessage() {
        return createStreamMessage(false);
    }

    private ActiveMQStreamMessage createStreamMessage(boolean compression) {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();

        if (compression) {
            ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);
            Mockito.when(connection.isUseCompression()).thenReturn(true);
            message.setConnection(connection);
        }

        return message;
    }

    private ActiveMQObjectMessage createObjectMessage() {
        return createObjectMessage(null);
    }

    private ActiveMQObjectMessage createObjectMessage(Serializable payload) {
        return createObjectMessage(payload, false);
    }

    private ActiveMQObjectMessage createObjectMessage(Serializable payload, boolean compression) {
        ActiveMQObjectMessage result = new ActiveMQObjectMessage();

        if (compression) {
            ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);
            Mockito.when(connection.isUseCompression()).thenReturn(true);
            result.setConnection(connection);
        }

        try {
            result.setObject(payload);
        } catch (JMSException ex) {
            throw new AssertionError("Should not fail to setObject in this test");
        }

        result = Mockito.spy(result);

        try {
            Mockito.doThrow(new AssertionError("invalid setObject")).when(result).setObject(Mockito.any(Serializable.class));
            Mockito.doThrow(new AssertionError("invalid getObject")).when(result).getObject();
        } catch (JMSException e) {
        }

        return result;
    }

    private ActiveMQTextMessage createTextMessage() {
        return createTextMessage(null);
    }

    private ActiveMQTextMessage createTextMessage(String text) {
        return createTextMessage(text, false);
    }

    private ActiveMQTextMessage createTextMessage(String text, boolean compression) {
        ActiveMQTextMessage result = new ActiveMQTextMessage();

        if (compression) {
            ActiveMQConnection connection = Mockito.mock(ActiveMQConnection.class);
            Mockito.when(connection.isUseCompression()).thenReturn(true);
            result.setConnection(connection);
        }

        try {
            result.setText(text);
        } catch (JMSException e) {
        }

        return result;
    }

    private Object deserialize(byte[] payload) throws Exception {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload);
             ObjectInputStream ois = new ObjectInputStream(bis);) {

            return ois.readObject();
        }
    }
}
