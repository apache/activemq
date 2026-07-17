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
package org.apache.activemq.openwire.v13;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collection;

import jakarta.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Round-trips every concrete v13 message marshaller in both tight and loose
 * encodings, verifying the {@code deliveryTime} field added in v13 survives
 * the wire and that type-specific fields following it stay correctly aligned.
 */
@RunWith(Parameterized.class)
public class MessageMarshallerRoundTripTest {

    private static final long DELIVERY_TIME = 1234567890123L;

    private final String label;
    private final Message message;

    public MessageMarshallerRoundTripTest(String label, Message message) {
        this.label = label;
        this.message = message;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> messageTypes() throws Exception {
        ActiveMQTextMessage text = new ActiveMQTextMessage();
        text.setText("hello world");

        ActiveMQBytesMessage bytes = new ActiveMQBytesMessage();
        bytes.writeBytes(new byte[] {1, 2, 3, 4});
        bytes.storeContent();

        ActiveMQMapMessage map = new ActiveMQMapMessage();
        map.setString("key", "value");
        map.storeContent();

        ActiveMQStreamMessage stream = new ActiveMQStreamMessage();
        stream.writeString("streamed");
        stream.storeContent();

        ActiveMQObjectMessage object = new ActiveMQObjectMessage();
        object.setObject("payload");
        object.storeContent();

        ActiveMQBlobMessage blob = new ActiveMQBlobMessage();
        blob.setRemoteBlobUrl("http://localhost/blob/1");
        blob.setMimeType("application/octet-stream");
        blob.setDeletedByBroker(true);

        return Arrays.asList(new Object[][] {
            {"ActiveMQMessage", new ActiveMQMessage()},
            {"ActiveMQTextMessage", text},
            {"ActiveMQBytesMessage", bytes},
            {"ActiveMQMapMessage", map},
            {"ActiveMQStreamMessage", stream},
            {"ActiveMQObjectMessage", object},
            {"ActiveMQBlobMessage", blob},
        });
    }

    @Test
    public void testDeliveryTimeSurvivesTightRoundTrip() throws Exception {
        assertDeliveryTimeRoundTrip(true);
    }

    @Test
    public void testDeliveryTimeSurvivesLooseRoundTrip() throws Exception {
        assertDeliveryTimeRoundTrip(false);
    }

    private void assertDeliveryTimeRoundTrip(boolean tightEncoding) throws Exception {
        OpenWireFormat wireFormat = wireFormat(tightEncoding);

        Message original = prepare(message);
        original.setDeliveryTime(DELIVERY_TIME);

        Message restored = (Message) unmarshal(wireFormat, marshal(wireFormat, original));

        assertEquals(label + " must round-trip as the same type",
                original.getClass(), restored.getClass());
        assertEquals(label + " deliveryTime must survive the v13 wire",
                DELIVERY_TIME, restored.getDeliveryTime());
        assertEquals(original.getMessageId(), restored.getMessageId());
        assertEquals(original.getDestination(), restored.getDestination());
    }

    @Test
    public void testDeliveryTimeDefaultsToZero() throws Exception {
        OpenWireFormat wireFormat = wireFormat(true);

        Message original = prepare(message);
        Message restored = (Message) unmarshal(wireFormat, marshal(wireFormat, original));

        assertEquals(0L, restored.getDeliveryTime());
    }

    /**
     * The v13 field must not reach the v12 wire. This pins the version gate --
     * a broker left at the default store version neither writes nor reads
     * {@code deliveryTime} -- and confirms the round-trip assertions above owe
     * their result to the v13 marshaller rather than to incidental state.
     */
    @Test
    public void testDeliveryTimeIsNotWrittenAtV12() throws Exception {
        OpenWireFormat wireFormat = wireFormat(true);
        wireFormat.setVersion(12);

        Message original = prepare(message);
        original.setDeliveryTime(DELIVERY_TIME);

        Message restored = (Message) unmarshal(wireFormat, marshal(wireFormat, original));

        assertEquals(label + " must not carry deliveryTime on the v12 wire",
                0L, restored.getDeliveryTime());
    }

    /**
     * The blob marshaller inserts {@code deliveryTime} ahead of its own fields,
     * so a misaligned offset would corrupt the blob payload rather than the
     * timestamp. Assert both together.
     */
    @Test
    public void testBlobFieldsStayAlignedAfterDeliveryTime() throws Exception {
        if (!(message instanceof ActiveMQBlobMessage)) {
            return;
        }
        OpenWireFormat wireFormat = wireFormat(true);

        ActiveMQBlobMessage original = (ActiveMQBlobMessage) prepare(message);
        original.setDeliveryTime(DELIVERY_TIME);

        ActiveMQBlobMessage restored =
                (ActiveMQBlobMessage) unmarshal(wireFormat, marshal(wireFormat, original));

        assertEquals(DELIVERY_TIME, restored.getDeliveryTime());
        assertEquals("http://localhost/blob/1", restored.getRemoteBlobUrl());
        assertEquals("application/octet-stream", restored.getMimeType());
        assertTrue(restored.isDeletedByBroker());
    }

    private static Message prepare(Message message) throws JMSException {
        Message copy = message.copy();
        copy.setMessageId(new MessageId(new ProducerId("id:localhost:1:1:1"), 1));
        copy.setDestination(new ActiveMQQueue("TEST.QUEUE"));
        return copy;
    }

    private static OpenWireFormat wireFormat(boolean tightEncoding) {
        OpenWireFormat wireFormat = new OpenWireFormat();
        wireFormat.setVersion(13);
        wireFormat.setCacheEnabled(false);
        wireFormat.setTightEncodingEnabled(tightEncoding);
        return wireFormat;
    }

    private static byte[] marshal(OpenWireFormat wireFormat, Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        wireFormat.marshal(obj, out);
        out.flush();
        return baos.toByteArray();
    }

    private static Object unmarshal(OpenWireFormat wireFormat, byte[] bytes) throws Exception {
        return wireFormat.unmarshal(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
