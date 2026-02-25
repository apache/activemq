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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Map;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.ObjectMessage;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class ActiveMQJMS2MessageTest {

    @Test
    public void testMessageIsAssignableTo() throws JMSException {
        Message message = new ActiveMQMessage();
        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Integer.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testMessageGetBody() throws JMSException {
        Message message = new ActiveMQMessage();
        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Object.class));
    }

    @Test
    public void testStringMessageIsAssignableTo() throws JMSException {
        TextMessage nullBody = new ActiveMQTextMessage();
        assertTrue(nullBody.isBodyAssignableTo(String.class));
        //Spec says type is ignored and returns true if null body
        assertTrue(nullBody.isBodyAssignableTo(Integer.class));

        TextMessage message = new ActiveMQTextMessage();
        message.setText("Test message");
        assertTrue(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Integer.class));
    }

    @Test
    public void testStringMessageGetBody() throws JMSException {
        TextMessage nullMessage = new ActiveMQTextMessage();
        assertNull(nullMessage.getBody(String.class));

        TextMessage message = new ActiveMQTextMessage();
        message.setText("Test message");
        assertEquals("Test message", message.getBody(String.class));
    }

    @Test(expected = MessageFormatException.class)
    public void testStringMessageGetBodyWrongType() throws JMSException {
        TextMessage message = new ActiveMQTextMessage();
        message.setText("Test message");
        message.getBody(Integer.class);
    }

    @Test
    public void testByteMessageIsAssignableTo() throws JMSException {
        BytesMessage nullBody = new ActiveMQBytesMessage();
        //Spec says type is ignored and returns true if null body
        assertTrue(nullBody.isBodyAssignableTo(String.class));
        assertTrue(nullBody.isBodyAssignableTo(Integer.class));

        ByteSequence testBytes = new ByteSequence("test".getBytes());
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(testBytes);
        assertArrayEquals(testBytes.getData(), message.getBody(byte[].class));
    }

    @Test
    public void testByteMessageGetBody() throws JMSException {
        BytesMessage nullMessage = new ActiveMQBytesMessage();
        assertNull(nullMessage.getBody(String.class));

        ByteSequence testBytes = new ByteSequence("test".getBytes());
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(testBytes);
        assertArrayEquals(testBytes.getData(), message.getBody(byte[].class));
    }

    @Test(expected = MessageFormatException.class)
    public void testByteMessageGetBodyWrongType() throws JMSException {
        ByteSequence testBytes = new ByteSequence("test".getBytes());
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(testBytes);
        message.getBody(Integer.class);
    }

    @Test
    public void testObjectMessageIsAssignableTo() throws JMSException {
        ObjectMessage nullBody = new ActiveMQObjectMessage();
        //Spec says type is ignored and returns true if null body
        assertTrue(nullBody.isBodyAssignableTo(String.class));
        assertTrue(nullBody.isBodyAssignableTo(Object.class));
        assertTrue(nullBody.isBodyAssignableTo(Integer.class));

        ObjectMessage message = new ActiveMQObjectMessage();
        message.setObject("Test message");
        assertTrue(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Integer.class));
    }

    @Test
    public void testObjectMessageGetBody() throws JMSException {
        ObjectMessage nullMessage = new ActiveMQObjectMessage();
        assertNull(nullMessage.getBody(String.class));

        ObjectMessage message = new ActiveMQObjectMessage();
        message.setObject("Test message");
        assertEquals("Test message", message.getBody(Serializable.class));
        assertEquals("Test message", message.getBody(String.class));
    }

    @Test(expected = MessageFormatException.class)
    public void testObjectMessageGetBodyWrongType() throws JMSException {
        ObjectMessage message = new ActiveMQObjectMessage();
        message.setObject("Test message");
        message.getBody(Integer.class);
    }

    @Test
    public void testMapMessageIsAssignableTo() throws JMSException {
        MapMessage nullBody = new ActiveMQMapMessage();
        //Spec says type is ignored and returns true if null body
        assertTrue(nullBody.isBodyAssignableTo(String.class));
        assertTrue(nullBody.isBodyAssignableTo(Integer.class));
        assertTrue(nullBody.isBodyAssignableTo(Map.class));
    }

    @Test
    public void testMapMessageGetBody() throws JMSException {
        MapMessage nullMessage = new ActiveMQMapMessage();
        assertNull(nullMessage.getBody(Map.class));

        MapMessage message = new ActiveMQMapMessage();
        message.setString("testkey", "testvalue");
        assertEquals("testvalue", message.getBody(Map.class).get("testkey"));
    }

    @Test(expected = MessageFormatException.class)
    public void testMapMessageGetBodyWrongType() throws JMSException {
        MapMessage message = new ActiveMQMapMessage();
        message.setString("testkey", "testvalue");
        message.getBody(String.class);
    }

    @Test
    public void testStreamMessageIsAssignableTo() throws JMSException {
        StreamMessage nullBody = new ActiveMQStreamMessage();
        //Spec says always false
        assertFalse(nullBody.isBodyAssignableTo(String.class));
        assertFalse(nullBody.isBodyAssignableTo(Integer.class));
        assertFalse(nullBody.isBodyAssignableTo(Map.class));
    }

    @Test(expected = MessageFormatException.class)
    public void testStreamMessageGetBody() throws JMSException {
        StreamMessage message = new ActiveMQStreamMessage();
        //spec says always throws exception
        message.getBody(Object.class);
    }
}
