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
package org.apache.activemq.command;

import static org.mockito.Mockito.mock;

import java.beans.Transient;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import jakarta.jms.JMSException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.util.MarshallingSupport.ActiveMQUnmarshalEOFException;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * 
 */
public class ActiveMQTextMessageTest extends TestCase {

    public static void main(String[] args) {
        TestRunner.run(ActiveMQTextMessageTest.class);
    }

    public void testGetDataStructureType() {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        assertEquals(msg.getDataStructureType(), CommandTypes.ACTIVEMQ_TEXT_MESSAGE);
    }

    public void testShallowCopy() throws JMSException {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String string = "str";
        msg.setText(string);
        Message copy = msg.copy();
        assertTrue(msg.getText() == ((ActiveMQTextMessage) copy).getText());
    }

    public void testSetText() {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String str = "testText";
        try {
            msg.setText(str);
            assertEquals(msg.getText(), str);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void testGetBytes() throws JMSException, IOException {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String str = "testText";
        msg.setText(str);
        msg.beforeMarshall(null);
        
        ByteSequence bytes = msg.getContent();
        msg = new ActiveMQTextMessage();
        msg.setContent(bytes);
        
        assertEquals(msg.getText(), str);
    }

    public void testClearBody() throws JMSException, IOException {
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText("string");
        textMessage.clearBody();
        assertFalse(textMessage.isReadOnlyBody());
        assertNull(textMessage.getText());
        try {
            textMessage.setText("String");
            textMessage.getText();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
    }

    public void testReadOnlyBody() throws JMSException {
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText("test");
        textMessage.setReadOnlyBody(true);
        try {
            textMessage.getText();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        try {
            textMessage.setText("test");
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    public void testWriteOnlyBody() throws JMSException { // should always be readable
        ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setReadOnlyBody(false);
        try {
            textMessage.setText("test");
            textMessage.getText();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        textMessage.setReadOnlyBody(true);
        try {
            textMessage.getText();
            textMessage.setText("test");
            fail("should throw exception");
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        } catch (MessageNotWriteableException mnwe) {
        }
    }
    
    public void testShortText() throws Exception {
        String shortText = "Content";
    	ActiveMQTextMessage shortMessage = new ActiveMQTextMessage();
        setContent(shortMessage, shortText);
        assertTrue(shortMessage.toString().contains("text = " + shortText));
        assertTrue(shortMessage.getText().equals(shortText));
        
        String longText = "Very very very very veeeeeeery loooooooooooooooooooooooooooooooooong text";
        String longExpectedText = "Very very very very veeeeeeery looooooooooooo...ooooong text";
        ActiveMQTextMessage longMessage = new ActiveMQTextMessage();
        setContent(longMessage, longText);
        assertTrue(longMessage.toString().contains("text = " + longExpectedText));
        assertTrue(longMessage.getText().equals(longText));         
    }
    
    public void testNullText() throws Exception {
    	ActiveMQTextMessage nullMessage = new ActiveMQTextMessage();
    	setContent(nullMessage, null);
    	assertTrue(nullMessage.toString().contains("text = null"));
    }

    public void testTransient() throws Exception {
        Method method = ActiveMQTextMessage.class.getMethod("getRegionDestination");
        assertTrue(method.isAnnotationPresent(Transient.class));
    }

    public void testUnUnmarshalException() throws Exception {
        ActiveMQConnection connection = mock(ActiveMQConnection.class);

        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        msg.setConnection(connection);
        msg.setText("content");

        // store and marshal
        msg.storeContentAndClear();
        assertNull(msg.text);

        // corrupt the buffer
        ByteSequenceData.writeIntBig(msg.content, 1000);

        try {
            msg.getText();
            fail("Should have thrown exception");
        } catch (JMSException e) {
            // expected
            assertTrue(
                    ExceptionUtils.getRootCause(e) instanceof ActiveMQUnmarshalEOFException);
        }
    }
    
    protected void setContent(Message message, String text) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(baos);
        MarshallingSupport.writeUTF8(dataOut, text);
        dataOut.close();
        message.setContent(baos.toByteSequence());
    }
}
