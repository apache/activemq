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

import java.beans.Transient;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import jakarta.jms.JMSException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.junit.experimental.categories.Category;

/**
 * 
 */
@Category(ParallelTest.class)
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

    public void testBeforeMarshallRetainsText() throws Exception {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String text = new String("testText");

        msg.setText(text);
        msg.beforeMarshall(null);

        assertNotNull(msg.getContent());
        assertSame(text, msg.getText());
    }

    public void testGetTextRetainsContent() throws Exception {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String text = "testText";

        setContent(msg, text);

        ByteSequence content = msg.getContent();
        assertEquals(text, msg.getText());
        assertSame(content, msg.getContent());
    }

    public void testGetSizeIncludesRetainedTextAndMarshalledContent() throws Exception {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String text = "testText";

        msg.setText(text);

        int sizeBeforeMarshall = msg.getSize();
        int expectedSizeBeforeMarshall = msg.getMinimumMessageSize() + text.length() * 2;
        assertEquals(expectedSizeBeforeMarshall, sizeBeforeMarshall);

        msg.beforeMarshall(null);

        int sizeAfterMarshall = msg.getSize();
        int expectedSizeAfterMarshall = msg.getMinimumMessageSize() + text.length() * 2 + msg.getContent().getLength();
        assertEquals(expectedSizeAfterMarshall, sizeAfterMarshall);
        assertTrue(sizeAfterMarshall > sizeBeforeMarshall);
    }

    public void testGetSizeIncludesDecodedTextAndExistingContent() throws Exception {
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        String text = "testText";

        setContent(msg, text);

        int sizeBeforeGetText = msg.getSize();
        int expectedSizeBeforeGetText = msg.getMinimumMessageSize() + msg.getContent().getLength();
        assertEquals(expectedSizeBeforeGetText, sizeBeforeGetText);

        assertEquals(text, msg.getText());

        int sizeAfterGetText = msg.getSize();
        int expectedSizeAfterGetText = msg.getMinimumMessageSize() + msg.getContent().getLength() + text.length() * 2;
        assertEquals(expectedSizeAfterGetText, sizeAfterGetText);
        assertTrue(sizeAfterGetText > sizeBeforeGetText);
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
    
    protected void setContent(Message message, String text) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(baos);
        MarshallingSupport.writeUTF8(dataOut, text);
        dataOut.close();
        message.setContent(baos.toByteSequence());
    }
}
