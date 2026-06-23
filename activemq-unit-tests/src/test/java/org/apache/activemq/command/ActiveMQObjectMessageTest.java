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

import java.io.IOException;

import jakarta.jms.JMSException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * 
 */
public class ActiveMQObjectMessageTest extends TestCase {

    /**
     * Constructor for ActiveMQObjectMessageTest.
     *
     * @param name
     */
    public ActiveMQObjectMessageTest(String name) {
        super(name);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ActiveMQObjectMessageTest.class);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testBytes() throws JMSException, IOException {
        ActiveMQObjectMessage msg = new ActiveMQObjectMessage();
        String str = "testText";
        msg.setObject(str);
        
        msg = (ActiveMQObjectMessage) msg.copy();
        assertEquals(msg.getObject(), str);

    }

    public void testSetObject() throws JMSException {
        ActiveMQObjectMessage msg = new ActiveMQObjectMessage();
        String str = "testText";
        msg.setObject(str);
        assertTrue(msg.getObject() == str);
    }

    public void testClearBody() throws JMSException {
        ActiveMQObjectMessage objectMessage = new ActiveMQObjectMessage();
        try {
            objectMessage.setObject("String");
            objectMessage.clearBody();
            assertFalse(objectMessage.isReadOnlyBody());
            assertNull(objectMessage.getObject());
            objectMessage.setObject("String");
            objectMessage.getObject();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    public void testReadOnlyBody() throws JMSException {
        ActiveMQObjectMessage msg = new ActiveMQObjectMessage();
        msg.setObject("test");
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        try {
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotWriteableException e) {
        }
    }

    public void testWriteOnlyBody() throws JMSException { // should always be readable
        ActiveMQObjectMessage msg = new ActiveMQObjectMessage();
        msg.setReadOnlyBody(false);
        try {
            msg.setObject("test");
            msg.getObject();
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        }
        msg.setReadOnlyBody(true);
        try {
            msg.getObject();
            msg.setObject("test");
            fail("should throw exception");
        } catch (MessageNotReadableException e) {
            fail("should be readable");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    public void testUnCompressedException() throws Exception {
        ActiveMQConnection connection = mock(ActiveMQConnection.class);

        ActiveMQObjectMessage msg = new ActiveMQObjectMessage();
        msg.setConnection(connection);
        msg.setObject("test");

        // store and marshal
        msg.storeContentAndClear();
        assertNull(msg.object);

        // corrupt the buffer
        ByteSequenceData.writeIntBig(msg.content, 1000);

        try {
            // trigger unmarshalling the object
            msg.getObject();
            fail("Should have thrown exception");
        } catch (JMSException e) {
            // uncompressed will have an error from the JDK deserialization
            assertTrue(ExceptionUtils.getRootCause(e) instanceof IOException);
        }
    }

}
