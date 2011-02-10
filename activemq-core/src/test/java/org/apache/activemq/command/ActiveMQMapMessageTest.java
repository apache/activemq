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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision$
 */
public class ActiveMQMapMessageTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQMapMessageTest.class);

    private String name = "testName";

    /**
     * Constructor for ActiveMQMapMessageTest.
     * 
     * @param name
     */
    public ActiveMQMapMessageTest(String name) {
        super(name);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ActiveMQMapMessageTest.class);
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

    public void testBytesConversion() throws JMSException, IOException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setBoolean("boolean", true);
        msg.setByte("byte", (byte)1);
        msg.setBytes("bytes", new byte[1]);
        msg.setChar("char", 'a');
        msg.setDouble("double", 1.5);
        msg.setFloat("float", 1.5f);
        msg.setInt("int", 1);
        msg.setLong("long", 1);
        msg.setObject("object", "stringObj");
        msg.setShort("short", (short)1);
        msg.setString("string", "string");

        // Test with a 1Meg String
        StringBuffer bigSB = new StringBuffer(1024 * 1024);
        for (int i = 0; i < 1024 * 1024; i++) {
            bigSB.append((char)'a' + i % 26);
        }
        String bigString = bigSB.toString();

        msg.setString("bigString", bigString);

        msg = (ActiveMQMapMessage)msg.copy();

        assertEquals(msg.getBoolean("boolean"), true);
        assertEquals(msg.getByte("byte"), (byte)1);
        assertEquals(msg.getBytes("bytes").length, 1);
        assertEquals(msg.getChar("char"), 'a');
        assertEquals(msg.getDouble("double"), 1.5, 0);
        assertEquals(msg.getFloat("float"), 1.5f, 0);
        assertEquals(msg.getInt("int"), 1);
        assertEquals(msg.getLong("long"), 1);
        assertEquals(msg.getObject("object"), "stringObj");
        assertEquals(msg.getShort("short"), (short)1);
        assertEquals(msg.getString("string"), "string");
        assertEquals(msg.getString("bigString"), bigString);
    }

    public void testGetBoolean() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setBoolean(name, true);
        msg.setReadOnlyBody(true);
        assertTrue(msg.getBoolean(name));
        msg.clearBody();
        msg.setString(name, "true");

        msg = (ActiveMQMapMessage)msg.copy();

        assertTrue(msg.getBoolean(name));
    }

    public void testGetByte() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setByte(this.name, (byte)1);
        msg = (ActiveMQMapMessage)msg.copy();
        assertTrue(msg.getByte(this.name) == (byte)1);
    }

    public void testGetShort() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setShort(this.name, (short)1);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getShort(this.name) == (short)1);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetChar() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setChar(this.name, 'a');
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getChar(this.name) == 'a');
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetInt() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setInt(this.name, 1);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getInt(this.name) == 1);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetLong() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setLong(this.name, 1);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getLong(this.name) == 1);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetFloat() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setFloat(this.name, 1.5f);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getFloat(this.name) == 1.5f);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetDouble() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            msg.setDouble(this.name, 1.5);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(msg.getDouble(this.name) == 1.5);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetString() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            String str = "test";
            msg.setString(this.name, str);
            msg = (ActiveMQMapMessage)msg.copy();
            assertEquals(msg.getString(this.name), str);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetBytes() {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        try {
            byte[] bytes1 = new byte[3];
            byte[] bytes2 = new byte[2];
            System.arraycopy(bytes1, 0, bytes2, 0, 2);
            msg.setBytes(this.name, bytes1);
            msg.setBytes(this.name + "2", bytes1, 0, 2);
            msg = (ActiveMQMapMessage)msg.copy();
            assertTrue(Arrays.equals(msg.getBytes(this.name), bytes1));
            assertEquals(msg.getBytes(this.name + "2").length, bytes2.length);
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    public void testGetObject() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        Boolean booleanValue = Boolean.TRUE;
        Byte byteValue = Byte.valueOf("1");
        byte[] bytesValue = new byte[3];
        Character charValue = new Character('a');
        Double doubleValue = Double.valueOf("1.5");
        Float floatValue = Float.valueOf("1.5");
        Integer intValue = Integer.valueOf("1");
        Long longValue = Long.valueOf("1");
        Short shortValue = Short.valueOf("1");
        String stringValue = "string";

        try {
            msg.setObject("boolean", booleanValue);
            msg.setObject("byte", byteValue);
            msg.setObject("bytes", bytesValue);
            msg.setObject("char", charValue);
            msg.setObject("double", doubleValue);
            msg.setObject("float", floatValue);
            msg.setObject("int", intValue);
            msg.setObject("long", longValue);
            msg.setObject("short", shortValue);
            msg.setObject("string", stringValue);
        } catch (MessageFormatException mfe) {
            LOG.warn("Caught: " + mfe);
            mfe.printStackTrace();
            fail("object formats should be correct");
        }

        msg = (ActiveMQMapMessage)msg.copy();

        assertTrue(msg.getObject("boolean") instanceof Boolean);
        assertEquals(msg.getObject("boolean"), booleanValue);
        assertEquals(msg.getBoolean("boolean"), booleanValue.booleanValue());
        assertTrue(msg.getObject("byte") instanceof Byte);
        assertEquals(msg.getObject("byte"), byteValue);
        assertEquals(msg.getByte("byte"), byteValue.byteValue());
        assertTrue(msg.getObject("bytes") instanceof byte[]);
        assertEquals(((byte[])msg.getObject("bytes")).length, bytesValue.length);
        assertEquals(msg.getBytes("bytes").length, bytesValue.length);
        assertTrue(msg.getObject("char") instanceof Character);
        assertEquals(msg.getObject("char"), charValue);
        assertEquals(msg.getChar("char"), charValue.charValue());
        assertTrue(msg.getObject("double") instanceof Double);
        assertEquals(msg.getObject("double"), doubleValue);
        assertEquals(msg.getDouble("double"), doubleValue.doubleValue(), 0);
        assertTrue(msg.getObject("float") instanceof Float);
        assertEquals(msg.getObject("float"), floatValue);
        assertEquals(msg.getFloat("float"), floatValue.floatValue(), 0);
        assertTrue(msg.getObject("int") instanceof Integer);
        assertEquals(msg.getObject("int"), intValue);
        assertEquals(msg.getInt("int"), intValue.intValue());
        assertTrue(msg.getObject("long") instanceof Long);
        assertEquals(msg.getObject("long"), longValue);
        assertEquals(msg.getLong("long"), longValue.longValue());
        assertTrue(msg.getObject("short") instanceof Short);
        assertEquals(msg.getObject("short"), shortValue);
        assertEquals(msg.getShort("short"), shortValue.shortValue());
        assertTrue(msg.getObject("string") instanceof String);
        assertEquals(msg.getObject("string"), stringValue);
        assertEquals(msg.getString("string"), stringValue);

        msg.clearBody();
        try {
            msg.setObject("object", new Object());
            fail("should have thrown exception");
        } catch (MessageFormatException e) {
        }

    }

    public void testGetMapNames() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setBoolean("boolean", true);
        msg.setByte("byte", (byte)1);
        msg.setBytes("bytes1", new byte[1]);
        msg.setBytes("bytes2", new byte[3], 0, 2);
        msg.setChar("char", 'a');
        msg.setDouble("double", 1.5);
        msg.setFloat("float", 1.5f);
        msg.setInt("int", 1);
        msg.setLong("long", 1);
        msg.setObject("object", "stringObj");
        msg.setShort("short", (short)1);
        msg.setString("string", "string");

        msg = (ActiveMQMapMessage)msg.copy();

        Enumeration<String> mapNamesEnum = msg.getMapNames();
        List<String> mapNamesList = Collections.list(mapNamesEnum);

        assertEquals(mapNamesList.size(), 12);
        assertTrue(mapNamesList.contains("boolean"));
        assertTrue(mapNamesList.contains("byte"));
        assertTrue(mapNamesList.contains("bytes1"));
        assertTrue(mapNamesList.contains("bytes2"));
        assertTrue(mapNamesList.contains("char"));
        assertTrue(mapNamesList.contains("double"));
        assertTrue(mapNamesList.contains("float"));
        assertTrue(mapNamesList.contains("int"));
        assertTrue(mapNamesList.contains("long"));
        assertTrue(mapNamesList.contains("object"));
        assertTrue(mapNamesList.contains("short"));
        assertTrue(mapNamesList.contains("string"));
    }

    public void testItemExists() throws JMSException {
        ActiveMQMapMessage mapMessage = new ActiveMQMapMessage();

        mapMessage.setString("exists", "test");

        mapMessage = (ActiveMQMapMessage)mapMessage.copy();

        assertTrue(mapMessage.itemExists("exists"));
        assertFalse(mapMessage.itemExists("doesntExist"));
    }

    public void testClearBody() throws JMSException {
        ActiveMQMapMessage mapMessage = new ActiveMQMapMessage();
        mapMessage.setString("String", "String");
        mapMessage.clearBody();
        assertFalse(mapMessage.isReadOnlyBody());

        mapMessage.onSend();
        mapMessage.setContent(mapMessage.getContent());
        assertNull(mapMessage.getString("String"));
        mapMessage.clearBody();
        mapMessage.setString("String", "String");

        mapMessage = (ActiveMQMapMessage)mapMessage.copy();

        mapMessage.getString("String");
    }

    public void testReadOnlyBody() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setBoolean("boolean", true);
        msg.setByte("byte", (byte)1);
        msg.setBytes("bytes", new byte[1]);
        msg.setBytes("bytes2", new byte[3], 0, 2);
        msg.setChar("char", 'a');
        msg.setDouble("double", 1.5);
        msg.setFloat("float", 1.5f);
        msg.setInt("int", 1);
        msg.setLong("long", 1);
        msg.setObject("object", "stringObj");
        msg.setShort("short", (short)1);
        msg.setString("string", "string");

        msg.setReadOnlyBody(true);

        try {
            msg.getBoolean("boolean");
            msg.getByte("byte");
            msg.getBytes("bytes");
            msg.getChar("char");
            msg.getDouble("double");
            msg.getFloat("float");
            msg.getInt("int");
            msg.getLong("long");
            msg.getObject("object");
            msg.getShort("short");
            msg.getString("string");
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
        try {
            msg.setBoolean("boolean", true);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setByte("byte", (byte)1);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setBytes("bytes", new byte[1]);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setBytes("bytes2", new byte[3], 0, 2);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setChar("char", 'a');
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setDouble("double", 1.5);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setFloat("float", 1.5f);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setInt("int", 1);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setLong("long", 1);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setObject("object", "stringObj");
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setShort("short", (short)1);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            msg.setString("string", "string");
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    public void testWriteOnlyBody() throws JMSException {
        ActiveMQMapMessage msg = new ActiveMQMapMessage();
        msg.setReadOnlyBody(false);

        msg.setBoolean("boolean", true);
        msg.setByte("byte", (byte)1);
        msg.setBytes("bytes", new byte[1]);
        msg.setBytes("bytes2", new byte[3], 0, 2);
        msg.setChar("char", 'a');
        msg.setDouble("double", 1.5);
        msg.setFloat("float", 1.5f);
        msg.setInt("int", 1);
        msg.setLong("long", 1);
        msg.setObject("object", "stringObj");
        msg.setShort("short", (short)1);
        msg.setString("string", "string");

        msg.setReadOnlyBody(true);

        msg.getBoolean("boolean");
        msg.getByte("byte");
        msg.getBytes("bytes");
        msg.getChar("char");
        msg.getDouble("double");
        msg.getFloat("float");
        msg.getInt("int");
        msg.getLong("long");
        msg.getObject("object");
        msg.getShort("short");
        msg.getString("string");
    }

}
