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
package org.apache.activemq.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Test;

/**
 * Tests for the ActiveMQ StreamMessage implementation
 */
public class ActiveMQStreamMessageTest {

    @Test
    public void testGetDataStructureType() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        assertEquals(msg.getDataStructureType(), CommandTypes.ACTIVEMQ_STREAM_MESSAGE);
    }

    @Test
    public void testReadBoolean() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(msg.readBoolean());
            msg.reset();
            assertTrue(msg.readString().equals("true"));
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testreadByte() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            byte test = (byte)4;
            msg.writeByte(test);
            msg.reset();
            assertTrue(msg.readByte() == test);
            msg.reset();
            assertTrue(msg.readShort() == test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Byte.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadShort() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            short test = (short)4;
            msg.writeShort(test);
            msg.reset();
            assertTrue(msg.readShort() == test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Short.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadChar() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            char test = 'z';
            msg.writeChar(test);
            msg.reset();
            assertTrue(msg.readChar() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Character.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadInt() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            int test = 4;
            msg.writeInt(test);
            msg.reset();
            assertTrue(msg.readInt() == test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Integer.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadLong() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            long test = 4L;
            msg.writeLong(test);
            msg.reset();
            assertTrue(msg.readLong() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Long.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readDouble();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg = new ActiveMQStreamMessage();
            msg.writeObject(Long.valueOf("1"));
            // reset so it's readable now
            msg.reset();
            assertEquals(Long.parseLong("1"), msg.readObject());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadFloat() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            float test = 4.4f;
            msg.writeFloat(test);
            msg.reset();
            assertTrue(msg.readFloat() == test);
            msg.reset();
            assertTrue(msg.readDouble() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Float.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadDouble()  {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            double test = 4.4d;
            msg.writeDouble(test);
            msg.reset();
            assertTrue(msg.readDouble() == test);
            msg.reset();
            assertTrue(msg.readString().equals(Double.toString(test)));
            msg.reset();
            try {
                msg.readBoolean();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readBytes(new byte[1]);
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadString() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            byte testByte = (byte)2;
            msg.writeString(Byte.toString(testByte));
            msg.reset();
            assertTrue(msg.readByte() == testByte);
            msg.clearBody();
            short testShort = 3;
            msg.writeString(Short.toString(testShort));
            msg.reset();
            assertTrue(msg.readShort() == testShort);
            msg.clearBody();
            int testInt = 4;
            msg.writeString(Integer.toString(testInt));
            msg.reset();
            assertTrue(msg.readInt() == testInt);
            msg.clearBody();
            long testLong = 6L;
            msg.writeString(Long.toString(testLong));
            msg.reset();
            assertTrue(msg.readLong() == testLong);
            msg.clearBody();
            float testFloat = 6.6f;
            msg.writeString(Float.toString(testFloat));
            msg.reset();
            assertTrue(msg.readFloat() == testFloat);
            msg.clearBody();
            double testDouble = 7.7d;
            msg.writeString(Double.toString(testDouble));
            msg.reset();
            assertTrue(msg.readDouble() == testDouble);
            msg.clearBody();
            msg.writeString("true");
            msg.reset();
            assertTrue(msg.readBoolean());
            msg.clearBody();
            msg.writeString("a");
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException e) {
            }
            msg.clearBody();
            msg.writeString("777");
            msg.reset();
            try {
                msg.readBytes(new byte[3]);
                fail("Should have thrown exception");
            } catch (MessageFormatException e) {
            }

        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBigString() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            // Test with a 1Meg String
            StringBuffer bigSB = new StringBuffer(1024 * 1024);
            for (int i = 0; i < 1024 * 1024; i++) {
                bigSB.append('a' + i % 26);
            }
            String bigString = bigSB.toString();

            msg.writeString(bigString);
            msg.reset();
            assertEquals(bigString, msg.readString());

        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadBytes() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            byte[] test = new byte[50];
            for (int i = 0; i < test.length; i++) {
                test[i] = (byte)i;
            }
            msg.writeBytes(test);
            msg.reset();
            byte[] valid = new byte[test.length];
            msg.readBytes(valid);
            for (int i = 0; i < valid.length; i++) {
                assertTrue(valid[i] == test[i]);
            }
            msg.reset();
            try {
                msg.readByte();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readShort();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readInt();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readLong();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readFloat();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readChar();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
            msg.reset();
            try {
                msg.readString();
                fail("Should have thrown exception");
            } catch (MessageFormatException mfe) {
            }
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testReadObject() {
        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
        try {
            byte testByte = (byte)2;
            msg.writeByte(testByte);
            msg.reset();
            assertTrue(((Byte)msg.readObject()).byteValue() == testByte);
            msg.clearBody();

            short testShort = 3;
            msg.writeShort(testShort);
            msg.reset();
            assertTrue(((Short)msg.readObject()).shortValue() == testShort);
            msg.clearBody();

            int testInt = 4;
            msg.writeInt(testInt);
            msg.reset();
            assertTrue(((Integer)msg.readObject()).intValue() == testInt);
            msg.clearBody();

            long testLong = 6L;
            msg.writeLong(testLong);
            msg.reset();
            assertTrue(((Long)msg.readObject()).longValue() == testLong);
            msg.clearBody();

            float testFloat = 6.6f;
            msg.writeFloat(testFloat);
            msg.reset();
            assertTrue(((Float)msg.readObject()).floatValue() == testFloat);
            msg.clearBody();

            double testDouble = 7.7d;
            msg.writeDouble(testDouble);
            msg.reset();
            assertTrue(((Double)msg.readObject()).doubleValue() == testDouble);
            msg.clearBody();

            char testChar = 'z';
            msg.writeChar(testChar);
            msg.reset();
            assertTrue(((Character)msg.readObject()).charValue() == testChar);
            msg.clearBody();

            byte[] data = new byte[50];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte)i;
            }
            msg.writeBytes(data);
            msg.reset();
            byte[] valid = (byte[])msg.readObject();
            assertTrue(valid.length == data.length);
            for (int i = 0; i < valid.length; i++) {
                assertTrue(valid[i] == data[i]);
            }
            msg.clearBody();
            msg.writeBoolean(true);
            msg.reset();
            assertTrue(((Boolean)msg.readObject()).booleanValue());
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testClearBody() throws JMSException {
        ActiveMQStreamMessage streamMessage = new ActiveMQStreamMessage();
        try {
            streamMessage.writeObject(Long.valueOf(2));
            streamMessage.clearBody();
            assertFalse(streamMessage.isReadOnlyBody());
            streamMessage.writeObject(Long.valueOf(2));
            streamMessage.readObject();
            fail("should throw exception");
        } catch (MessageNotReadableException mnwe) {
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    @Test
    public void testReset() throws JMSException {
        ActiveMQStreamMessage streamMessage = new ActiveMQStreamMessage();
        try {
            streamMessage.writeDouble(24.5);
            streamMessage.writeLong(311);
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
        streamMessage.reset();
        try {
            assertTrue(streamMessage.isReadOnlyBody());
            assertEquals(streamMessage.readDouble(), 24.5, 0);
            assertEquals(streamMessage.readLong(), 311);
        } catch (MessageNotReadableException mnre) {
            fail("should be readable");
        }
        try {
            streamMessage.writeInt(33);
            fail("should throw exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testReadOnlyBody() throws JMSException {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        try {
            message.writeBoolean(true);
            message.writeByte((byte)1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float)1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short)1);
            message.writeString("string");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        message.reset();
        try {
            message.readBoolean();
            message.readByte();
            assertEquals(1, message.readBytes(new byte[10]));
            assertEquals(-1, message.readBytes(new byte[10]));
            assertEquals(2, message.readBytes(new byte[10]));
            assertEquals(-1, message.readBytes(new byte[10]));
            message.readChar();
            message.readDouble();
            message.readFloat();
            message.readInt();
            message.readLong();
            message.readString();
            message.readShort();
            message.readString();
        } catch (MessageNotReadableException mnwe) {
            fail("Should be readable");
        }
        try {
            message.writeBoolean(true);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeByte((byte)1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeBytes(new byte[3], 0, 2);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeChar('a');
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeDouble(1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeFloat((float)1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeInt(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeLong(1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeObject("stringobj");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeShort((short)1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
        try {
            message.writeString("string");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException mnwe) {
        }
    }

    @Test
    public void testWriteOnlyBody() throws JMSException {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        message.clearBody();
        try {
            message.writeBoolean(true);
            message.writeByte((byte)1);
            message.writeBytes(new byte[1]);
            message.writeBytes(new byte[3], 0, 2);
            message.writeChar('a');
            message.writeDouble(1.5);
            message.writeFloat((float)1.5);
            message.writeInt(1);
            message.writeLong(1);
            message.writeObject("stringobj");
            message.writeShort((short)1);
            message.writeString("string");
        } catch (MessageNotWriteableException mnwe) {
            fail("Should be writeable");
        }
        try {
            message.readBoolean();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException mnwe) {
        }
        try {
            message.readByte();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[1]);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readBytes(new byte[2]);
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readChar();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readDouble();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readFloat();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readInt();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readLong();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readString();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readShort();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
        try {
            message.readString();
            fail("Should have thrown exception");
        } catch (MessageNotReadableException e) {
        }
    }

    @Test
    public void testWriteObject() {
        try {
            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
            message.clearBody();
            message.writeObject("test");
            message.writeObject(Character.valueOf('a'));
            message.writeObject(Boolean.FALSE);
            message.writeObject(Byte.valueOf((byte) 2));
            message.writeObject(Short.valueOf((short) 2));
            message.writeObject(Integer.valueOf(2));
            message.writeObject(Long.valueOf(2l));
            message.writeObject(Float.valueOf(2.0f));
            message.writeObject(Double.valueOf(2.0d));
        } catch(Exception e) {
            fail(e.getMessage());
        }
        try {
            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
            message.clearBody();
            message.writeObject(new Object());
            fail("should throw an exception");
        } catch(MessageFormatException e) {
        } catch(Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testReadEmptyBufferFromStream() throws JMSException {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        message.clearBody();

        final byte[] BYTE_LIST = {1, 2, 4};

        byte[] readList = new byte[BYTE_LIST.length - 1];
        byte[] emptyList = {};

        message.writeBytes(emptyList);
        message.reset();

        // First call should return zero as the array written was zero sized.
        assertEquals(0, message.readBytes(readList));

        // Second call should return -1 as we've reached the end of element.
        assertEquals(-1, message.readBytes(readList));
    }

    @Test
    public void testReadMixBufferValuesFromStream() throws JMSException {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        message.clearBody();

        final int size = 3;

        final byte[] BYTE_LIST_1 = {1, 2, 3};
        final byte[] BYTE_LIST_2 = {4, 5, 6};
        final byte[] EMPTY_LIST = {};

        byte[] bigBuffer = new byte[size + size];
        byte[] smallBuffer = new byte[size - 1];

        message.writeBytes(BYTE_LIST_1);
        message.writeBytes(EMPTY_LIST);
        message.writeBytes(BYTE_LIST_2);
        message.writeBytes(EMPTY_LIST);
        message.reset();

        // Read first with big buffer
        assertEquals(size, message.readBytes(bigBuffer));
        assertEquals(1, bigBuffer[0]);
        assertEquals(2, bigBuffer[1]);
        assertEquals(3, bigBuffer[2]);
        assertEquals(-1, message.readBytes(bigBuffer));

        // Read the empty buffer, should not be able to read anything else until
        // the bytes read is completed.
        assertEquals(0, message.readBytes(bigBuffer));
        try {
            message.readBoolean();
        } catch (JMSException ex) {}
        assertEquals(-1, message.readBytes(bigBuffer));

        // Read the third buffer with small buffer, anything that is attempted
        // to be read in between reads or before read completion should throw.
        assertEquals(smallBuffer.length, message.readBytes(smallBuffer));
        assertEquals(4, smallBuffer[0]);
        assertEquals(5, smallBuffer[1]);
        try {
            message.readByte();
        } catch (JMSException ex) {}
        assertEquals(1, message.readBytes(smallBuffer));
        assertEquals(6, smallBuffer[0]);
        try {
            message.readBoolean();
        } catch (JMSException ex) {}
        assertEquals(-1, message.readBytes(bigBuffer));

        // Read the empty buffer, should not be able to read anything else until
        // the bytes read is completed.
        assertEquals(0, message.readBytes(bigBuffer));
        try {
            message.readBoolean();
        } catch (JMSException ex) {}
        assertEquals(-1, message.readBytes(bigBuffer));

        // Message should be empty now
        try {
            message.readBoolean();
        } catch (MessageEOFException ex) {}
    }
}
