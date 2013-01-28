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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import junit.framework.TestCase;
/**
 * An AMQ-1282 Test
 * 
 */
public class AMQ1282 extends TestCase {
    private ConnectionFactory factory;
    private Connection connection;
    private MapMessage message;

    protected void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        message = session.createMapMessage();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
    }

    public void testUnmappedBooleanMessage() throws JMSException {
        Object expected;
        try {
            expected = Boolean.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Boolean actual = message.getBoolean("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            assertEquals(expected, ex);
        }
    }

    public void testUnmappedIntegerMessage() throws JMSException {
        Object expected;
        try {
            expected = Integer.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Integer actual = message.getInt("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedShortMessage() throws JMSException {
        Object expected;
        try {
            expected = Short.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Short actual = message.getShort("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedLongMessage() throws JMSException {
        Object expected;
        try {
            expected = Long.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Long actual = message.getLong("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedStringMessage() throws JMSException {
        Object expected;
        try {
            expected = String.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            String actual = message.getString("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedCharMessage() throws JMSException {
        try {
            message.getChar("foo");
            fail("should have thrown NullPointerException");
        } catch (NullPointerException success) {
            assertNotNull(success);
        }
    }

    public void testUnmappedByteMessage() throws JMSException {
        Object expected;
        try {
            expected = Byte.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Byte actual = message.getByte("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedDoubleMessage() throws JMSException {
        Object expected;
        try {
            expected = Double.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Double actual = message.getDouble("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }

    public void testUnmappedFloatMessage() throws JMSException {
        Object expected;
        try {
            expected = Float.valueOf(null);
        } catch (Exception ex) {
            expected = ex;
        }
        try {
            Float actual = message.getFloat("foo");
            assertEquals(expected, actual);
        } catch (Exception ex) {
            Class aClass = expected.getClass();
            assertTrue(aClass.isInstance(ex));
        }
    }
}
