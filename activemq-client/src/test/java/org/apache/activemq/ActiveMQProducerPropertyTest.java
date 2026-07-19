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
package org.apache.activemq;

import static org.junit.Assert.*;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;

import org.apache.activemq.transport.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Property accessors on {@link JMSProducer} must treat a property that was never
 * set as if it exists with a null value, which means applying the JMS property
 * conversion table to {@code null} — the same semantics {@code Boolean.valueOf(null)},
 * {@code Integer.valueOf(null)} and friends have, and the same behaviour
 * {@code ActiveMQMessage} already implements:
 *
 * <pre>
 *   boolean                -&gt; false
 *   byte, short, int, long -&gt; NumberFormatException
 *   float, double          -&gt; NullPointerException
 *   String, Object         -&gt; null
 * </pre>
 */
public class ActiveMQProducerPropertyTest {

    private static final String MISSING = "TESTDUMMY";

    private JMSContext context;
    private JMSProducer producer;

    @Before
    public void setUp() {
        final StubTransport transport = new StubTransport();
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("tcp://localhost:61616") {
                    @Override
                    protected Transport createTransport() {
                        return transport;
                    }
                };
        context = factory.createContext();
        producer = context.createProducer();
    }

    @After
    public void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    // --- missing property: the conversion table applied to null ---

    @Test
    public void testMissingBooleanPropertyReturnsFalse() {
        assertFalse("Boolean.valueOf(null) is false", producer.getBooleanProperty(MISSING));
    }

    @Test(expected = NumberFormatException.class)
    public void testMissingBytePropertyThrowsNumberFormat() {
        producer.getByteProperty(MISSING);
    }

    @Test(expected = NumberFormatException.class)
    public void testMissingShortPropertyThrowsNumberFormat() {
        producer.getShortProperty(MISSING);
    }

    @Test(expected = NumberFormatException.class)
    public void testMissingIntPropertyThrowsNumberFormat() {
        producer.getIntProperty(MISSING);
    }

    @Test(expected = NumberFormatException.class)
    public void testMissingLongPropertyThrowsNumberFormat() {
        producer.getLongProperty(MISSING);
    }

    @Test(expected = NullPointerException.class)
    public void testMissingFloatPropertyThrowsNullPointer() {
        producer.getFloatProperty(MISSING);
    }

    @Test(expected = NullPointerException.class)
    public void testMissingDoublePropertyThrowsNullPointer() {
        producer.getDoubleProperty(MISSING);
    }

    @Test
    public void testMissingStringPropertyReturnsNull() {
        assertNull(producer.getStringProperty(MISSING));
    }

    @Test
    public void testMissingObjectPropertyReturnsNull() {
        assertNull(producer.getObjectProperty(MISSING));
    }

    @Test
    public void testMissingPropertyDoesNotExist() {
        assertFalse(producer.propertyExists(MISSING));
    }

    /**
     * The whole table in one pass, mirroring how the TCK's msgPropertiesTest
     * walks every accessor for a single absent property.
     */
    @Test
    public void testFullConversionTableForMissingProperty() {
        assertFalse(producer.getBooleanProperty(MISSING));
        assertNull(producer.getStringProperty(MISSING));
        assertNull(producer.getObjectProperty(MISSING));
        assertThrows(NumberFormatException.class, () -> producer.getByteProperty(MISSING));
        assertThrows(NumberFormatException.class, () -> producer.getShortProperty(MISSING));
        assertThrows(NumberFormatException.class, () -> producer.getIntProperty(MISSING));
        assertThrows(NumberFormatException.class, () -> producer.getLongProperty(MISSING));
        assertThrows(NullPointerException.class, () -> producer.getFloatProperty(MISSING));
        assertThrows(NullPointerException.class, () -> producer.getDoubleProperty(MISSING));
    }

    // --- regression guard: values that WERE set must still convert normally ---

    @Test
    public void testSetPropertiesStillReadBack() {
        producer.setProperty("aBoolean", true);
        producer.setProperty("anInt", 42);
        producer.setProperty("aString", "hello");

        assertTrue(producer.getBooleanProperty("aBoolean"));
        assertEquals(42, producer.getIntProperty("anInt"));
        assertEquals("hello", producer.getStringProperty("aString"));
        assertTrue(producer.propertyExists("anInt"));
    }

    /**
     * A property explicitly set to a numeric string must still convert — the
     * null-branch change must not short-circuit real values.
     */
    @Test
    public void testNumericStringPropertyStillConverts() {
        producer.setProperty("num", "7");

        assertEquals(7, producer.getIntProperty("num"));
        assertEquals(7L, producer.getLongProperty("num"));
        assertEquals(7, producer.getByteProperty("num"));
    }

    /**
     * A boolean read of a non-boolean value is a conversion failure, not a
     * missing property — it must still raise, not silently return false.
     */
    @Test
    public void testNonConvertibleValueStillRaises() {
        producer.setProperty("notANumber", "abc");

        assertThrows(NumberFormatException.class, () -> producer.getIntProperty("notANumber"));
        // Boolean.valueOf("abc") is false — a defined conversion, not an error.
        assertFalse(producer.getBooleanProperty("notANumber"));
    }
}
