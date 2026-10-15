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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import org.junit.Test;

public class ActiveMQMapMessageTest {

    @Test
    public void nestedMapsAndListsPreserveValuesAndStringTypes() throws Exception {
        Map<String, Object> nested = Map.of(
                "text", "\u03a9\u4e2d",
                "map", Map.of("list", List.of("inner", Map.of("leaf", "value"))),
                "emptyMap", Map.of(),
                "emptyList", List.of());
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setObject("nested", nested);
        message.storeContentAndClear();
        message.setReadOnlyBody(true);

        assertEquals(nested, message.getObject("nested"));
        assertEquals(nested, message.getContentMap().get("nested"));
    }

    @Test
    public void nestedListPreservesNullsPrimitiveTypesAndByteArrays() throws Exception {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setObject("nested", Arrays.asList(null, true, (byte) 2, (short) 3, 'x',
                4, 5L, 6.5f, 7.5d, new byte[] {8, 9},
                Map.of("list", Arrays.asList("text", null))));
        message.storeContentAndClear();
        message.setReadOnlyBody(true);

        List<?> received = (List<?>) message.getObject("nested");
        assertEquals(Arrays.asList(null, true, (byte) 2, (short) 3, 'x', 4, 5L, 6.5f, 7.5d),
                received.subList(0, 9));
        assertArrayEquals(new byte[] {8, 9}, (byte[]) received.get(9));
        assertEquals(Map.of("list", Arrays.asList("text", null)), received.get(10));
        assertEquals(11, received.size());
    }

    @Test
    public void nestedListPreservesPrimitiveBoundariesAndZeroValues() throws Exception {
        List<Object> expected = Arrays.asList(
                null,
                false, true,
                Byte.MIN_VALUE, (byte) 0, Byte.MAX_VALUE,
                Short.MIN_VALUE, (short) 0, Short.MAX_VALUE,
                Character.MIN_VALUE, Character.MAX_VALUE,
                Integer.MIN_VALUE, 0, Integer.MAX_VALUE,
                Long.MIN_VALUE, 0L, Long.MAX_VALUE,
                Float.MIN_VALUE, 0.0f, Float.MAX_VALUE,
                Double.MIN_VALUE, 0.0d, Double.MAX_VALUE);
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setObject("nested", List.of(expected));
        message.storeContentAndClear();
        message.setReadOnlyBody(true);

        List<?> nested = (List<?>) message.getObject("nested");
        assertEquals(1, nested.size());
        assertEquals(expected, nested.get(0));
    }

    @Test
    public void compressedNestedValuesPreserveLargeUnicodeStrings() throws Exception {
        String largeString = "\u03a9\u4e2d\ud83d\ude03".repeat(2048);
        Map<String, Object> nested = Map.of("list", List.of(largeString, Map.of("text", largeString)));
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setObject("nested", nested);
        message.compress();
        message.clearUnMarshalledState();
        message.setReadOnlyBody(true);

        assertTrue(message.isCompressed());
        assertEquals(nested, message.getObject("nested"));
    }

    @Test
    public void primitiveValuesRoundTripAndExposeNames() throws Exception {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setBoolean("boolean", true);
        message.setByte("byte", (byte) 2);
        message.setShort("short", (short) 3);
        message.setChar("char", 'x');
        message.setInt("int", 4);
        message.setLong("long", 5L);
        message.setFloat("float", 6.5f);
        message.setDouble("double", 7.5d);
        message.setString("string", "value");
        message.setBytes("bytes", new byte[] {8, 9});
        message.setObject("nullable", null);

        message.storeContentAndClear();
        message.setReadOnlyBody(true);

        assertTrue(message.getBoolean("boolean"));
        assertEquals(2, message.getByte("byte"));
        assertEquals(3, message.getShort("short"));
        assertEquals('x', message.getChar("char"));
        assertEquals(4, message.getInt("int"));
        assertEquals(5L, message.getLong("long"));
        assertEquals(6.5f, message.getFloat("float"), 0.0f);
        assertEquals(7.5d, message.getDouble("double"), 0.0d);
        assertEquals("value", message.getString("string"));
        assertArrayEquals(new byte[] {8, 9}, message.getBytes("bytes"));
        assertTrue(message.itemExists("nullable"));
        assertFalse(message.itemExists("missing"));

        Enumeration<String> names = message.getMapNames();
        List<String> found = new ArrayList<>();
        while (names.hasMoreElements()) {
            found.add(names.nextElement());
        }
        assertEquals(11, found.size());
        assertTrue(found.contains("nullable"));
    }

    @Test
    public void conversionsAndInvalidReads() throws Exception {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        message.setString("number", "12");
        message.setByte("small", (byte) 7);
        message.setFloat("decimal", 2.5f);
        message.setBytes("bytes", new byte[] {1});

        assertEquals(12, message.getInt("number"));
        assertEquals(7, message.getLong("small"));
        assertEquals(2.5d, message.getDouble("decimal"), 0.0d);
        assertThrows(MessageFormatException.class, () -> message.getInt("bytes"));
        assertThrows(MessageFormatException.class, () -> message.getString("bytes"));
        assertThrows(MessageFormatException.class, () -> message.getBytes("number"));
        assertThrows(NullPointerException.class, () -> message.getChar("missing"));
    }

    @Test
    public void bodyReadOnlyClearAndInvalidSetters() throws Exception {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        assertThrows(IllegalArgumentException.class, () -> message.setInt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> message.setInt("", 1));
        assertThrows(MessageFormatException.class, () -> message.setObject("object", new Object()));

        message.setInt("number", 1);
        message.storeContentAndClear();
        message.setReadOnlyBody(true);
        assertThrows(MessageNotWriteableException.class, () -> message.setInt("number", 2));

        message.clearBody();
        message.setInt("number", 3);
        assertEquals(3, message.getInt("number"));
        assertFalse(message.itemExists("old"));
    }

}
