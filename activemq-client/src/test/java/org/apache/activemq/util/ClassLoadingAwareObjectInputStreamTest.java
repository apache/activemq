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
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.Vector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ClassLoadingAwareObjectInputStreamTest {

    private final String ACCEPTS_ALL_FILTER = "*";
    private final String ACCEPTS_NONE_FILTER = "";

    @Rule
    public TestName name = new TestName();

    //----- Test for serialized objects --------------------------------------//

    @Test
    public void testReadObject() throws Exception {
        // Expect to succeed
        doTestReadObject(new SimplePojo(name.getMethodName()), ACCEPTS_ALL_FILTER);

        // Expect to fail
        try {
            doTestReadObject(new SimplePojo(name.getMethodName()), ACCEPTS_NONE_FILTER);
            fail("Should have failed to read");
        } catch (ClassNotFoundException cnfe) {
            // Expected
        }
    }

    @Test
    public void testReadObjectWithAnonymousClass() throws Exception {
        AnonymousSimplePojoParent pojoParent = new AnonymousSimplePojoParent(name.getMethodName());

        byte[] serialized = serializeObject(pojoParent);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "org.apache.activemq.util" }));

            Object obj = reader.readObject();

            assertTrue(obj instanceof AnonymousSimplePojoParent);
            assertEquals("Unexpected payload", pojoParent.getPayload(), ((AnonymousSimplePojoParent)obj).getPayload());
        }
    }

    @Test
    public void testReadObjectWitLocalClass() throws Exception {
        LocalSimplePojoParent pojoParent = new LocalSimplePojoParent(name.getMethodName());

        byte[] serialized = serializeObject(pojoParent);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "org.apache.activemq.util" }));

            Object obj = reader.readObject();

            assertTrue(obj instanceof LocalSimplePojoParent);
            assertEquals("Unexpected payload", pojoParent.getPayload(), ((LocalSimplePojoParent)obj).getPayload());
        }
    }

    @Test
    public void testReadObjectByte() throws Exception {
        doTestReadObject(Byte.valueOf((byte) 255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectShort() throws Exception {
        doTestReadObject(Short.valueOf((short) 255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectInteger() throws Exception {
        doTestReadObject(Integer.valueOf(255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectLong() throws Exception {
        doTestReadObject(Long.valueOf(255l), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectFloat() throws Exception {
        doTestReadObject(Float.valueOf(255.0f), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectDouble() throws Exception {
        doTestReadObject(Double.valueOf(255.0), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectBoolean() throws Exception {
        doTestReadObject(Boolean.FALSE, ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectString() throws Exception {
        doTestReadObject(new String(name.getMethodName()), ACCEPTS_ALL_FILTER);
    }

    //----- Test that arrays of objects can be read --------------------------//

    @Test
    public void testReadObjectStringArray() throws Exception {
        String[] value = new String[2];

        value[0] = name.getMethodName() + "-1";
        value[1] = name.getMethodName() + "-2";

        doTestReadObject(value, ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectMultiDimensionalArray() throws Exception {
        String[][][] value = new String[2][2][1];

        value[0][0][0] = "0-0-0";
        value[0][1][0] = "0-1-0";
        value[1][0][0] = "1-0-0";
        value[1][1][0] = "1-1-0";

        doTestReadObject(value, ACCEPTS_ALL_FILTER);
    }

    //----- Test that primitive types are not filtered -----------------------//

    @Test
    public void testPrimitiveByteNotFiltered() throws Exception {
        doTestReadPrimitive((byte) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveShortNotFiltered() throws Exception {
        doTestReadPrimitive((short) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveIntegerNotFiltered() throws Exception {
        doTestReadPrimitive(255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveLongNotFiltered() throws Exception {
        doTestReadPrimitive((long) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveFloatNotFiltered() throws Exception {
        doTestReadPrimitive((float) 255.0, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveDoubleNotFiltered() throws Exception {
        doTestReadPrimitive(255.0, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveBooleanNotFiltered() throws Exception {
        doTestReadPrimitive(false, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitveCharNotFiltered() throws Exception {
        doTestReadPrimitive('c', ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testReadObjectStringNotFiltered() throws Exception {
        doTestReadObject(new String(name.getMethodName()), ACCEPTS_NONE_FILTER);
    }

    //----- Test that primitive arrays get past filters ----------------------//

    @Test
    public void testPrimitiveByteArrayNotFiltered() throws Exception {
        byte[] value = new byte[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveShortArrayNotFiltered() throws Exception {
        short[] value = new short[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveIntegerArrayNotFiltered() throws Exception {
        int[] value = new int[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveLongArrayNotFiltered() throws Exception {
        long[] value = new long[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveFloatArrayNotFiltered() throws Exception {
        float[] value = new float[2];

        value[0] = 1.1f;
        value[1] = 2.1f;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveDoubleArrayNotFiltered() throws Exception {
        double[] value = new double[2];

        value[0] = 1.1;
        value[1] = 2.1;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    //----- Tests for types that should be filtered --------------------------//

    @Test
    public void testReadObjectArrayFiltered() throws Exception {
        UUID[] value = new UUID[2];

        value[0] = UUID.randomUUID();
        value[1] = UUID.randomUUID();

        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(ACCEPTS_NONE_FILTER.split(",")));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }
    }

    @Test
    public void testReadObjectMixedTypeArrayGetsFiltered() throws Exception {
        Object[] value = new Object[4];

        value[0] = name.getMethodName();
        value[1] = UUID.randomUUID();
        value[2] = new Vector<Object>();
        value[3] = new SimplePojo(name.getMethodName());

        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
            ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "java" }));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {
            }
        }

        // Replace the filtered type and try again
        value[3] = new Integer(20);

        serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
            ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "java" }));

            try {
                Object result = reader.readObject();

                assertNotNull(result);
                assertTrue(result.getClass().isArray());
            } catch (ClassNotFoundException ex) {
                fail("Should be able to read the payload.");
            }
        }
    }

    @Test
    public void testReadObjectMultiDimensionalArrayFiltered() throws Exception {
        UUID[][] value = new UUID[2][2];

        value[0][0] = UUID.randomUUID();
        value[0][1] = UUID.randomUUID();
        value[1][0] = UUID.randomUUID();
        value[1][1] = UUID.randomUUID();

        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(ACCEPTS_NONE_FILTER.split(",")));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }
    }

    @Test
    public void testReadObjectFailsWithUntrustedType() throws Exception {
        byte[] serialized = serializeObject(new SimplePojo(name.getMethodName()));

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "java" }));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }

        serialized = serializeObject(UUID.randomUUID());
        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            try {
                reader.readObject();
            } catch (ClassNotFoundException ex) {
                fail("Should be able to read the payload.");
            }
        }
    }

    @Test
    public void testReadObjectFailsWithUnstrustedContentInTrustedType() throws Exception {
        byte[] serialized = serializeObject(new SimplePojo(UUID.randomUUID()));

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "org.apache.activemq" }));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }

        serialized = serializeObject(UUID.randomUUID());
        input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(new String[] { "org.apache.activemq" }));

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {
            }
        }
    }

    //----- Internal methods -------------------------------------------------//

    private void doTestReadObject(Object value, String filter) throws Exception {
        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader =
                 new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(filter.split(",")));

            Object result = reader.readObject();
            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            if (result.getClass().isArray()) {
                assertTrue(Arrays.deepEquals((Object[]) value, (Object[]) result));
            } else {
                assertEquals(value, result);
            }
        }
    }

    private byte[] serializeObject(Object value) throws IOException {
        byte[] result = new byte[0];

        if (value != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                oos.writeObject(value);
                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }

    private void doTestReadPrimitive(Object value, String filter) throws Exception {
        byte[] serialized = serializePrimitive(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(filter.split(",")));

            Object result = null;

            if (value instanceof Byte) {
                result = reader.readByte();
            } else if (value instanceof Short) {
                result = reader.readShort();
            } else if (value instanceof Integer) {
                result = reader.readInt();
            } else if (value instanceof Long) {
                result = reader.readLong();
            } else if (value instanceof Float) {
                result = reader.readFloat();
            } else if (value instanceof Double) {
                result = reader.readDouble();
            } else if (value instanceof Boolean) {
                result = reader.readBoolean();
            } else if (value instanceof Character) {
                result = reader.readChar();
            } else {
                throw new IllegalArgumentException("unsuitable type for primitive deserialization");
            }

            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            assertEquals(value, result);
        }
    }

    private void doTestReadPrimitiveArray(Object value, String filter) throws Exception {
        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input)) {

            reader.setTrustAllPackages(false);
            reader.setTrustedPackages(Arrays.asList(filter.split(",")));

            Object result = reader.readObject();

            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            assertTrue(result.getClass().isArray());
            assertEquals(value.getClass().getComponentType(), result.getClass().getComponentType());
            assertTrue(result.getClass().getComponentType().isPrimitive());
        }
    }

    private byte[] serializePrimitive(Object value) throws IOException {
        byte[] result = new byte[0];

        if (value != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                if (value instanceof Byte) {
                    oos.writeByte((byte) value);
                } else if (value instanceof Short) {
                    oos.writeShort((short) value);
                } else if (value instanceof Integer) {
                    oos.writeInt((int) value);
                } else if (value instanceof Long) {
                    oos.writeLong((long) value);
                } else if (value instanceof Float) {
                    oos.writeFloat((float) value);
                } else if (value instanceof Double) {
                    oos.writeDouble((double) value);
                } else if (value instanceof Boolean) {
                    oos.writeBoolean((boolean) value);
                } else if (value instanceof Character) {
                    oos.writeChar((char) value);
                } else {
                    throw new IllegalArgumentException("unsuitable type for primitive serialization");
                }

                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }
}
