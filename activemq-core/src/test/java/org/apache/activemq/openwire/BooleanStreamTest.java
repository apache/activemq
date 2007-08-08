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
package org.apache.activemq.openwire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

/**
 * @version $Revision$
 */
public class BooleanStreamTest extends TestCase {

    protected OpenWireFormat openWireformat;
    protected int endOfStreamMarker = 0x12345678;
    int numberOfBytes = 8 * 200;

    interface BooleanValueSet {
        public boolean getBooleanValueFor(int index, int count);
    }

    public void testBooleanMarshallingUsingAllTrue() throws Exception {
        testBooleanStream(numberOfBytes, new BooleanValueSet() {
            public boolean getBooleanValueFor(int index, int count) {
                return true;
            }
        });
    }

    public void testBooleanMarshallingUsingAllFalse() throws Exception {
        testBooleanStream(numberOfBytes, new BooleanValueSet() {
            public boolean getBooleanValueFor(int index, int count) {
                return false;
            }
        });
    }

    public void testBooleanMarshallingUsingOddAlternateTrueFalse() throws Exception {
        testBooleanStream(numberOfBytes, new BooleanValueSet() {
            public boolean getBooleanValueFor(int index, int count) {
                return (index & 1) == 0;
            }
        });
    }

    public void testBooleanMarshallingUsingEvenAlternateTrueFalse() throws Exception {
        testBooleanStream(numberOfBytes, new BooleanValueSet() {
            public boolean getBooleanValueFor(int index, int count) {
                return (index & 1) != 0;
            }
        });
    }

    protected void testBooleanStream(int numberOfBytes, BooleanValueSet valueSet) throws Exception {
        for (int i = 0; i < numberOfBytes; i++) {
            try {
                assertMarshalBooleans(i, valueSet);
            } catch (Throwable e) {
                throw (AssertionFailedError)new AssertionFailedError("Iteration failed at: " + i).initCause(e);
            }
        }
    }

    protected void assertMarshalBooleans(int count, BooleanValueSet valueSet) throws Exception {
        BooleanStream bs = new BooleanStream();
        for (int i = 0; i < count; i++) {
            bs.writeBoolean(valueSet.getBooleanValueFor(i, count));
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(buffer);
        bs.marshal(ds);
        ds.writeInt(endOfStreamMarker);

        // now lets read from the stream
        ds.close();

        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        bs = new BooleanStream();
        try {
            bs.unmarshal(dis);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to unmarshal: " + count + " booleans: " + e);
        }

        for (int i = 0; i < count; i++) {
            boolean expected = valueSet.getBooleanValueFor(i, count);
            // /System.out.println("Unmarshaling value: " + i + " = " + expected
            // + " out of: " + count);

            try {
                boolean actual = bs.readBoolean();
                assertEquals("value of object: " + i + " was: " + actual, expected, actual);
            } catch (IOException e) {
                e.printStackTrace();
                fail("Failed to parse boolean: " + i + " out of: " + count + " due to: " + e);
            }
        }
        int marker = dis.readInt();
        assertEquals("Marker int when unmarshalling: " + count + " booleans", Integer.toHexString(endOfStreamMarker), Integer.toHexString(marker));

        // lets try read and we should get an exception
        try {
            byte value = dis.readByte();
            fail("Should have reached the end of the stream");
        } catch (IOException e) {
            // worked!
        }
    }

    protected void setUp() throws Exception {
        super.setUp();
        openWireformat = createOpenWireFormat();
    }

    protected OpenWireFormat createOpenWireFormat() {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(true);
        wf.setStackTraceEnabled(false);
        wf.setVersion(1);
        return wf;
    }

}
