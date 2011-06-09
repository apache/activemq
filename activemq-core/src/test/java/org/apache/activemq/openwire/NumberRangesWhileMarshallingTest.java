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

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.SessionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class NumberRangesWhileMarshallingTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NumberRangesWhileMarshallingTest.class);

    protected String connectionId = "Cheese";
    protected ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    protected DataOutputStream ds = new DataOutputStream(buffer);
    protected OpenWireFormat openWireformat;
    protected int endOfStreamMarker = 0x12345678;

    public void testLongNumberRanges() throws Exception {
        long[] numberValues = {
            // bytes
            0, 1, 0x7e, 0x7f, 0x80, 0x81, 0xf0, 0xff,
            // shorts
            0x7eff, 0x7fffL, 0x8001L, 0x8000L, 0xe000L, 0xe0001L, 0xff00L, 0xffffL,
            // ints
            0x10000L, 0x700000L, 0x12345678L, 0x72345678L, 0x7fffffffL, 0x80000000L, 0x80000001L, 0xE0000001L, 0xFFFFFFFFL,
            // 3 byte longs
            0x123456781L, 0x1234567812L, 0x12345678123L, 0x123456781234L, 0x1234567812345L, 0x12345678123456L, 0x7e345678123456L, 0x7fffffffffffffL,
            0x80000000000000L, 0x80000000000001L, 0xe0000000000001L, 0xffffffffffffffL,
            // 4 byte longs
            0x1234567812345678L, 0x7fffffffffffffffL, 0x8000000000000000L, 0x8000000000000001L, 0xe000000000000001L, 0xffffffffffffffffL, 1
        };

        for (int i = 0; i < numberValues.length; i++) {
            long value = numberValues[i];

            SessionId object = new SessionId();
            object.setConnectionId(connectionId);
            object.setValue(value);
            writeObject(object);
        }
        ds.writeInt(endOfStreamMarker);

        // now lets read from the stream
        ds.close();

        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        for (int i = 0; i < numberValues.length; i++) {
            long value = numberValues[i];
            String expected = Long.toHexString(value);
            LOG.info("Unmarshaling value: " + i + " = " + expected);

            SessionId command = (SessionId)openWireformat.unmarshal(dis);
            assertEquals("connection ID in object: " + i, connectionId, command.getConnectionId());
            String actual = Long.toHexString(command.getValue());
            assertEquals("value of object: " + i + " was: " + actual, expected, actual);
        }
        int marker = dis.readInt();
        assertEquals("Marker int", Integer.toHexString(endOfStreamMarker), Integer.toHexString(marker));

        // lets try read and we should get an exception
        try {
            byte value = dis.readByte();
            fail("Should have reached the end of the stream");
        } catch (IOException e) {
            // worked!
        }
    }

    public void testMaxFrameSize() throws Exception {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setMaxFrameSize(10);
        ActiveMQTextMessage msg = new ActiveMQTextMessage();
        msg.setText("This is a test");

        writeObject(msg);
        ds.writeInt(endOfStreamMarker);

        // now lets read from the stream
        ds.close();

        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);

        try {
            wf.unmarshal(dis);
        } catch (IOException ioe) {
           return;
        }

        fail("Should fail because of the large frame size");


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

    private void writeObject(Object object) throws IOException {
        openWireformat.marshal(object, ds);
    }
}
