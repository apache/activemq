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
package org.apache.activemq.soaktest;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.openwire.OpenWireFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

/**
 *
 * @version $Revision: 383541 $
 */
public class MarshallingWithCachingTest extends TestCase {

    protected long commandCount = Short.MAX_VALUE;
    protected String connectionId = "Cheese";
    protected ActiveMQDestination destination = new ActiveMQQueue("Foo");
    protected int endOfStreamMarker = 0x12345678;
    
    protected ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    protected DataOutputStream ds = new DataOutputStream(buffer);
    protected OpenWireFormat openWireformat;
    protected long logGroup = 10000;
    

    
    public void testReadAndWriteLotsOfCommands() throws Exception {
        System.out.println("Marshalling: " + commandCount + " objects");
        for (long i = 0; i < commandCount ; i++) {
            logProgress("Marshalling", i);
            DataStructure object = createDataStructure(i);
            writeObject(object);
        }
        ds.writeInt(endOfStreamMarker);
        
        // now lets read from the stream
        ds.close();
        
        System.out.println("Unmarshalling: " + commandCount + " objects");
        
        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        for (long i = 0; i < commandCount ; i++) {
            logProgress("Unmarshalling", i);
            DataStructure command = null;
            try {
                command = (DataStructure) openWireformat.unmarshal(dis);
            }
            catch (Exception e) {
                e.printStackTrace();
                fail("Failed to unmarshal object: " + i + ". Reason: " + e);
            }
            assertDataStructureExpected(command, i);
        }

        int marker = dis.readInt();
        assertEquals("Marker int", Integer.toHexString(endOfStreamMarker), Integer.toHexString(marker));
        
        // lets try read and we should get an exception
        try {
            dis.readByte();
            fail("Should have reached the end of the stream");
        }
        catch (IOException e) {
            // worked!
        }
    }

    protected void logProgress(String message, long i) {
        if (i % logGroup  == 0) {
            System.out.println(message + " at object: " + i);
        }
    }

    protected void setUp() throws Exception {
        super.setUp();
        openWireformat = createOpenWireFormat();
    }

    protected OpenWireFormat createOpenWireFormat() {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(true);
        wf.setStackTraceEnabled(true);
        wf.setVersion(1);
        return wf;
    }

    private void writeObject(Object object) throws IOException {
        openWireformat.marshal(object, ds);
    }
    
    protected DataStructure createDataStructure(long index) {
        ProducerId id = new ProducerId();
        id.setConnectionId(connectionId);
        id.setSessionId(index);
        id.setValue(index);
        
        ProducerInfo object = new ProducerInfo();
        object.setProducerId(id);
        object.setDestination(destination);
        return object;
    }
    
    protected void assertDataStructureExpected(DataStructure object, long i) {
        assertEquals("Type of object for index: " + i, ProducerInfo.class, object.getClass());
        ProducerInfo command = (ProducerInfo) object;
        
        ProducerId id = command.getProducerId();
        assertNotNull("ProducerID for object at index: " + i, id);
        
        assertEquals("connection ID in object: "+ i, connectionId, id.getConnectionId());
        String actual = Long.toHexString(id.getValue());
        String expected = Long.toHexString(i);
        assertEquals("value of object: "+ i + " was: " + actual, expected, actual);
        assertEquals("value of object: "+ i + " was: " + actual, i, id.getSessionId());
    }
}
