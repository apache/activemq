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

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.openwire.v1.ActiveMQTextMessageTest;
import org.apache.activemq.openwire.v1.BrokerInfoTest;
import org.apache.activemq.openwire.v1.MessageAckTest;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.util.ByteSequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class DataFileGeneratorTestSupport extends TestSupport {

    protected static final Object[] EMPTY_ARGUMENTS = {};
    private static final Log LOG = LogFactory.getLog(DataFileGeneratorTestSupport.class);

    private static final Throwable SINGLETON_EXCEPTION = new Exception("shared exception");
    private static final File MODULE_BASE_DIR;
    private static final File CONTROL_DIR;


    static {
        File basedir = null;
        try {
            URL resource = DataFileGeneratorTestSupport.class.getResource("DataFileGeneratorTestSupport.class");
            URI baseURI = new URI(resource.toString()).resolve("../../../../..");
            basedir = new File(baseURI).getCanonicalFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MODULE_BASE_DIR = basedir;
        CONTROL_DIR = new File(MODULE_BASE_DIR, "src/test/resources/openwire-control");
    }

    private int counter;
    private OpenWireFormat openWireformat;

    public void xtestControlFileIsValid() throws Exception {
        generateControlFile();
        assertControlFileIsEqual();
    }

    public void testGenerateAndReParsingIsTheSame() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(buffer);
        Object expected = createObject();
        LOG.info("Created: " + expected);
        openWireformat.marshal(expected, ds);
        ds.close();

        // now lets try parse it back again
        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        Object actual = openWireformat.unmarshal(dis);


        assertBeansEqual("", new HashSet<Object>(), expected, actual);
        LOG.info("Parsed: " + actual);
    }

    protected void assertBeansEqual(String message, Set<Object> comparedObjects, Object expected, Object actual) throws Exception {
        assertNotNull("Actual object should be equal to: " + expected + " but was null", actual);
        if (comparedObjects.contains(expected)) {
            return;
        }
        comparedObjects.add(expected);
        Class<? extends Object> type = expected.getClass();
        assertEquals("Should be of same type", type, actual.getClass());
        BeanInfo beanInfo = Introspector.getBeanInfo(type);
        PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
        for (int i = 0; i < descriptors.length; i++) {
            PropertyDescriptor descriptor = descriptors[i];
            Method method = descriptor.getReadMethod();
            if (method != null) {
                String name = descriptor.getName();
                Object expectedValue = null;
                Object actualValue = null;
                try {
                    expectedValue = method.invoke(expected, EMPTY_ARGUMENTS);
                    actualValue = method.invoke(actual, EMPTY_ARGUMENTS);
                } catch (Exception e) {
                    LOG.info("Failed to access property: " + name);
                }
                assertPropertyValuesEqual(message + name, comparedObjects, expectedValue, actualValue);
            }
        }
    }

    protected void assertPropertyValuesEqual(String name, Set<Object> comparedObjects, Object expectedValue, Object actualValue) throws Exception {
        String message = "Property " + name + " not equal";
        if (expectedValue == null) {
            assertNull("Property " + name + " should be null", actualValue);
        } else if (expectedValue instanceof Object[]) {
            assertArrayEqual(message, comparedObjects, (Object[])expectedValue, (Object[])actualValue);
        } else if (expectedValue.getClass().isArray()) {
            assertPrimitiveArrayEqual(message, comparedObjects, expectedValue, actualValue);
        } else {
            if (expectedValue instanceof Exception) {
                assertExceptionsEqual(message, (Exception)expectedValue, actualValue);
            } else if (expectedValue instanceof ByteSequence) {
                assertByteSequencesEqual(message, (ByteSequence)expectedValue, actualValue);
            } else if (expectedValue instanceof DataStructure) {
                assertBeansEqual(message + name, comparedObjects, expectedValue, actualValue);
            } else if (expectedValue instanceof Enumeration) {
            	assertEnumerationEqual(message + name, comparedObjects, (Enumeration)expectedValue, (Enumeration)actualValue);
            } else {
                assertEquals(message, expectedValue, actualValue);
            }

        }
    }

    protected void assertArrayEqual(String message, Set<Object> comparedObjects, Object[] expected, Object[] actual) throws Exception {
        assertEquals(message + ". Array length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertPropertyValuesEqual(message + ". element: " + i, comparedObjects, expected[i], actual[i]);
        }
    }
    
    protected void assertEnumerationEqual(String message, Set<Object> comparedObjects, Enumeration expected, Enumeration actual) throws Exception {
        while (expected.hasMoreElements()) {
        	Object expectedElem = expected.nextElement();
        	Object actualElem = actual.nextElement();
        	assertPropertyValuesEqual(message + ". element: " + expectedElem, comparedObjects, expectedElem, actualElem);
        }
    }

    protected void assertPrimitiveArrayEqual(String message, Set<Object> comparedObjects, Object expected, Object actual) throws ArrayIndexOutOfBoundsException, IllegalArgumentException,
        Exception {
        int length = Array.getLength(expected);
        assertEquals(message + ". Array length", length, Array.getLength(actual));
        for (int i = 0; i < length; i++) {
            assertPropertyValuesEqual(message + ". element: " + i, comparedObjects, Array.get(expected, i), Array.get(actual, i));
        }
    }

    protected void assertByteSequencesEqual(String message, ByteSequence expected, Object actualValue) {
        assertTrue(message + ". Actual value should be a ByteSequence but was: " + actualValue, actualValue instanceof ByteSequence);
        ByteSequence actual = (ByteSequence)actualValue;
        int length = expected.getLength();
        assertEquals(message + ". Length", length, actual.getLength());
        int offset = expected.getOffset();
        assertEquals(message + ". Offset", offset, actual.getOffset());
        byte[] data = expected.getData();
        byte[] actualData = actual.getData();
        for (int i = 0; i < length; i++) {
            assertEquals(message + ". Offset " + i, data[offset + i], actualData[offset + i]);
        }
    }

    protected void assertExceptionsEqual(String message, Exception expected, Object actualValue) {
        assertTrue(message + ". Actual value should be an exception but was: " + actualValue, actualValue instanceof Exception);
        Exception actual = (Exception)actualValue;
        assertEquals(message, expected.getMessage(), actual.getMessage());
    }

    protected void setUp() throws Exception {
        super.setUp();
        openWireformat = createOpenWireFormat();
    }

    public void generateControlFile() throws Exception {
        CONTROL_DIR.mkdirs();
        File dataFile = new File(CONTROL_DIR, getClass().getName() + ".bin");

        FileOutputStream os = new FileOutputStream(dataFile);
        DataOutputStream ds = new DataOutputStream(os);
        openWireformat.marshal(createObject(), ds);
        ds.close();
    }

    public InputStream generateInputStream() throws Exception {

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(os);
        openWireformat.marshal(createObject(), ds);
        ds.close();

        return new ByteArrayInputStream(os.toByteArray());
    }

    public void assertControlFileIsEqual() throws Exception {
        File dataFile = new File(CONTROL_DIR, getClass().getName() + ".bin");
        FileInputStream is1 = new FileInputStream(dataFile);
        int pos = 0;
        try {
            InputStream is2 = generateInputStream();
            int a = is1.read();
            int b = is2.read();
            pos++;
            assertEquals("Data does not match control file: " + dataFile + " at byte position " + pos, a, b);
            while (a >= 0 && b >= 0) {
                a = is1.read();
                b = is2.read();
                pos++;
                assertEquals("Data does not match control file: " + dataFile + " at byte position " + pos, a, b);
            }
            is2.close();
        } finally {
            is1.close();
        }
    }

    protected abstract Object createObject() throws Exception;

    protected void populateObject(Object info) throws Exception {
        // empty method to allow derived classes to call super
        // to simplify generated code
    }

    protected OpenWireFormat createOpenWireFormat() {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(true);
        wf.setStackTraceEnabled(false);
        wf.setVersion(OpenWireFormat.DEFAULT_VERSION);
        return wf;
    }

    protected BrokerId createBrokerId(String text) {
        return new BrokerId(text);
    }

    protected TransactionId createTransactionId(String string) {
        return new LocalTransactionId(createConnectionId(string), ++counter);
    }

    protected ConnectionId createConnectionId(String string) {
        return new ConnectionId(string);
    }

    protected SessionId createSessionId(String string) {
        return new SessionId(createConnectionId(string), ++counter);
    }

    protected ProducerId createProducerId(String string) {
        return new ProducerId(createSessionId(string), ++counter);
    }

    protected ConsumerId createConsumerId(String string) {
        return new ConsumerId(createSessionId(string), ++counter);
    }

    protected MessageId createMessageId(String string) {
        return new MessageId(createProducerId(string), ++counter);
    }

    protected ActiveMQDestination createActiveMQDestination(String string) {
        return new ActiveMQQueue(string);
    }

    protected Message createMessage(String string) throws Exception {
        ActiveMQTextMessage message = (ActiveMQTextMessage)ActiveMQTextMessageTest.SINGLETON.createObject();
        message.setText(string);
        return message;
    }

    protected BrokerInfo createBrokerInfo(String string) throws Exception {
        return (BrokerInfo)BrokerInfoTest.SINGLETON.createObject();
    }

    protected MessageAck createMessageAck(String string) throws Exception {
        return (MessageAck)MessageAckTest.SINGLETON.createObject();
    }

    protected DataStructure createDataStructure(String string) throws Exception {
        return createBrokerInfo(string);
    }

    protected Throwable createThrowable(String string) {
        // we have issues with stack frames not being equal so share the same
        // exception each time
        return SINGLETON_EXCEPTION;
    }

    protected BooleanExpression createBooleanExpression(String string) {
        return new NetworkBridgeFilter(new BrokerId(string), 10);
    }

}
