/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.openwire;

import org.apache.activemq.command.ActiveMQDestination;
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
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.openwire.v1.ActiveMQTextMessageTest;
import org.apache.activemq.openwire.v1.BrokerInfoTest;
import org.apache.activemq.openwire.v1.MessageAckTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import junit.framework.TestCase;

public abstract class DataFileGeneratorTestSupport extends TestCase {

    static final File moduleBaseDir;
    static final File controlDir;
    static final File classFileDir;
    private static Throwable singletonException = new Exception("shared exception");

    static {
        File basedir = null;
        try {
            URL resource = DataFileGeneratorTestSupport.class.getResource("DataFileGeneratorTestSupport.class");
            URI baseURI = new URI(resource.toString()).resolve("../../../../..");
            basedir = new File(baseURI).getCanonicalFile();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        moduleBaseDir = basedir;
        controlDir = new File(moduleBaseDir, "src/test/resources/openwire-control");
        classFileDir = new File(moduleBaseDir, "src/test/java/org/activemq/openwire");
    }

    private int counter;
    private OpenWireFormat openWireformat;

    public void XXXX_testControlFileIsValid() throws Exception {
        generateControlFile();
        assertControlFileIsEqual();
    }
    
    public void testGenerateAndReParsingIsTheSame() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(buffer);
        Object expected = createObject();
        System.out.println("Created: " + expected);
        openWireformat.marshal(expected, ds);
        ds.close();
        
        // now lets try parse it back again
        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        Object actual = openWireformat.unmarshal(dis);
        
        System.out.println("Parsed: " + actual);
        
        assertEquals("Objects should be equal", expected.toString(), actual.toString());
        
        // TODO generate a property based equality method?
    }

    protected void setUp() throws Exception {
        super.setUp();
        openWireformat = createOpenWireFormat();
    }

    public void generateControlFile() throws Exception {
        controlDir.mkdirs();
        File dataFile = new File(controlDir, getClass().getName() + ".bin");

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
        File dataFile = new File(controlDir, getClass().getName() + ".bin");
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
        }
        finally {
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
        wf.setVersion(1);
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
        ActiveMQTextMessage message = (ActiveMQTextMessage) ActiveMQTextMessageTest.SINGLETON.createObject();
        message.setText(string);
        return message;
    }

    protected BrokerInfo createBrokerInfo(String string) throws Exception {
        return (BrokerInfo) BrokerInfoTest.SINGLETON.createObject();
    }

    protected MessageAck createMessageAck(String string) throws Exception {
        return (MessageAck) MessageAckTest.SINGLETON.createObject();
    }

    protected DataStructure createDataStructure(String string) throws Exception {
        return createBrokerInfo(string);
    }

    protected Throwable createThrowable(String string) {
        // we have issues with stack frames not being equal so share the same
        // exception each time
        return singletonException;
    }
}
