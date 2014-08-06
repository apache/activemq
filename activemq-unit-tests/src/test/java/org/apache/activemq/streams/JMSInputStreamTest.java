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
package org.apache.activemq.streams;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQInputStream;
import org.apache.activemq.ActiveMQOutputStream;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * JMSInputStreamTest
 */
@Deprecated
public class JMSInputStreamTest extends JmsTestSupport {

    public Destination destination;
    protected DataOutputStream out;
    protected DataInputStream in;
    private ActiveMQConnection connection2;

    private ActiveMQInputStream amqIn;
    private ActiveMQOutputStream amqOut;

    public static Test suite() {
        return suite(JMSInputStreamTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombos() {
        addCombinationValues("destination", new Object[] { new ActiveMQQueue("TEST.QUEUE"), new ActiveMQTopic("TEST.TOPIC") });
    }

    @Override
    protected void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

    private void setUpConnection(Map<String, Object> props, long timeout) throws JMSException {
        connection2 = (ActiveMQConnection) factory.createConnection(userName, password);
        connections.add(connection2);
        if (props != null) {
            amqOut = (ActiveMQOutputStream) connection.createOutputStream(destination, props, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        } else {
            amqOut = (ActiveMQOutputStream) connection.createOutputStream(destination);
        }

        out = new DataOutputStream(amqOut);
        if (timeout == -1) {
            amqIn = (ActiveMQInputStream) connection2.createInputStream(destination);
        } else {
            amqIn = (ActiveMQInputStream) connection2.createInputStream(destination, null, false, timeout);
        }
        in = new DataInputStream(amqIn);
    }

    /*
     * @see TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test for AMQ-3010
     */
    public void testInputStreamTimeout() throws Exception {
        long timeout = 500;

        setUpConnection(null, timeout);
        try {
            in.read();
            fail();
        } catch (ActiveMQInputStream.ReadTimeoutException e) {
            // timeout reached, everything ok
        }
        in.close();
    }

    // Test for AMQ-2988
    public void testStreamsWithProperties() throws Exception {
        String name1 = "PROPERTY_1";
        String name2 = "PROPERTY_2";
        String value1 = "VALUE_1";
        String value2 = "VALUE_2";
        Map<String, Object> jmsProperties = new HashMap<String, Object>();
        jmsProperties.put(name1, value1);
        jmsProperties.put(name2, value2);
        setUpConnection(jmsProperties, -1);

        out.writeInt(4);
        out.flush();
        assertTrue(in.readInt() == 4);
        out.writeFloat(2.3f);
        out.flush();
        assertTrue(in.readFloat() == 2.3f);
        String str = "this is a test string";
        out.writeUTF(str);
        out.flush();
        assertTrue(in.readUTF().equals(str));
        for (int i = 0; i < 100; i++) {
            out.writeLong(i);
        }
        out.flush();

        // check properties before we try to read the stream
        checkProperties(jmsProperties);

        for (int i = 0; i < 100; i++) {
            assertTrue(in.readLong() == i);
        }

        // check again after read was done
        checkProperties(jmsProperties);
    }

    public void testStreamsWithPropertiesOnlyOnFirstMessage() throws Exception {
        String name1 = "PROPERTY_1";
        String name2 = "PROPERTY_2";
        String value1 = "VALUE_1";
        String value2 = "VALUE_2";
        Map<String, Object> jmsProperties = new HashMap<String, Object>();
        jmsProperties.put(name1, value1);
        jmsProperties.put(name2, value2);

        ActiveMQDestination dest = (ActiveMQDestination) destination;

        if (dest.isQueue()) {
            destination = new ActiveMQQueue(dest.getPhysicalName() + "?producer.addPropertiesOnFirstMsgOnly=true");
        } else {
            destination = new ActiveMQTopic(dest.getPhysicalName() + "?producer.addPropertiesOnFirstMsgOnly=true");
        }

        setUpConnection(jmsProperties, -1);

        assertTrue(amqOut.isAddPropertiesOnFirstMsgOnly());

        out.writeInt(4);
        out.flush();
        assertTrue(in.readInt() == 4);
        out.writeFloat(2.3f);
        out.flush();
        assertTrue(in.readFloat() == 2.3f);
        String str = "this is a test string";
        out.writeUTF(str);
        out.flush();
        assertTrue(in.readUTF().equals(str));
        for (int i = 0; i < 100; i++) {
            out.writeLong(i);
        }
        out.flush();

        // check properties before we try to read the stream
        checkProperties(jmsProperties);

        for (int i = 0; i < 100; i++) {
            assertTrue(in.readLong() == i);
        }

        // check again after read was done
        checkProperties(jmsProperties);
    }

    // check if the received stream has the properties set
    // Test for AMQ-2988
    private void checkProperties(Map<String, Object> jmsProperties) throws IOException {
        Map<String, Object> receivedJmsProps = amqIn.getJMSProperties();

        // we should at least have the same amount or more properties
        assertTrue(jmsProperties.size() <= receivedJmsProps.size());

        // check the properties to see if we have everything in there
        Iterator<String> propsIt = jmsProperties.keySet().iterator();
        while (propsIt.hasNext()) {
            String key = propsIt.next();
            assertTrue(receivedJmsProps.containsKey(key));
            assertEquals(jmsProperties.get(key), receivedJmsProps.get(key));
        }
    }

    public void testLarge() throws Exception {
        setUpConnection(null, -1);

        final int testData = 23;
        final int dataLength = 4096;
        final int count = 1024;
        byte[] data = new byte[dataLength];
        for (int i = 0; i < data.length; i++) {
            data[i] = testData;
        }
        final AtomicBoolean complete = new AtomicBoolean(false);
        Thread runner = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int x = 0; x < count; x++) {
                        byte[] b = new byte[2048];
                        in.readFully(b);
                        for (int i = 0; i < b.length; i++) {
                            assertTrue(b[i] == testData);
                        }
                    }
                    complete.set(true);
                    synchronized (complete) {
                        complete.notify();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        runner.start();
        for (int i = 0; i < count; i++) {
            out.write(data);
        }
        out.flush();
        synchronized (complete) {
            if (!complete.get()) {
                complete.wait(30000);
            }
        }
        assertTrue(complete.get());
    }

    public void testStreams() throws Exception {
        setUpConnection(null, -1);
        out.writeInt(4);
        out.flush();
        assertTrue(in.readInt() == 4);
        out.writeFloat(2.3f);
        out.flush();
        assertTrue(in.readFloat() == 2.3f);
        String str = "this is a test string";
        out.writeUTF(str);
        out.flush();
        assertTrue(in.readUTF().equals(str));
        for (int i = 0; i < 100; i++) {
            out.writeLong(i);
        }
        out.flush();

        for (int i = 0; i < 100; i++) {
            assertTrue(in.readLong() == i);
        }
    }
}
