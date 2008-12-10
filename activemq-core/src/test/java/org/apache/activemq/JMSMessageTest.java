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

import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageEOFException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Test cases used to test the JMS message consumer.
 * 
 * @version $Revision$
 */
public class JMSMessageTest extends JmsTestSupport {

    public ActiveMQDestination destination;
    public int deliveryMode;
    public int prefetch;
    public int ackMode;
    public byte destinationType;
    public boolean durableConsumer;
    public String connectURL;

    /**
     * Run all these tests in both marshaling and non-marshaling mode.
     */
    public void initCombos() {
        addCombinationValues("connectURL", new Object[] {"vm://localhost?marshal=false",
                                                         "vm://localhost?marshal=true"});
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE)});
    }

    public void testTextMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message.
        {
            TextMessage message = session.createTextMessage();
            message.setText("Hi");
            producer.send(message);
        }

        // Check the Message
        {
            TextMessage message = (TextMessage)consumer.receive(1000);
            assertNotNull(message);
            assertEquals("Hi", message.getText());
        }

        assertNull(consumer.receiveNoWait());
    }

    public static Test suite() {
        return suite(JMSMessageTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected ConnectionFactory createConnectionFactory() throws URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectURL);
        return factory;
    }

    public void testBytesMessageLength() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message
        {
            BytesMessage message = session.createBytesMessage();
            message.writeInt(1);
            message.writeInt(2);
            message.writeInt(3);
            message.writeInt(4);
            producer.send(message);
        }

        // Check the message.
        {
            BytesMessage message = (BytesMessage)consumer.receive(1000);
            assertNotNull(message);
            assertEquals(16, message.getBodyLength());
        }

        assertNull(consumer.receiveNoWait());
    }

    public void testObjectMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // send the message.
        {
            ObjectMessage message = session.createObjectMessage();
            message.setObject("Hi");
            producer.send(message);
        }

        // Check the message
        {
            ObjectMessage message = (ObjectMessage)consumer.receive(1000);
            assertNotNull(message);
            assertEquals("Hi", message.getObject());
        }
        assertNull(consumer.receiveNoWait());
    }

    public void testBytesMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message
        {
            BytesMessage message = session.createBytesMessage();
            message.writeBoolean(true);
            producer.send(message);
        }

        // Check the message
        {
            BytesMessage message = (BytesMessage)consumer.receive(1000);
            assertNotNull(message);
            assertTrue(message.readBoolean());

            try {
                message.readByte();
                fail("Expected exception not thrown.");
            } catch (MessageEOFException e) {
            }

        }
        assertNull(consumer.receiveNoWait());
    }

    public void testStreamMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message.
        {
            StreamMessage message = session.createStreamMessage();
            message.writeString("This is a test to see how it works.");
            producer.send(message);
        }

        // Check the message.
        {
            StreamMessage message = (StreamMessage)consumer.receive(1000);
            assertNotNull(message);

            // Invalid conversion should throw exception and not move the stream
            // position.
            try {
                message.readByte();
                fail("Should have received NumberFormatException");
            } catch (NumberFormatException e) {
            }

            assertEquals("This is a test to see how it works.", message.readString());

            // Invalid conversion should throw exception and not move the stream
            // position.
            try {
                message.readByte();
                fail("Should have received MessageEOFException");
            } catch (MessageEOFException e) {
            }
        }
        assertNull(consumer.receiveNoWait());
    }

    public void testMapMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // send the message.
        {
            MapMessage message = session.createMapMessage();
            message.setBoolean("boolKey", true);
            producer.send(message);
        }

        // get the message.
        {
            MapMessage message = (MapMessage)consumer.receive(1000);
            assertNotNull(message);
            assertTrue(message.getBoolean("boolKey"));
        }
        assertNull(consumer.receiveNoWait());
    }

    static class ForeignMessage implements TextMessage {

        public int deliveryMode;

        private String messageId;
        private long timestamp;
        private String correlationId;
        private Destination replyTo;
        private Destination destination;
        private boolean redelivered;
        private String type;
        private long expiration;
        private int priority;
        private String text;
        private HashMap<String, Object> props = new HashMap<String, Object>();

        public String getJMSMessageID() throws JMSException {
            return messageId;
        }

        public void setJMSMessageID(String arg0) throws JMSException {
            messageId = arg0;
        }

        public long getJMSTimestamp() throws JMSException {
            return timestamp;
        }

        public void setJMSTimestamp(long arg0) throws JMSException {
            timestamp = arg0;
        }

        public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
            return null;
        }

        public void setJMSCorrelationIDAsBytes(byte[] arg0) throws JMSException {
        }

        public void setJMSCorrelationID(String arg0) throws JMSException {
            correlationId = arg0;
        }

        public String getJMSCorrelationID() throws JMSException {
            return correlationId;
        }

        public Destination getJMSReplyTo() throws JMSException {
            return replyTo;
        }

        public void setJMSReplyTo(Destination arg0) throws JMSException {
            replyTo = arg0;
        }

        public Destination getJMSDestination() throws JMSException {
            return destination;
        }

        public void setJMSDestination(Destination arg0) throws JMSException {
            destination = arg0;
        }

        public int getJMSDeliveryMode() throws JMSException {
            return deliveryMode;
        }

        public void setJMSDeliveryMode(int arg0) throws JMSException {
            deliveryMode = arg0;
        }

        public boolean getJMSRedelivered() throws JMSException {
            return redelivered;
        }

        public void setJMSRedelivered(boolean arg0) throws JMSException {
            redelivered = arg0;
        }

        public String getJMSType() throws JMSException {
            return type;
        }

        public void setJMSType(String arg0) throws JMSException {
            type = arg0;
        }

        public long getJMSExpiration() throws JMSException {
            return expiration;
        }

        public void setJMSExpiration(long arg0) throws JMSException {
            expiration = arg0;
        }

        public int getJMSPriority() throws JMSException {
            return priority;
        }

        public void setJMSPriority(int arg0) throws JMSException {
            priority = arg0;
        }

        public void clearProperties() throws JMSException {
        }

        public boolean propertyExists(String arg0) throws JMSException {
            return false;
        }

        public boolean getBooleanProperty(String arg0) throws JMSException {
            return false;
        }

        public byte getByteProperty(String arg0) throws JMSException {
            return 0;
        }

        public short getShortProperty(String arg0) throws JMSException {
            return 0;
        }

        public int getIntProperty(String arg0) throws JMSException {
            return 0;
        }

        public long getLongProperty(String arg0) throws JMSException {
            return 0;
        }

        public float getFloatProperty(String arg0) throws JMSException {
            return 0;
        }

        public double getDoubleProperty(String arg0) throws JMSException {
            return 0;
        }

        public String getStringProperty(String arg0) throws JMSException {
            return (String)props.get(arg0);
        }

        public Object getObjectProperty(String arg0) throws JMSException {
            return props.get(arg0);
        }

        public Enumeration getPropertyNames() throws JMSException {
            return new Vector<String>(props.keySet()).elements();
        }

        public void setBooleanProperty(String arg0, boolean arg1) throws JMSException {
        }

        public void setByteProperty(String arg0, byte arg1) throws JMSException {
        }

        public void setShortProperty(String arg0, short arg1) throws JMSException {
        }

        public void setIntProperty(String arg0, int arg1) throws JMSException {
        }

        public void setLongProperty(String arg0, long arg1) throws JMSException {
        }

        public void setFloatProperty(String arg0, float arg1) throws JMSException {
        }

        public void setDoubleProperty(String arg0, double arg1) throws JMSException {
        }

        public void setStringProperty(String arg0, String arg1) throws JMSException {
            props.put(arg0, arg1);
        }

        public void setObjectProperty(String arg0, Object arg1) throws JMSException {
            props.put(arg0, arg1);
        }

        public void acknowledge() throws JMSException {
        }

        public void clearBody() throws JMSException {
        }

        public void setText(String arg0) throws JMSException {
            text = arg0;
        }

        public String getText() throws JMSException {
            return text;
        }
    }

    public void testForeignMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the message.
        {
            ForeignMessage message = new ForeignMessage();
            message.text = "Hello";
            message.setStringProperty("test", "value");
            long timeToLive = 10000L;
            long start = System.currentTimeMillis();
            producer.send(message, Session.AUTO_ACKNOWLEDGE, 7, timeToLive);
            long end = System.currentTimeMillis();


            //validate jms spec 1.1 section 3.4.11 table 3.1
            // JMSDestination, JMSDeliveryMode,  JMSExpiration, JMSPriority, JMSMessageID, and JMSTimestamp
            //must be set by sending a message.
            assertEquals(destination, message.getJMSDestination());
            assertEquals(Session.AUTO_ACKNOWLEDGE, message.getJMSDeliveryMode());
            assertTrue(start  + timeToLive <= message.getJMSExpiration());
            assertTrue(end + timeToLive >= message.getJMSExpiration());
            assertEquals(7, message.getJMSPriority());
            assertNotNull(message.getJMSMessageID());
            assertTrue(start <= message.getJMSTimestamp());
            assertTrue(end >= message.getJMSTimestamp());
        }

        // Validate message is OK.
        {
            TextMessage message = (TextMessage)consumer.receive(1000);
            assertNotNull(message);
            assertEquals("Hello", message.getText());
            assertEquals("value", message.getStringProperty("test"));
        }

        assertNull(consumer.receiveNoWait());
    }

}
