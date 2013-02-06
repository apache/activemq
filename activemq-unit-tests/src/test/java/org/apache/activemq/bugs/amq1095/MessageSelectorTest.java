/* ====================================================================
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
==================================================================== */

package org.apache.activemq.bugs.amq1095;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * <p>
 * Test cases for various ActiveMQ functionalities.
 * </p>
 *
 * <ul>
 * <li>
 * <p>
 * Durable subscriptions are used.
 * </p>
 * </li>
 * <li>
 * <p>
 * The Kaha persistence manager is used.
 * </p>
 * </li>
 * <li>
 * <p>
 * An already existing Kaha directory is used. Everything runs fine if the
 * ActiveMQ broker creates a new Kaha directory.
 * </p>
 * </li>
 * </ul>
 *
 * @author Rainer Klute <a
 *         href="mailto:rainer.klute@dp-itsolutions.de">&lt;rainer.klute@dp-itsolutions.de&gt;</a>
 * @since 2007-08-09
 * @version $Id: MessageSelectorTest.java 12 2007-08-14 12:02:02Z rke $
 */
public class MessageSelectorTest extends ActiveMQTestCase {

    private MessageConsumer consumer1;
    private MessageConsumer consumer2;

    /** <p>Constructor</p> */
    public MessageSelectorTest()
    {}

    /** <p>Constructor</p>
     * @param name the test case's name
     */
    public MessageSelectorTest(final String name)
    {
        super(name);
    }

    /**
     * <p>
     * Tests whether message selectors work for durable subscribers.
     * </p>
     */
    public void testMessageSelectorForDurableSubscribersRunA()
    {
        runMessageSelectorTest(true);
    }

    /**
     * <p>
     * Tests whether message selectors work for durable subscribers.
     * </p>
     */
    public void testMessageSelectorForDurableSubscribersRunB()
    {
        runMessageSelectorTest(true);
    }

    /**
     * <p>
     * Tests whether message selectors work for non-durable subscribers.
     * </p>
     */
    public void testMessageSelectorForNonDurableSubscribers()
    {
        runMessageSelectorTest(false);
    }

    /**
     * <p>
     * Tests whether message selectors work. This is done by sending two
     * messages to a topic. Both have an int property with different values. Two
     * subscribers use message selectors to receive the messages. Each one
     * should receive exactly one of the messages.
     * </p>
     */
    private void runMessageSelectorTest(final boolean isDurableSubscriber)
    {
        try
        {
            final String PROPERTY_CONSUMER = "consumer";
            final String CONSUMER_1 = "Consumer 1";
            final String CONSUMER_2 = "Consumer 2";
            final String MESSAGE_1 = "Message to " + CONSUMER_1;
            final String MESSAGE_2 = "Message to " + CONSUMER_2;

            assertNotNull(connection);
            assertNotNull(destination);

            final Session producingSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = producingSession.createProducer(destination);

            final Session consumingSession1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Session consumingSession2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            if (isDurableSubscriber)
            {
                consumer1 = consumingSession1.createDurableSubscriber
                    ((Topic) destination, CONSUMER_1, PROPERTY_CONSUMER + " = 1", false);
                consumer2 = consumingSession2.createDurableSubscriber
                    ((Topic) destination, CONSUMER_2, PROPERTY_CONSUMER + " = 2", false);
            }
            else
            {
                consumer1 = consumingSession1.createConsumer(destination, PROPERTY_CONSUMER + " = 1");
                consumer2 = consumingSession2.createConsumer(destination, PROPERTY_CONSUMER + " = 2");
            }
            registerToBeEmptiedOnShutdown(consumer1);
            registerToBeEmptiedOnShutdown(consumer2);

            connection.start();

            TextMessage msg1;
            TextMessage msg2;
            int propertyValue;
            String contents;

            /* Try to receive any messages from the consumers. There shouldn't be any yet. */
            msg1 = (TextMessage) consumer1.receive(RECEIVE_TIMEOUT);
            if (msg1 != null)
            {
                final StringBuffer msg = new StringBuffer("The consumer read a message that was left over from a former ActiveMQ broker run.");
                propertyValue = msg1.getIntProperty(PROPERTY_CONSUMER);
                contents = msg1.getText();
                if (propertyValue != 1) // Is the property value as expected?
                {
                    msg.append(" That message does not match the consumer's message selector.");
                    fail(msg.toString());
                }
                assertEquals(1, propertyValue);
                assertEquals(MESSAGE_1, contents);
            }
            msg2 = (TextMessage) consumer2.receive(RECEIVE_TIMEOUT);
            if (msg2 != null)
            {
                final StringBuffer msg = new StringBuffer("The consumer read a message that was left over from a former ActiveMQ broker run.");
                propertyValue = msg2.getIntProperty(PROPERTY_CONSUMER);
                contents = msg2.getText();
                if (propertyValue != 2) // Is the property value as expected?
                {
                    msg.append(" That message does not match the consumer's message selector.");
                    fail(msg.toString());
                }
                assertEquals(2, propertyValue);
                assertEquals(MESSAGE_2, contents);
            }

            /* Send two messages. Each is targeted at one of the consumers. */
            TextMessage msg;
            msg = producingSession.createTextMessage();
            msg.setText(MESSAGE_1);
            msg.setIntProperty(PROPERTY_CONSUMER, 1);
            producer.send(msg);

            msg = producingSession.createTextMessage();
            msg.setText(MESSAGE_2);
            msg.setIntProperty(PROPERTY_CONSUMER, 2);
            producer.send(msg);

            /* Receive the messages that have just been sent. */

            /* Use consumer 1 to receive one of the messages. The receive()
             * method is called twice to make sure there is nothing else in
             * stock for this consumer. */
            msg1 = (TextMessage) consumer1.receive(RECEIVE_TIMEOUT);
            assertNotNull(msg1);
            propertyValue = msg1.getIntProperty(PROPERTY_CONSUMER);
            contents = msg1.getText();
            assertEquals(1, propertyValue);
            assertEquals(MESSAGE_1, contents);
            msg1 = (TextMessage) consumer1.receive(RECEIVE_TIMEOUT);
            assertNull(msg1);

            /* Use consumer 2 to receive the other message. The receive()
             * method is called twice to make sure there is nothing else in
             * stock for this consumer. */
            msg2 = (TextMessage) consumer2.receive(RECEIVE_TIMEOUT);
            assertNotNull(msg2);
            propertyValue = msg2.getIntProperty(PROPERTY_CONSUMER);
            contents = msg2.getText();
            assertEquals(2, propertyValue);
            assertEquals(MESSAGE_2, contents);
            msg2 = (TextMessage) consumer2.receive(RECEIVE_TIMEOUT);
            assertNull(msg2);
        }
        catch (JMSException ex)
        {
            ex.printStackTrace();
            fail();
        }
    }

}
