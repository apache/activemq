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
package org.apache.activemq.bugs;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AMQ5381Test {

    public static final byte[] ORIG_MSG_CONTENT = randomByteArray();
    public static final String AMQ5381_EXCEPTION_MESSAGE = "java.util.zip.DataFormatException: incorrect header check";

    private BrokerService brokerService;
    private String brokerURI;

    @Rule public TestName name = new TestName();

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        brokerURI = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    private ActiveMQConnection createConnection(boolean useCompression) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
        factory.setUseCompression(useCompression);
        Connection connection = factory.createConnection();
        connection.start();
        return (ActiveMQConnection) connection;
    }

    @Test
    public void amq5381Test() throws Exception {

        // Consumer Configured for (useCompression=true)
        final ActiveMQConnection consumerConnection = createConnection(true);
        final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue consumerQueue = consumerSession.createQueue(name.getMethodName());
        final MessageConsumer consumer = consumerSession.createConsumer(consumerQueue);

        // Producer Configured for (useCompression=false)
        final ActiveMQConnection producerConnection = createConnection(false);
        final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue producerQueue = producerSession.createQueue(name.getMethodName());

        try {

            final ActiveMQBytesMessage messageProduced = (ActiveMQBytesMessage) producerSession.createBytesMessage();
            messageProduced.writeBytes(ORIG_MSG_CONTENT);
            Assert.assertFalse(messageProduced.isReadOnlyBody());

            Assert.assertFalse(
                "Produced Message's 'compressed' flag should remain false until the message is sent (where it will be compressed, if necessary)",
                messageProduced.isCompressed());

            final MessageProducer producer = producerSession.createProducer(null);
            producer.send(producerQueue, messageProduced);

            Assert.assertEquals("Once sent, the produced Message's 'compressed' flag should match its Connection's 'useCompression' flag",
                producerConnection.isUseCompression(), messageProduced.isCompressed());

            final ActiveMQBytesMessage messageConsumed = (ActiveMQBytesMessage) consumer.receive();
            Assert.assertNotNull(messageConsumed);
            Assert.assertTrue("Consumed Message should be read-only", messageConsumed.isReadOnlyBody());
            Assert.assertEquals("Consumed Message's 'compressed' flag should match the produced Message's 'compressed' flag",
                                messageProduced.isCompressed(), messageConsumed.isCompressed());

            // ensure consumed message content matches what was originally set
            final byte[] consumedMsgContent = new byte[(int) messageConsumed.getBodyLength()];
            messageConsumed.readBytes(consumedMsgContent);

            Assert.assertTrue("Consumed Message content should match the original Message content", Arrays.equals(ORIG_MSG_CONTENT, consumedMsgContent));

            // make message writable so the consumer can modify and reuse it
            makeWritable(messageConsumed);

            // modify message, attempt to trigger DataFormatException due
            // to old incorrect compression logic
            try {
                messageConsumed.setStringProperty(this.getClass().getName(), "test");
            } catch (JMSException jmsE) {
                if (AMQ5381_EXCEPTION_MESSAGE.equals(jmsE.getMessage())) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    jmsE.printStackTrace(pw);

                    Assert.fail("AMQ5381 Error State Achieved: attempted to decompress BytesMessage contents that are not compressed\n" + sw.toString());
                } else {
                    throw jmsE;
                }
            }

            Assert.assertEquals(
                "The consumed Message's 'compressed' flag should still match the produced Message's 'compressed' flag after it has been made writable",
                messageProduced.isCompressed(), messageConsumed.isCompressed());

            // simulate re-publishing message
            simulatePublish(messageConsumed);

            // ensure consumed message content matches what was originally set
            final byte[] modifiedMsgContent = new byte[(int) messageConsumed.getBodyLength()];
            messageConsumed.readBytes(modifiedMsgContent);

            Assert.assertTrue(
                "After the message properties are modified and it is re-published, its message content should still match the original message content",
                Arrays.equals(ORIG_MSG_CONTENT, modifiedMsgContent));
        } finally {
            producerSession.close();
            producerConnection.close();
            consumerSession.close();
            consumerConnection.close();
        }
    }

    protected static final int MAX_RANDOM_BYTE_ARRAY_SIZE_KB = 128;

    protected static byte[] randomByteArray() {
        final Random random = new Random();
        final byte[] byteArray = new byte[random.nextInt(MAX_RANDOM_BYTE_ARRAY_SIZE_KB * 1024)];
        random.nextBytes(byteArray);

        return byteArray;
    }

    protected static void makeWritable(final ActiveMQMessage message) {
        message.setReadOnlyBody(false);
        message.setReadOnlyProperties(false);
    }

    protected static void simulatePublish(final ActiveMQBytesMessage message) throws JMSException {
        message.reset();
        message.onSend();
    }
}
