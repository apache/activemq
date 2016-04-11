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

import com.google.common.base.Throwables;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.DataFormatException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class AMQ6244Test {

    public static final byte[] ORIG_MSG_CONTENT = randomByteArray();

    @Rule
    public TestName name = new TestName();

    @Rule
    public EmbeddedActiveMQBroker brokerRule = new EmbeddedActiveMQBroker();

    public AMQ6244Test() {
        brokerRule.setBrokerName(this.getClass().getName());
    }

    @Test
    public void bytesMsgCompressedFlagTest() throws Exception {
        final ActiveMQConnection compressionEnabledConnection = createConnection(brokerRule.getVmURL(), true);
        final ActiveMQConnection compressionDisabledConnection = createConnection(brokerRule.getVmURL(), false);

        // Consumer (compression=false)
        final Session consumerSession = compressionDisabledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = consumerSession.createQueue(name.getMethodName());
        final MessageConsumer consumer = consumerSession.createConsumer(destination);

        // Producer (compression=false)
        final Session compressionDisabledProducerSession = compressionDisabledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer compressionDisabledProducer = compressionDisabledProducerSession.createProducer(destination);

        // Producer (compression=true)
        final Session compressionEnabledProducerSession = compressionEnabledConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer compressionEnabledProducer = compressionEnabledProducerSession.createProducer(destination);

        try {
            /*
             * Publish a BytesMessage on the compressed connection
             */
            final ActiveMQBytesMessage originalCompressedMsg = (ActiveMQBytesMessage) compressionEnabledProducerSession.createBytesMessage();
            originalCompressedMsg.writeBytes(ORIG_MSG_CONTENT);
            Assert.assertFalse(originalCompressedMsg.isReadOnlyBody());

            // send first message
            compressionEnabledProducer.send(originalCompressedMsg);
            Assert.assertEquals(
                    "Once sent, the Message's 'compressed' flag should match the 'useCompression' flag on the Producer's Connection",
                    compressionEnabledConnection.isUseCompression(), originalCompressedMsg.isCompressed());

            /*
             * Consume the compressed message and resend it decompressed
             */
            final ActiveMQBytesMessage compressedMsg = receiveMsg(consumer, originalCompressedMsg);
            validateMsgContent(compressedMsg);

            // make message writable so the client can reuse it
            makeWritable(compressedMsg);
            compressedMsg.setStringProperty(this.getClass().getName(), "test");
            compressionDisabledProducer.send(compressedMsg);

            /*
             * AMQ-6244 ERROR STATE 1: Produced Message is marked 'compressed' when its contents are not compressed
             */
            Assert.assertEquals(
                    "AMQ-6244 Error State Achieved: Produced Message's 'compressed' flag is enabled after message is published on a connection with 'useCompression=false'",
                    compressionDisabledConnection.isUseCompression(), compressedMsg.isCompressed());

            /*
             * AMQ-6244 ERROR STATE 2: Consumer cannot handle Message marked 'compressed' when its contents are not compressed
             */
            try {
                final ActiveMQBytesMessage uncompressedMsg = receiveMsg(consumer, compressedMsg);
                validateMsgContent(uncompressedMsg);
            } catch (JMSException jmsE) {
                final Throwable rootCause = Throwables.getRootCause(jmsE);

                if (rootCause instanceof DataFormatException || rootCause instanceof NegativeArraySizeException) {
                    final StringWriter sw = new StringWriter();
                    final PrintWriter pw = new PrintWriter(sw);

                    jmsE.printStackTrace(pw);

                    Assert.fail(
                            "AMQ-6244 Error State Achieved: Attempted to decompress BytesMessage contents that are not compressed\n" + sw
                                    .toString());
                } else {
                    throw jmsE;
                }
            }
        } finally {
            compressionEnabledProducerSession.close();
            compressionEnabledConnection.close();
            consumerSession.close();
            compressionDisabledProducerSession.close();
            compressionDisabledConnection.close();
        }
    }

    private ActiveMQBytesMessage receiveMsg(final MessageConsumer consumer, final ActiveMQMessage sentMessage) throws JMSException {
        // receive the message
        final ActiveMQBytesMessage message = (ActiveMQBytesMessage) consumer.receive();
        Assert.assertNotNull(message);
        Assert.assertTrue("Consumed Message should be read-only", message.isReadOnlyBody());
        Assert.assertEquals("Consumed Message's 'compressed' flag should match the produced Message's 'compressed' flag",
                            sentMessage.isCompressed(), message.isCompressed());

        return message;
    }

    private void validateMsgContent(final ActiveMQBytesMessage message) throws JMSException {
        // ensure consumed message content matches what was originally set
        final byte[] msgContent = new byte[(int) message.getBodyLength()];
        message.readBytes(msgContent);

        Assert.assertTrue("Consumed Message content should match the original Message content",
                          Arrays.equals(ORIG_MSG_CONTENT, msgContent));
    }

    protected static ActiveMQConnection createConnection(final String URL, final boolean useCompression) throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URL);
        factory.setUseCompression(useCompression);
        Connection connection = factory.createConnection();
        connection.start();
        return (ActiveMQConnection) connection;
    }

    protected static byte[] randomByteArray() {
        final Random random = new Random();
        final byte[] byteArray = new byte[random.nextInt(10 * 1024)];
        random.nextBytes(byteArray);

        return byteArray;
    }

    protected static void makeWritable(final ActiveMQMessage message) {
        message.setReadOnlyBody(false);
        message.setReadOnlyProperties(false);
    }
}
