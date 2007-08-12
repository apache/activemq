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
package org.apache.activemq.tool;

import java.util.Arrays;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.activemq.tool.properties.JmsClientProperties;
import org.apache.activemq.tool.properties.JmsProducerProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JmsProducerClient extends AbstractJmsMeasurableClient {
    private static final Log LOG = LogFactory.getLog(JmsProducerClient.class);

    protected JmsProducerProperties client;
    protected MessageProducer jmsProducer;
    protected TextMessage jmsTextMessage;

    public JmsProducerClient(ConnectionFactory factory) {
        this(new JmsProducerProperties(), factory);
    }

    public JmsProducerClient(JmsProducerProperties clientProps, ConnectionFactory factory) {
        super(factory);
        this.client = clientProps;
    }

    public void sendMessages() throws JMSException {
        // Send a specific number of messages
        if (client.getSendType().equalsIgnoreCase(JmsProducerProperties.COUNT_BASED_SENDING)) {
            sendCountBasedMessages(client.getSendCount());

        // Send messages for a specific duration
        } else {
            sendTimeBasedMessages(client.getSendDuration());
        }
    }

    public void sendMessages(int destCount) throws JMSException {
        this.destCount = destCount;
        sendMessages();
    }

    public void sendMessages(int destIndex, int destCount) throws JMSException {
        this.destIndex = destIndex;
        sendMessages(destCount);
    }

    public void sendCountBasedMessages(long messageCount) throws JMSException {
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance
        Destination[] dest = createDestination(destIndex, destCount);

        // Create a producer, if none is created.
        if (getJmsProducer() == null) {
            if (dest.length == 1) {
                createJmsProducer(dest[0]);
            } else {
                createJmsProducer();
            }
        }
        try {
            getConnection().start();
            LOG.info("Starting to publish " + client.getMessageSize() + " byte(s) of " + messageCount + " messages...");

            // Send one type of message only, avoiding the creation of different messages on sending
            if (!client.isCreateNewMsg()) {
                // Create only a single message
                createJmsTextMessage();

                // Send to more than one actual destination
                if (dest.length > 1) {
                    for (int i = 0; i < messageCount; i++) {
                        for (int j = 0; j < dest.length; j++) {
                            getJmsProducer().send(dest[j], getJmsTextMessage());
                            incThroughput();
                        }
                    }
                    // Send to only one actual destination
                } else {
                    for (int i = 0; i < messageCount; i++) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                    }
                }

                // Send different type of messages using indexing to identify each one.
                // Message size will vary. Definitely slower, since messages properties
                // will be set individually each send.
            } else {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    for (int i = 0; i < messageCount; i++) {
                        for (int j = 0; j < dest.length; j++) {
                            getJmsProducer().send(dest[j], createJmsTextMessage("Text Message [" + i + "]"));
                            incThroughput();
                        }
                    }

                    // Send to only one actual destination
                } else {
                    for (int i = 0; i < messageCount; i++) {
                        getJmsProducer().send(createJmsTextMessage("Text Message [" + i + "]"));
                        incThroughput();
                    }
                }
            }
        } finally {
            getConnection().close();
        }
    }

    public void sendTimeBasedMessages(long duration) throws JMSException {
        long endTime = System.currentTimeMillis() + duration;
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance

        Destination[] dest = createDestination(destIndex, destCount);

        // Create a producer, if none is created.
        if (getJmsProducer() == null) {
            if (dest.length == 1) {
                createJmsProducer(dest[0]);
            } else {
                createJmsProducer();
            }
        }

        try {
            getConnection().start();
            LOG.info("Starting to publish " + client.getMessageSize() + " byte(s) messages for " + duration + " ms");

            // Send one type of message only, avoiding the creation of different messages on sending
            if (!client.isCreateNewMsg()) {
                // Create only a single message
                createJmsTextMessage();

                // Send to more than one actual destination
                if (dest.length > 1) {
                    while (System.currentTimeMillis() < endTime) {
                        for (int j = 0; j < dest.length; j++) {
                            getJmsProducer().send(dest[j], getJmsTextMessage());
                            incThroughput();
                        }
                    }
                    // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                    }
                }

                // Send different type of messages using indexing to identify each one.
                // Message size will vary. Definitely slower, since messages properties
                // will be set individually each send.
            } else {
                // Send to more than one actual destination
                long count = 1;
                if (dest.length > 1) {
                    while (System.currentTimeMillis() < endTime) {
                        for (int j = 0; j < dest.length; j++) {
                            getJmsProducer().send(dest[j], createJmsTextMessage("Text Message [" + count++ + "]"));
                            incThroughput();
                        }
                    }

                    // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {

                        getJmsProducer().send(createJmsTextMessage("Text Message [" + count++ + "]"));
                        incThroughput();
                    }
                }
            }
        } finally {
            getConnection().close();
        }
    }

    public MessageProducer createJmsProducer() throws JMSException {
        jmsProducer = getSession().createProducer(null);
        if (client.getDeliveryMode().equalsIgnoreCase(JmsProducerProperties.DELIVERY_MODE_PERSISTENT)) {
            LOG.info("Creating producer to possible multiple destinations with persistent delivery.");
            jmsProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else if (client.getDeliveryMode().equalsIgnoreCase(JmsProducerProperties.DELIVERY_MODE_NON_PERSISTENT)) {
            LOG.info("Creating producer to possible multiple destinations with non-persistent delivery.");
            jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } else {
            LOG.warn("Unknown deliveryMode value. Defaulting to non-persistent.");
            jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }
        return jmsProducer;
    }

    public MessageProducer createJmsProducer(Destination dest) throws JMSException {
        jmsProducer = getSession().createProducer(dest);
        if (client.getDeliveryMode().equalsIgnoreCase(JmsProducerProperties.DELIVERY_MODE_PERSISTENT)) {
            LOG.info("Creating producer to: " + dest.toString() + " with persistent delivery.");
            jmsProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else if (client.getDeliveryMode().equalsIgnoreCase(JmsProducerProperties.DELIVERY_MODE_NON_PERSISTENT)) {
            LOG.info("Creating  producer to: " + dest.toString() + " with non-persistent delivery.");
            jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } else {
            LOG.warn("Unknown deliveryMode value. Defaulting to non-persistent.");
            jmsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }
        return jmsProducer;
    }

    public MessageProducer getJmsProducer() {
        return jmsProducer;
    }

    public TextMessage createJmsTextMessage() throws JMSException {
        return createJmsTextMessage(client.getMessageSize());
    }

    public TextMessage createJmsTextMessage(int size) throws JMSException {
        jmsTextMessage = getSession().createTextMessage(buildText("", size));
        return jmsTextMessage;
    }

    public TextMessage createJmsTextMessage(String text) throws JMSException {
        jmsTextMessage = getSession().createTextMessage(buildText(text, client.getMessageSize()));
        return jmsTextMessage;
    }

    public TextMessage getJmsTextMessage() {
        return jmsTextMessage;
    }

    public JmsClientProperties getClient() {
        return client;
    }

    public void setClient(JmsClientProperties clientProps) {
        client = (JmsProducerProperties)clientProps;
    }

    protected String buildText(String text, int size) {
        byte[] data = new byte[size - text.length()];
        Arrays.fill(data, (byte) 0);
        return text + new String(data);
    }
}
