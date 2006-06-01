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
package org.apache.activemq.tool;

import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Map;
import java.util.Arrays;

public class JmsProducerClient extends JmsPerfClientSupport {

    private ConnectionFactory factory = null;
    private String factoryClass = "";
    private String brokerUrl = "";
    private String[] destName  = null;

    private Destination[] dest  = null;
    private TextMessage message = null;

    public JmsProducerClient(ConnectionFactory factory, String destName) {
        this.factory  = factory;
        this.destName = new String[] {destName};
    }

    public JmsProducerClient(String factoryClass, String brokerUrl, String destName) {
        this.factoryClass = factoryClass;
        this.brokerUrl    = brokerUrl;
        this.destName     = new String[] {destName};
    }

    public JmsProducerClient(String brokerUrl, String destName) {
        this.brokerUrl = brokerUrl;
        this.destName  = new String[] {destName};
    }

    public JmsProducerClient(ConnectionFactory factory, String[] destName) {
        this.factory  = factory;
        this.destName = destName;
    }

    public JmsProducerClient(String factoryClass, String brokerUrl, String[] destName) {
        this.factoryClass = factoryClass;
        this.brokerUrl    = brokerUrl;
        this.destName     = destName;
    }

    public JmsProducerClient(String brokerUrl, String[] destName) {
        this.brokerUrl = brokerUrl;
        this.destName  = destName;
    }

    public void createProducer() throws JMSException {
        createProducer(0);
    }

    public void createProducer(Map settings) throws JMSException {
        createProducer(0, settings);
    }

    public void createProducer(int messageSize, Map settings) throws JMSException {
        addConfigParam(settings);
        createProducer(messageSize);
    }

    public void createProducer(int messageSize) throws JMSException {

        listener.onConfigStart();

        // Create connection factory
        if (factory != null) {
            createConnectionFactory(factory);
        } else if (factoryClass != null) {
            createConnectionFactory(factoryClass, brokerUrl);
        } else {
            createConnectionFactory(brokerUrl);
        }
        createConnectionFactory(brokerUrl);


        // Create destinations
        dest = new Destination[destName.length];
        for (int i=0; i<destName.length; i++) {
            if (destName[i].startsWith("topic://")) {
                dest[i] = createTopic(destName[i].substring("topic://".length()));
            } else if (destName[i].startsWith("queue://")) {
                dest[i] = createQueue(destName[i].substring("queue://".length()));
            } else {
                dest[i] = createQueue(destName[i]);
            }
        }

        // Create actual message producer
        if (dest.length > 1) {
            createMessageProducer(null);
        } else {
            createMessageProducer(dest[0]);
        }

        // Create message to sent
        if (messageSize > 0) {
            byte[] val = new byte[messageSize];
            Arrays.fill(val, (byte)0);
            String buff = new String(val);
            message = createTextMessage(buff);
        }

        listener.onConfigEnd();
    }

    public void sendCountBasedMessages(long messageCount) throws JMSException {
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance
        try {
            getConnection().start();
            // Send one type of message only, avoiding the creation of different messages on sending
            if (message != null) {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    listener.onPublishStart();
                    for (int i=0; i<messageCount; i++) {
                        for (int j=0; j<dest.length; j++) {
                            getMessageProducer().send(dest[j], message);
                        }
                    }
                    listener.onPublishEnd();
                // Send to only one actual destination
                } else {
                    listener.onPublishStart();
                    for (int i=0; i<messageCount; i++) {
                        getMessageProducer().send(message);
                    }
                    listener.onPublishEnd();
                }

            // Send different type of messages using indexing to identify each one.
            // Message size will vary. Definitely slower, since messages properties
            // will be set individually each send.
            } else {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    listener.onPublishStart();
                    for (int i=0; i<messageCount; i++) {
                        for (int j=0; j<dest.length; j++) {
                            getMessageProducer().send(dest[j], createTextMessage("Text Message [" + i + "]"));
                        }
                    }
                    listener.onPublishEnd();

                // Send to only one actual destination
                } else {
                    listener.onPublishStart();
                    for (int i=0; i<messageCount; i++) {
                        getMessageProducer().send(createTextMessage("Text Message [" + i + "]"));
                    }
                    listener.onPublishEnd();
                }
            }
        } finally {
            getConnection().close();
        }
    }

    public void sendTimeBasedMessages(long duration) throws JMSException {
        long endTime   = System.currentTimeMillis() + duration;
        // Parse through different ways to send messages
        // Avoided putting the condition inside the loop to prevent effect on performance

        // Send one type of message only, avoiding the creation of different messages on sending
        try {
            getConnection().start();

            if (message != null) {
                // Send to more than one actual destination
                if (dest.length > 1) {
                    listener.onPublishStart();
                    while (System.currentTimeMillis() < endTime) {
                        for (int j=0; j<dest.length; j++) {
                            getMessageProducer().send(dest[j], message);
                        }
                    }
                    listener.onPublishEnd();
                // Send to only one actual destination
                } else {
                    listener.onPublishStart();
                    while (System.currentTimeMillis() < endTime) {
                        getMessageProducer().send(message);
                    }
                    listener.onPublishEnd();
                }

            // Send different type of messages using indexing to identify each one.
            // Message size will vary. Definitely slower, since messages properties
            // will be set individually each send.
            } else {
                // Send to more than one actual destination
                long count = 1;
                if (dest.length > 1) {
                    listener.onPublishStart();
                    while (System.currentTimeMillis() < endTime) {
                        for (int j=0; j<dest.length; j++) {
                            getMessageProducer().send(dest[j], createTextMessage("Text Message [" + count++ + "]"));
                        }
                    }
                    listener.onPublishEnd();

                // Send to only one actual destination
                } else {
                    listener.onPublishStart();
                    while (System.currentTimeMillis() < endTime) {
                        getMessageProducer().send(createTextMessage("Text Message [" + count++ + "]"));
                    }
                    listener.onPublishEnd();
                }
            }
        } finally {
            getConnection().close();
        }
    }

    public static void main(String[] args) throws Exception {
        JmsProducerClient prod = new JmsProducerClient("org.apache.activemq.ActiveMQConnectionFactory", "tcp://localhost:61616", "topic://TEST.FOO");
        prod.setPerfEventListener(new PerfEventAdapter());
        prod.createProducer();
        prod.sendTimeBasedMessages(2000);
    }
}
