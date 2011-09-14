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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.Arrays;
import java.util.Set;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.activemq.tool.properties.JmsClientProperties;
import org.apache.activemq.tool.properties.JmsProducerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsProducerClient extends AbstractJmsMeasurableClient {
    private static final Logger LOG = LoggerFactory.getLogger(JmsProducerClient.class);

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
            if (client.getMsgFileName() != null) {
            	LOG.info("Starting to publish " +
            		messageCount + 
            		" messages from file " + 
            		client.getMsgFileName()
            	);
            } else {
            	LOG.info("Starting to publish " +
            		messageCount +
            		" messages of size " +
            		client.getMessageSize() + 
            		" byte(s)." 
            	);
            }

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
                            sleep();
                            commitTxIfNecessary();
                        }
                    }
                    // Send to only one actual destination
                } else {
                    for (int i = 0; i < messageCount; i++) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                        sleep();
                        commitTxIfNecessary();
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
                            sleep();
                            commitTxIfNecessary();
                        }
                    }

                    // Send to only one actual destination
                } else {
                    for (int i = 0; i < messageCount; i++) {
                        getJmsProducer().send(createJmsTextMessage("Text Message [" + i + "]"));
                        incThroughput();
                        sleep();
                        commitTxIfNecessary();
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
            if (client.getMsgFileName() != null) {
            	LOG.info("Starting to publish messages from file " + 
            			client.getMsgFileName() + 
            			" for " +
            			duration + 
            			" ms");
            } else {
            	LOG.info("Starting to publish " + 
            			client.getMessageSize() + 
            			" byte(s) messages for " + 
            			duration + 
            			" ms");
            }
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
                            sleep();
                            commitTxIfNecessary();
                        }
                    }
                    // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {
                        getJmsProducer().send(getJmsTextMessage());
                        incThroughput();
                        sleep();
                        commitTxIfNecessary();
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
                            sleep();
                            commitTxIfNecessary();
                        }
                    }

                    // Send to only one actual destination
                } else {
                    while (System.currentTimeMillis() < endTime) {

                        getJmsProducer().send(createJmsTextMessage("Text Message [" + count++ + "]"));
                        incThroughput();
                        sleep();
                        commitTxIfNecessary();
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
    	if (client.getMsgFileName() != null) {
    		return loadJmsMessage();
    	} else {
          return createJmsTextMessage(client.getMessageSize());
    	}
    }

    public TextMessage createJmsTextMessage(int size) throws JMSException {
        jmsTextMessage = getSession().createTextMessage(buildText("", size));
        
        // support for adding message headers
        Set<String> headerKeys = this.client.getHeaderKeys();
        for (String key : headerKeys) {
        	jmsTextMessage.setObjectProperty(key, this.client.getHeaderValue(key));
        }
        
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
    
    protected void sleep() {
        if (client.getSendDelay() > 0) {
        	try {
        		LOG.trace("Sleeping for " + client.getSendDelay() + " milliseconds");
        		Thread.sleep(client.getSendDelay());
        	} catch (java.lang.InterruptedException ex) {
        		LOG.warn(ex.getMessage());
        	}
        }
    }
    
    /**
     * loads the message to be sent from the specified TextFile
     */
    protected TextMessage loadJmsMessage() throws JMSException {
    	try {
    		// couple of sanity checks upfront 
    		if (client.getMsgFileName() == null) {
    			throw new JMSException("Invalid filename specified.");
    		}
    		
    		File f = new File(client.getMsgFileName());
    		if (f.isDirectory()) {
    			throw new JMSException("Cannot load from " + 
    					client.getMsgFileName() + 
    					" as it is a directory not a text file.");
    		} 
    		
    		// try to load file
    		BufferedReader br = new BufferedReader(new FileReader(f));
    		StringBuffer payload = new StringBuffer();
    		String tmp = null;
    		while ((tmp = br.readLine()) != null) {
    			payload.append(tmp);
    		}
    		jmsTextMessage = getSession().createTextMessage(payload.toString());
    		return jmsTextMessage;
    		
    	} catch (FileNotFoundException ex) {
    		throw new JMSException(ex.getMessage());
    	} catch (IOException iox) {
    		throw new JMSException(iox.getMessage());
    	}
    }
}
