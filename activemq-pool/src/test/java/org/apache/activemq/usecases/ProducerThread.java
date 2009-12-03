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
package org.apache.activemq.usecases;

import java.util.Random;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ProducerThread extends Thread {	
	private JmsTemplate jmsTemplate;
	private int numberOfTopics;
	private int numberOfMessagesToSend;	
	private int messagesSent;
	private Random generator;
	private String queuePrefix;
	private ConnectionFactory connectionFactory;
	private String message;	
	private MessageCreator messageCreator;
	private int sendDelay;
	private Log log = LogFactory.getLog(ProducerThread.class);
	
	@Override
	public void run() {
		initialize();
		Random generator = new Random(Thread.currentThread().getName().hashCode());
		
		while (messagesSent < numberOfMessagesToSend) {
			int queueSuffix = generator.nextInt(numberOfTopics);			
			jmsTemplate.send(queuePrefix + queueSuffix, messageCreator);			
			messagesSent++;
			log.debug(Thread.currentThread().getName() + 
					": sent msg #" + messagesSent);
			try {
				Thread.sleep(sendDelay);
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
		}
		
		log.info("ProducerThread shutting down.");
	}
	
	private void initialize() {
		jmsTemplate = new JmsTemplate();
		jmsTemplate.setPubSubDomain(false);
		jmsTemplate.setConnectionFactory(connectionFactory);
		
		messageCreator = new MessageCreator() {
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(message);
			}			
		};
	}

	/**
	 * @param numberOfTopics the numberOfTopics to set
	 */
	protected void setNumberOfQueues(int numberOfTopics) {
		this.numberOfTopics = numberOfTopics;
	}
	/**
	 * @param queuePrefix the queuePrefix to set
	 */
	protected void setQueuePrefix(String queuePrefix) {
		this.queuePrefix = queuePrefix;
	}
	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	protected void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}
	/**
	 * @param message the message to set
	 */
	protected void setMessage(String message) {
		this.message = message;
	}

	/**
	 * @param numberOfMessagesToSend the numberOfMessagesToSend to set
	 */
	protected void setNumberOfMessagesToSend(int numberOfMessagesToSend) {
		this.numberOfMessagesToSend = numberOfMessagesToSend;
	}
	
	public void setSendDelay(int sendDelay) {
		this.sendDelay = sendDelay;
	}
	
	public int getMessagesSent() {
		return messagesSent;
	}
}
