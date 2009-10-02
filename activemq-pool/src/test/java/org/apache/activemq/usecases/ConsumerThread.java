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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class ConsumerThread extends Thread {	
	private DefaultMessageListenerContainer container;
	private MessageDrivenPojo messageListener;	
	private boolean run;
	private String destination;
	private ConnectionFactory connectionFactory;
	private boolean durable;			
	private int concurrentConsumers;
	private boolean sessionTransacted;
	private boolean pubSubDomain;
	private boolean running;
	private Log log = LogFactory.getLog(ConsumerThread.class);
	private int numberOfQueues;
	private String consumerName;
	
	@Override
	public void run() {		
		run = true;
		createContainer();		
		container.initialize();				
		container.start();
		
		running = true;
		
		while (run) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();			
			}			
		}
		
		container.stop();
		container.destroy();
		
		if (connectionFactory instanceof SingleConnectionFactory) {
			((SingleConnectionFactory)connectionFactory).destroy();
		}
		
		log.info("ConsumerThread1 closing down");
	}

	private DefaultMessageListenerContainer createContainer() {
		Random generator = new Random(consumerName.hashCode());
		int queueSuffix = generator.nextInt(numberOfQueues);
		
		
		container = new DefaultMessageListenerContainer();				
		container.setPubSubDomain(pubSubDomain);
		container.setDestinationName(destination + queueSuffix);				
		container.setMessageListener(messageListener);
		container.setConnectionFactory(connectionFactory);	
		container.setConcurrentConsumers(concurrentConsumers);
		container.setSessionTransacted(sessionTransacted);

		//container.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER);
		//container.setMaxConcurrentConsumers(concurrentConsumers);
		//container.setAcceptMessagesWhileStopping(false);				
		//container.setAutoStartup(false);
		//without setting a tx manager, this will use local JMS tx.

		/*
		if (durable) {
			container.setSubscriptionDurable(true);
			container.setDurableSubscriptionName("ConsumerThread1" + Thread.currentThread().getId());
		}
		*/
		container.afterPropertiesSet();
		log.info("subscribing to " + destination + queueSuffix);
		return container;
	}
		
	/**
	 * @param messageListener the messageListener to set
	 */
	public void setMessageDrivenPojo(MessageDrivenPojo messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * @param run the run to set
	 */
	public void setRun(boolean run) {
		this.run = run;
	}

	/**
	 * @param destination the destination to set
	 */
	public void setDestination(String destination) {
		this.destination = destination;
	}
	
	public void setNumberOfQueues(int no) {
		this.numberOfQueues = no;
	}
	
	public int getNumberOfQueues() {
		return this.numberOfQueues;
	}
	

	public void setConsumerName(String name) {
		this.consumerName = name;
	}

	/**
	 * @param connectionFactory the connectionFactory to set
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param durable the durable to set
	 */
	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	/**
	 * @param concurrentConsumers the concurrentConsumers to set
	 */
	public void setConcurrentConsumers(int concurrentConsumers) {
		this.concurrentConsumers = concurrentConsumers;
	}

	/**
	 * @param sessionTransacted the sessionTransacted to set
	 */
	public void setSessionTransacted(boolean sessionTransacted) {
		this.sessionTransacted = sessionTransacted;
	}

	/**
	 * @param pubSubDomain the pubSubDomain to set
	 */
	public void setPubSubDomain(boolean pubSubDomain) {
		this.pubSubDomain = pubSubDomain;
	}
	
	/**
	 * @return the messageListener
	 */
	public MessageDrivenPojo getMessageDrivenPojo() {
		return messageListener;
	}	
	
	public boolean isRunning() {
		return running;
	}
}
