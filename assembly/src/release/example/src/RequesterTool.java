/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.Arrays;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.IndentPrinter;

/**
 * A simple tool for publishing messages
 * 
 * @version $Revision: 1.2 $
 */
public class RequesterTool {

	private int messageCount = 10;
	private long sleepTime = 0L;
	private boolean verbose = true;
	private int messageSize = 255;
	private long timeToLive;
	private String subject = "TOOL.DEFAULT";
	private String replySubject;
	private boolean topic = false;
	private String user = ActiveMQConnection.DEFAULT_USER;
	private String password = ActiveMQConnection.DEFAULT_PASSWORD;
	private String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	private boolean transacted = false;
	private boolean persistent = false;
	private String clientId;

	private Destination destination;
	private Destination replyDest;
	private MessageProducer producer;
	private MessageConsumer consumer;
	private Session session;

	public static void main(String[] args) {
		RequesterTool requesterTool = new RequesterTool();
		String[] unknown = CommandLineSupport.setOptions(requesterTool, args);
		if (unknown.length > 0) {
			System.out.println("Unknown options: " + Utilities.arrayToString(unknown));
			System.exit(-1);
		}
		requesterTool.run();
	}

	public void run() {
		
		Connection connection=null;
		try {
			
			System.out.println("Connecting to URL: " + url);
			System.out.println("Publishing a Message with size " + messageSize + " to " + (topic ? "topic" : "queue") + ": " + subject);
			System.out.println("Using " + (persistent ? "persistent" : "non-persistent") + " messages");
			System.out.println("Sleeping between publish " + sleepTime + " ms");
			
			// Create the connection
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);
			connection = connectionFactory.createConnection();
			if (persistent && clientId != null && clientId.length() > 0 && !"null".equals(clientId)) {
				connection.setClientID(clientId);
			}
			connection.start();

			// Create the Session
			session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
			
			// And the Destinations..
			if (topic) {
				destination = session.createTopic(subject);
				if( replySubject==null || replySubject.equals("") )
					replyDest = session.createTemporaryTopic();
				else
					replyDest = session.createTopic(replySubject);
			} else {
				destination = session.createQueue(subject);
				if( replySubject==null || replySubject.equals("") )
					replyDest = session.createTemporaryQueue();
				else
					replyDest = session.createQueue(replySubject);
			}
			System.out.println("Reply Destination: " + replyDest);

			// Create the producer
			producer = session.createProducer(destination);
			if (persistent) {
				producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			} else {
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			}
			if (timeToLive != 0) {
				System.out.println("Messages time to live " + timeToLive + " ms");
				producer.setTimeToLive(timeToLive);
			}

			// Create the reply consumer
			consumer = session.createConsumer(replyDest);
			
			// Start sending reqests.
			requestLoop();
			
			System.out.println("Done.");
			
			// Use the ActiveMQConnection interface to dump the connection stats.
			ActiveMQConnection c = (ActiveMQConnection) connection;
			c.getConnectionStats().dump(new IndentPrinter());
						
		} catch (Exception e) {
			System.out.println("Caught: " + e);
			e.printStackTrace();
		} finally {
			try { 
				connection.close();
			} catch (Throwable ignore) {
			}
		}
	}

	protected void requestLoop() throws Exception {

		for (int i = 0; i < messageCount || messageCount == 0; i++) {

			TextMessage message = session.createTextMessage(createMessageText(i));
			message.setJMSReplyTo(replyDest);

			if (verbose) {
				String msg = message.getText();
				if (msg.length() > 50) {
					msg = msg.substring(0, 50) + "...";
				}
				System.out.println("Sending message: " + msg);
			}

			producer.send(message);
			if (transacted) {
				session.commit();
			}

			System.out.println("Waiting for reponse message...");
			Message message2 = consumer.receive();
			if (message2 instanceof TextMessage) {
				System.out.println("Reponse message: " + ((TextMessage) message2).getText());
			} else {
				System.out.println("Reponse message: " + message2);
			}
			if (transacted) {
				session.commit();
			}

			Thread.sleep(sleepTime);

		}
	}

	/**
	 * @param i
	 * @return
	 */
	private String createMessageText(int index) {
		StringBuffer buffer = new StringBuffer(messageSize);
		buffer.append("Message: " + index + " sent at: " + new Date());
		if (buffer.length() > messageSize) {
			return buffer.substring(0, messageSize);
		}
		for (int i = buffer.length(); i < messageSize; i++) {
			buffer.append(' ');
		}
		return buffer.toString();
	}


	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public void setPersistent(boolean durable) {
		this.persistent = durable;
	}
	public void setMessageCount(int messageCount) {
		this.messageCount = messageCount;
	}
	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public void setSleepTime(long sleepTime) {
		this.sleepTime = sleepTime;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}
	public void setTopic(boolean topic) {
		this.topic = topic;
	}
	public void setQueue(boolean queue) {
		this.topic = !queue;
	}	
	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	public void setReplySubject(String replySubject) {
		this.replySubject = replySubject;
	}
}
