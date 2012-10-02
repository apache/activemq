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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;


/**
 * A simple example of log4j jms appender in conjuction with ActiveMQ
 */
public class Log4jJMSAppenderExample implements MessageListener {

	public Log4jJMSAppenderExample() throws Exception {
		// create a logTopic topic consumer
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		Connection conn = factory.createConnection();
		Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		conn.start();
		MessageConsumer consumer = sess.createConsumer(sess.createTopic("logTopic"));
		consumer.setMessageListener(this);
		// log a message
		Logger log = Logger.getLogger(Log4jJMSAppenderExample.class);
		log.info("Test log");
		// clean up
		Thread.sleep(1000);
		consumer.close();
		sess.close();
		conn.close();
		System.exit(1);
	}
	
	public static void main(String[] args) throws Exception {
		new Log4jJMSAppenderExample();
	}

	public void onMessage(Message message) {
		try {
			// receive log event in your consumer
			LoggingEvent event = (LoggingEvent)((ActiveMQObjectMessage)message).getObject();
			System.out.println("Received log [" + event.getLevel() + "]: "+ event.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
}