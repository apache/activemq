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
package org.apache.activemq.transport.failover;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class FailoverUpdateURIsTest extends TestCase {
	
	private static final String QUEUE_NAME = "test.failoverupdateuris";

	public void testUpdateURIs() throws Exception {
		
		long timeout = 1000;
		URI firstTcpUri = new URI("tcp://localhost:61616");
		URI secondTcpUri = new URI("tcp://localhost:61626");
                String targetDir = "target/" + getName();
                new File(targetDir).mkdir();
                File updateFile = new File(targetDir + "/updateURIsFile.txt");
                System.out.println(updateFile);
                System.out.println(updateFile.toURI());
                System.out.println(updateFile.getAbsoluteFile());
                System.out.println(updateFile.getAbsoluteFile().toURI());
                FileOutputStream out = new FileOutputStream(updateFile);
                out.write(firstTcpUri.toString().getBytes());
                out.close();
                              
		BrokerService bs1 = new BrokerService();
		bs1.setUseJmx(false);
		bs1.addConnector(firstTcpUri);
		bs1.start();

                // no failover uri's to start with, must be read from file...
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:()?updateURIsURL=file:///" + updateFile.getAbsoluteFile());
		Connection connection = cf.createConnection();
                connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue theQueue = session.createQueue(QUEUE_NAME);
		MessageProducer producer = session.createProducer(theQueue);
		MessageConsumer consumer = session.createConsumer(theQueue);
		Message message = session.createTextMessage("Test message");
		producer.send(message);
                Message msg = consumer.receive(2000);
                assertNotNull(msg);
		
		bs1.stop();
                bs1.waitUntilStopped();

		BrokerService bs2 = new BrokerService();
		bs2.setUseJmx(false);
		bs2.addConnector(secondTcpUri);
		bs2.start();
		
		// add the transport uri for broker number 2
                out = new FileOutputStream(updateFile, true);
                out.write(",".getBytes());
                out.write(secondTcpUri.toString().getBytes());
                out.close();

                producer.send(message);
                msg = consumer.receive(2000);
                assertNotNull(msg);
	}
	
}
