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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ObjectMessageNotSerializableTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ObjectMessageNotSerializableTest.class);
    
    BrokerService broker;
    Connection connection;
    ActiveMQSession session;
    MessageProducer producer;
    MessageConsumer consumer;
    public ActiveMQDestination destination = new ActiveMQQueue("test");

    int numReceived = 0;
    boolean writeObjectCalled, readObjectCalled, readObjectNoDataCalled;

    public static Test suite() {
        return suite(ObjectMessageNotSerializableTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
	protected void setUp() throws Exception {
        broker = createBroker();
    }
	
	public void testSendNotSerializeableObjectMessage() throws Exception {
		
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setOptimizedMessageDispatch(true);
        factory.setObjectMessageSerializationDefered(true);
        factory.setCopyMessageOnSend(false);


		connection = factory.createConnection();
		session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		producer = session.createProducer(destination);

		consumer = session.createConsumer(destination);
		connection.start();
		
        final MyObject obj = new MyObject("A message");

		Thread consumerThread = new Thread("Consumer Thread") {
			public void run() {
				try {
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)consumer.receive();
                    if ( message != null ) {
                        numReceived++;
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                    }
					consumer.close();
				} catch (Throwable ex) {
					ex.printStackTrace();
				}
			}
		};
		
        consumerThread.start();
		
		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)session.createObjectMessage();
                    message.setObject(obj);
                    producer.send(message);
                	producer.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
		};
		
		producingThread.start();
		
        consumerThread.join();
        producingThread.join();
        session.close();

        assertFalse("writeObject called", obj.getWriteObjectCalled());
        assertFalse("readObject called", obj.getReadObjectCalled());
        assertFalse("readObjectNoData called", obj.getReadObjectNoDataCalled());

	}

	private BrokerService createBroker() throws Exception {
	    BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        
        broker.start();
        broker.waitUntilStarted();
        return broker;
	}

	protected void tearDown() throws Exception {
		connection.stop();
		broker.stop();
		broker.waitUntilStopped();
	}
}
