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

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ObjectMessageNotSerializableTest extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectMessageNotSerializableTest.class);
    
    BrokerService broker;
    AtomicInteger numReceived = new AtomicInteger(0);
    final Vector<Throwable> exceptions = new Vector<Throwable>();

    public static Test suite() {
        return suite(ObjectMessageNotSerializableTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
	protected void setUp() throws Exception {
        exceptions.clear();
        broker = createBroker();
    }
	
	public void testSendNotSerializeableObjectMessage() throws Exception {

        final  ActiveMQDestination destination = new ActiveMQQueue("testQ");
        final MyObject obj = new MyObject("A message");

        final CountDownLatch consumerStarted = new CountDownLatch(1);

		Thread vmConsumerThread = new Thread("Consumer Thread") {
			public void run() {
				try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                    factory.setOptimizedMessageDispatch(true);
                    factory.setObjectMessageSerializationDefered(true);
                    factory.setCopyMessageOnSend(false);

                    Connection connection = factory.createConnection();
		            Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		            MessageConsumer consumer = session.createConsumer(destination);
		            connection.start();
                    consumerStarted.countDown();
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
					consumer.close();
				} catch (Throwable ex) {
					exceptions.add(ex);
				}
			}
		};
        vmConsumerThread.start();

		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                    factory.setOptimizedMessageDispatch(true);
                    factory.setObjectMessageSerializationDefered(true);
                    factory.setCopyMessageOnSend(false);

                    Connection connection = factory.createConnection();
		            Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		            MessageProducer producer = session.createProducer(destination);
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)session.createObjectMessage();
                    message.setObject(obj);
                    producer.send(message);
                	producer.close();
                } catch (Throwable ex) {
                    exceptions.add(ex);
                }
            }
		};

        assertTrue("consumers started", consumerStarted.await(10, TimeUnit.SECONDS));
		producingThread.start();

        vmConsumerThread.join();
        producingThread.join();

        assertEquals("writeObject called", 0, obj.getWriteObjectCalled());
        assertEquals("readObject called", 0, obj.getReadObjectCalled());
        assertEquals("readObjectNoData called", 0, obj.getReadObjectNoDataCalled());

        assertEquals("Got expected messages", 1, numReceived.get());
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
	}

    public void testSendNotSerializeableObjectMessageOverTcp() throws Exception {
        final  ActiveMQDestination destination = new ActiveMQTopic("testTopic");
        final MyObject obj = new MyObject("A message");

        final CountDownLatch consumerStarted = new CountDownLatch(3);
        final Vector<Throwable> exceptions = new Vector<Throwable>();
		Thread vmConsumerThread = new Thread("Consumer Thread") {
			public void run() {
				try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                    factory.setOptimizedMessageDispatch(true);
                    factory.setObjectMessageSerializationDefered(true);
                    factory.setCopyMessageOnSend(false);

                    Connection connection = factory.createConnection();
		            Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		            MessageConsumer consumer = session.createConsumer(destination);
		            connection.start();
                    consumerStarted.countDown();
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)consumer.receive(30000);
                    if ( message != null ) {                  
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
					consumer.close();
				} catch (Throwable ex) {
					exceptions.add(ex);
				}
			}
		};
        vmConsumerThread.start();

        Thread tcpConsumerThread = new Thread("Consumer Thread") {
			public void run() {
				try {

                    ActiveMQConnectionFactory factory =
                            new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
                    factory.setOptimizedMessageDispatch(true);

                    Connection connection = factory.createConnection();
		            Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		            MessageConsumer consumer = session.createConsumer(destination);
		            connection.start();
                    consumerStarted.countDown();
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                        assertEquals("readObject called", 1, object.getReadObjectCalled());
                    }
					consumer.close();
				} catch (Throwable ex) {
					exceptions.add(ex);
				}
			}
		};
        tcpConsumerThread.start();


        Thread notherVmConsumerThread = new Thread("Consumer Thread") {
            public void run() {
                try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                    factory.setOptimizedMessageDispatch(true);
                    factory.setObjectMessageSerializationDefered(true);
                    factory.setCopyMessageOnSend(false);

                    Connection connection = factory.createConnection();
                    Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    connection.start();
                    consumerStarted.countDown();
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)consumer.receive(30000);
                    if ( message != null ) {
                        MyObject object = (MyObject)message.getObject();
                        LOG.info("Got message " + object.getMessage());
                        numReceived.incrementAndGet();
                    }
                    consumer.close();
                } catch (Throwable ex) {
                    exceptions.add(ex);
                }
            }
        };
        notherVmConsumerThread.start();

		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                    factory.setOptimizedMessageDispatch(true);
                    factory.setObjectMessageSerializationDefered(true);
                    factory.setCopyMessageOnSend(false);

                    Connection connection = factory.createConnection();
		            Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		            MessageProducer producer = session.createProducer(destination);
                    ActiveMQObjectMessage message = (ActiveMQObjectMessage)session.createObjectMessage();
                    message.setObject(obj);
                    producer.send(message);
                	producer.close();
                } catch (Throwable ex) {
                    exceptions.add(ex);
                }
            }
		};

        assertTrue("consumers started", consumerStarted.await(10, TimeUnit.SECONDS));
		producingThread.start();
		
        vmConsumerThread.join();
        tcpConsumerThread.join();
        notherVmConsumerThread.join();
        producingThread.join();

        assertEquals("writeObject called", 1, obj.getWriteObjectCalled());
        assertEquals("readObject called", 0, obj.getReadObjectCalled());
        assertEquals("readObjectNoData called", 0, obj.getReadObjectNoDataCalled());

        assertEquals("Got expected messages", 3, numReceived.get());
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
	}

	private BrokerService createBroker() throws Exception {
	    BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:0");
        
        broker.start();
        broker.waitUntilStarted();
        return broker;
	}

	protected void tearDown() throws Exception {
		broker.stop();
		broker.waitUntilStopped();
	}
}
