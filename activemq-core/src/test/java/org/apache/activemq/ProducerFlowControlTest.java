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
package org.apache.activemq;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.tcp.TcpTransport;

public class ProducerFlowControlTest extends JmsTestSupport {

    ActiveMQQueue queueA = new ActiveMQQueue("QUEUE.A");
    ActiveMQQueue queueB = new ActiveMQQueue("QUEUE.B");
    protected TransportConnector connector;
    protected ActiveMQConnection connection;
    // used to test sendFailIfNoSpace on SystemUsage 
    protected final AtomicBoolean gotResourceException = new AtomicBoolean(false);

    public void test2ndPubisherWithProducerWindowSendConnectionThatIsBlocked() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        factory.setProducerWindowSize(1024 * 64);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queueB);

        // Test sending to Queue A
        // 1 few sends should not block until the producer window is used up.
        fillQueue(queueA);

        // Test sending to Queue B it should not block since the connection
        // should not be blocked.
        CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
        assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

        TextMessage msg = (TextMessage)consumer.receive();
        assertEquals("Message 1", msg.getText());
        msg.acknowledge();

        pubishDoneToQeueuB = asyncSendTo(queueB, "Message 2");
        assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

        msg = (TextMessage)consumer.receive();
        assertEquals("Message 2", msg.getText());
        msg.acknowledge();
    }

    public void testPubisherRecoverAfterBlock() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        factory.setProducerWindowSize(1024 * 64);
        factory.setUseAsyncSend(true);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(queueA);
        
        final AtomicBoolean done = new AtomicBoolean(true);
        final AtomicBoolean keepGoing = new AtomicBoolean(true);
        
   
		Thread thread = new Thread("Filler") {
			@Override
			public void run() {
                while (keepGoing.get()) {
                    done.set(false);
                    try {
						producer.send(session.createTextMessage("Test message"));
					} catch (JMSException e) {
					}
                }
			}
		};
		thread.start();
        waitForBlockedOrResourceLimit(done);

        // after receiveing messges, producer should continue sending messages 
        // (done == false)
        MessageConsumer consumer = session.createConsumer(queueA);
        TextMessage msg;
        for (int idx = 0; idx < 5; ++idx) {
        	msg = (TextMessage) consumer.receive(1000);
        	msg.acknowledge();
        }
        Thread.sleep(1000);
        keepGoing.set(false);
    	
		assertFalse(done.get());
    }
    
    public void test2ndPubisherWithSyncSendConnectionThatIsBlocked() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        factory.setAlwaysSyncSend(true);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queueB);

        // Test sending to Queue A
        // 1st send should not block. But the rest will.
        fillQueue(queueA);

        // Test sending to Queue B it should not block.
        CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
        assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

        TextMessage msg = (TextMessage)consumer.receive();
        assertEquals("Message 1", msg.getText());
        msg.acknowledge();

        pubishDoneToQeueuB = asyncSendTo(queueB, "Message 2");
        assertTrue(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));

        msg = (TextMessage)consumer.receive();
        assertEquals("Message 2", msg.getText());
        msg.acknowledge();
    }

    public void testSimpleSendReceive() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        factory.setAlwaysSyncSend(true);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queueA);

        // Test sending to Queue B it should not block.
        CountDownLatch pubishDoneToQeueuA = asyncSendTo(queueA, "Message 1");
        assertTrue(pubishDoneToQeueuA.await(2, TimeUnit.SECONDS));

        TextMessage msg = (TextMessage)consumer.receive();
        assertEquals("Message 1", msg.getText());
        msg.acknowledge();

        pubishDoneToQeueuA = asyncSendTo(queueA, "Message 2");
        assertTrue(pubishDoneToQeueuA.await(2, TimeUnit.SECONDS));

        msg = (TextMessage)consumer.receive();
        assertEquals("Message 2", msg.getText());
        msg.acknowledge();
    }

    public void test2ndPubisherWithStandardConnectionThatIsBlocked() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        // Test sending to Queue A
        // 1st send should not block.
        fillQueue(queueA);

        // Test sending to Queue B it should block.
        // Since even though the it's queue limits have not been reached, the
        // connection
        // is blocked.
        CountDownLatch pubishDoneToQeueuB = asyncSendTo(queueB, "Message 1");
        assertFalse(pubishDoneToQeueuB.await(2, TimeUnit.SECONDS));
    }

    private void fillQueue(final ActiveMQQueue queue) throws JMSException, InterruptedException {
        final AtomicBoolean done = new AtomicBoolean(true);
        final AtomicBoolean keepGoing = new AtomicBoolean(true);

        // Starts an async thread that every time it publishes it sets the done
        // flag to false.
        // Once the send starts to block it will not reset the done flag
        // anymore.
        new Thread("Fill thread.") {
            public void run() {
                Session session = null;
                try {
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(queue);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    while (keepGoing.get()) {
                        done.set(false);
                        producer.send(session.createTextMessage("Hello World"));
                    }
                } catch (JMSException e) {
                } finally {
                    safeClose(session);
                }
            }
        }.start();

        waitForBlockedOrResourceLimit(done);
        keepGoing.set(false);
    }

    protected void waitForBlockedOrResourceLimit(final AtomicBoolean done)
            throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            // the producer is blocked once the done flag stays true or there is a resource exception
            if (done.get() || gotResourceException.get()) {
                break;
            }
            done.set(true);
        }
    }

    private CountDownLatch asyncSendTo(final ActiveMQQueue queue, final String message) throws JMSException {
        final CountDownLatch done = new CountDownLatch(1);
        new Thread("Send thread.") {
            public void run() {
                Session session = null;
                try {
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(queue);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    producer.send(session.createTextMessage(message));
                    done.countDown();
                } catch (JMSException e) {
                } finally {
                    safeClose(session);
                }
            }
        }.start();
        return done;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);

        // Setup a destination policy where it takes only 1 message at a time.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(1);
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policy.setProducerFlowControl(true);
        policyMap.setDefaultEntry(policy);
        service.setDestinationPolicy(policyMap);

        connector = service.addConnector("tcp://localhost:0");
        return service;
    }

    public void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        if (connection != null) {
            TcpTransport t = (TcpTransport)connection.getTransport().narrow(TcpTransport.class);
            t.getTransportListener().onException(new IOException("Disposed."));
            connection.getTransport().stop();
            super.tearDown();
        }
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connector.getConnectUri());
    }
}
