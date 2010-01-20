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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

// see https://issues.apache.org/activemq/browse/AMQ-2473
public class FailoverTransactionTest {
	
    private static final Log LOG = LogFactory.getLog(FailoverTransactionTest.class);
	private static final String QUEUE_NAME = "FailoverWithTx";
	private String url = "tcp://localhost:61616";
	BrokerService broker;
	
	public void startCleanBroker() throws Exception {
	    startBroker(true);
	}
	
	@After
	public void stopBroker() throws Exception {
	    if (broker != null) {
	        broker.stop();
	    }
	}
	
	public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
	    broker = createBroker(deleteAllMessagesOnStartup);
        broker.start();
	}

	public BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {   
	    broker = new BrokerService();
	    broker.setUseJmx(false);
	    broker.addConnector(url);
	    broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);
	    return broker;
	}

	@Test
	public void testFailoverProducerCloseBeforeTransaction() throws Exception {
	    startCleanBroker();
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
		produceMessage(session, destination);
		
		// restart to force failover and connection state recovery before the commit
		broker.stop();
		startBroker(false);

		session.commit();
		assertNotNull("we got the message", consumer.receive(20000));
		session.commit();	
		connection.close();
	}
	
    @Test
    public void testFailoverCommitReplyLost() throws Exception {
        doTestFailoverCommitReplyLost(0);
    }  
    
    @Test
    public void testFailoverCommitReplyLostJdbc() throws Exception {
        doTestFailoverCommitReplyLost(1);
    }
    
    @Test
    public void testFailoverCommitReplyLostKahaDB() throws Exception {
        doTestFailoverCommitReplyLost(2);
    }
    
    public void doTestFailoverCommitReplyLost(final int adapter) throws Exception {
        
        broker = createBroker(true);
        setPersistenceAdapter(adapter);
            
        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(ConnectionContext context,
                            TransactionId xid, boolean onePhase) throws Exception {
                        super.commitTransaction(context, xid, onePhase);
                        // so commit will hang as if reply is lost
                        context.setDontSendReponse(true);
                        Executors.newSingleThreadExecutor().execute(new Runnable() {   
                            public void run() {
                                LOG.info("Stopping broker post commit...");
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                   }   
                }
        });
        broker.start();
        
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
        produceMessage(session, destination);
        
        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        // broker will die on commit reply so this will hang till restart
        Executors.newSingleThreadExecutor().execute(new Runnable() {   
            public void run() {
                LOG.info("doing async commit...");
                try {
                    session.commit();
                    commitDoneLatch.countDown();
                    LOG.info("done async commit");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
       
        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false);
        setPersistenceAdapter(adapter);
        broker.start();

        assertTrue("tx committed trough failover", commitDoneLatch.await(30, TimeUnit.SECONDS));
        
        // new transaction
        Message msg = consumer.receive(20000);
        LOG.info("Received: " + msg);
        assertNotNull("we got the message", msg);
        assertNull("we got just one message", consumer.receive(2000));
        session.commit();
        consumer.close();
        connection.close();
        
        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();
        
        LOG.info("Checking for remaining/hung messages..");
        broker = createBroker(false);
        setPersistenceAdapter(adapter);
        broker.start();
        
        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session2.createConsumer(destination);
        msg = consumer.receive(1000);
        if (msg == null) {
            msg = consumer.receive(5000);
        }
        LOG.info("Received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    private void setPersistenceAdapter(int adapter) throws IOException {
        switch (adapter) {
        case 0:
            break;
        case 1:
            broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
            break;
        case 2:
            KahaDBPersistenceAdapter store = new KahaDBPersistenceAdapter();
            store.setDirectory(new File("target/activemq-data/kahadb/FailoverTransactionTest"));
            broker.setPersistenceAdapter(store);
            break;
        }
    }

	@Test
	public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
	    startCleanBroker();        
	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?trackTransactionProducers=false");
	    Connection connection = cf.createConnection();
	    connection.start();
	    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = session.createQueue(QUEUE_NAME);
	    
	    MessageConsumer consumer = session.createConsumer(destination);
	    produceMessage(session, destination);
	    
	    // restart to force failover and connection state recovery before the commit
	    broker.stop();
	    startBroker(false);
	    
	    session.commit();
	    
	    // without tracking producers, message will not be replayed on recovery
	    assertNull("we got the message", consumer.receive(5000));
	    session.commit();   
	    connection.close();
	}
	
	@Test
	public void testFailoverMultipleProducerCloseBeforeTransaction() throws Exception {
	    startCleanBroker();	        
	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
	    Connection connection = cf.createConnection();
	    connection.start();
	    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = session.createQueue(QUEUE_NAME);
	    
	    MessageConsumer consumer = session.createConsumer(destination);
	    MessageProducer producer;
	    TextMessage message;
	    final int count = 10;
	    for (int i=0; i<count; i++) {
	        producer = session.createProducer(destination);	        
	        message = session.createTextMessage("Test message: " + count);
	        producer.send(message);
	        producer.close();
	    }
	    
	    // restart to force failover and connection state recovery before the commit
	    broker.stop();
	    startBroker(false);
	    
	    session.commit();
	    for (int i=0; i<count; i++) {
	        assertNotNull("we got all the message: " + count, consumer.receive(20000));
	    }
	    session.commit();
	    connection.close();
	}  
	
	@Test
	public void testFailoverConsumerCommitLost() throws Exception {
	    final int adapter = 0;
	    broker = createBroker(true);
	    setPersistenceAdapter(adapter);

	    broker.setPlugins(new BrokerPlugin[] {
	            new BrokerPluginSupport() {

	                @Override
	                public void commitTransaction(ConnectionContext context,
	                        TransactionId xid, boolean onePhase) throws Exception {
	                    super.commitTransaction(context, xid, onePhase);
	                    // so commit will hang as if reply is lost
	                    context.setDontSendReponse(true);
	                    Executors.newSingleThreadExecutor().execute(new Runnable() {   
	                        public void run() {
	                            LOG.info("Stopping broker post commit...");
	                            try {
	                                broker.stop();
	                            } catch (Exception e) {
	                                e.printStackTrace();
	                            }
	                        }
	                    });
	                }   
	            }
	    });
	    broker.start();

	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
	    Connection connection = cf.createConnection();
	    connection.start();
	    final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	    final Session consumerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = producerSession.createQueue(QUEUE_NAME);

	    final MessageConsumer consumer = consumerSession.createConsumer(destination);

	    produceMessage(producerSession, destination);

	    final Vector<Message> receivedMessages = new Vector<Message>();
	    final CountDownLatch commitDoneLatch = new CountDownLatch(1);  
	    Executors.newSingleThreadExecutor().execute(new Runnable() {   
	        public void run() {
	            LOG.info("doing async commit after consume...");
	            try {
	                Message msg = consumer.receive(20000);
	                LOG.info("Got message: " + msg);
	                receivedMessages.add(msg);
	                consumerSession.commit();
	                commitDoneLatch.countDown();
	                LOG.info("done async commit");
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
	    });


	    // will be stopped by the plugin
	    broker.waitUntilStopped();
	    broker = createBroker(false);
	    setPersistenceAdapter(adapter);
	    broker.start();

	    assertTrue("tx committed trough failover", commitDoneLatch.await(30, TimeUnit.SECONDS));

	    assertEquals("we got a message", 1, receivedMessages.size());

	    // new transaction
	    Message msg = consumer.receive(20000);
	    LOG.info("Received: " + msg);
	    assertNull("we did not get a duplicate message", msg);
	    consumerSession.commit();
	    consumer.close();
	    connection.close();

	    // ensure no dangling messages with fresh broker etc
	    broker.stop();
	    broker.waitUntilStopped();

	    LOG.info("Checking for remaining/hung messages..");
	    broker = createBroker(false);
	    setPersistenceAdapter(adapter);
	    broker.start();

	    // after restart, ensure no dangling messages
	    cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
	    connection = cf.createConnection();
	    connection.start();
	    Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	    MessageConsumer consumer2 = session2.createConsumer(destination);
	    msg = consumer2.receive(1000);
	    if (msg == null) {
	        msg = consumer2.receive(5000);
	    }
	    LOG.info("Received: " + msg);
	    assertNull("no messges left dangling but got: " + msg, msg);
	    connection.close();
	}
	
    @Test
    public void testFailoverConsumerAckLost() throws Exception {
        // as failure depends on hash order, do a few times
        for (int i=0; i<3; i++) {
            try {
                doTestFailoverConsumerAckLost();
            } finally {
                stopBroker();
            }
        }
    }
    
    public void doTestFailoverConsumerAckLost() throws Exception {
        final int adapter = 0;
        broker = createBroker(true);
        setPersistenceAdapter(adapter);
            
        broker.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {

                    // broker is killed on delivered ack as prefetch is 1
                    @Override
                    public void acknowledge(
                            ConsumerBrokerExchange consumerExchange,
                            final MessageAck ack) throws Exception {
                        
                        consumerExchange.getConnectionContext().setDontSendReponse(true);
                        Executors.newSingleThreadExecutor().execute(new Runnable() {   
                            public void run() {
                                LOG.info("Stopping broker on ack: "  + ack);
                                try {
                                    broker.stop();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                }
        });
        broker.start();
        
        Vector<Connection> connections = new Vector<Connection>();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        Connection connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue destination = producerSession.createQueue(QUEUE_NAME + "?consumer.prefetchSize=1");
           
        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession1 = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        
        connection = cf.createConnection();
        connection.start();
        connections.add(connection);
        final Session consumerSession2 = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        
        final MessageConsumer consumer1 = consumerSession1.createConsumer(destination);
        final MessageConsumer consumer2 = consumerSession2.createConsumer(destination);
        
        produceMessage(producerSession, destination);
        produceMessage(producerSession, destination);
        
        final Vector<Message> receivedMessages = new Vector<Message>();
        final CountDownLatch commitDoneLatch = new CountDownLatch(1);
        
        Executors.newSingleThreadExecutor().execute(new Runnable() {   
            public void run() {
                LOG.info("doing async commit after consume...");
                try {
                    Message msg = consumer1.receive(20000);
                    LOG.info("consumer1 first attempt got message: " + msg);
                    receivedMessages.add(msg);
                    
                    TimeUnit.SECONDS.sleep(7);
                    
                    // should not get a second message as there are two messages and two consumers
                    // but with failover and unordered connection restore it can get the second
                    // message which could create a problem for a pending ack
                    msg = consumer1.receive(5000);
                    LOG.info("consumer1 second attempt got message: " + msg);
                    if (msg != null) {
                        receivedMessages.add(msg);
                    }
                    
                    LOG.info("committing consumer1 session: " + receivedMessages.size() + " messsage(s)");
                    consumerSession1.commit();
                    commitDoneLatch.countDown();
                    LOG.info("done async commit");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        
               
        // will be stopped by the plugin
        broker.waitUntilStopped();
        broker = createBroker(false);
        setPersistenceAdapter(adapter);
        broker.start();

        assertTrue("tx committed trough failover", commitDoneLatch.await(30, TimeUnit.SECONDS));
        
        // getting 2 is indicative of a problem - proven with dangling message found after restart
        LOG.info("received message count: " + receivedMessages.size());
        
        // new transaction
        Message msg = consumer1.receive(2000);
        LOG.info("post: from consumer1 received: " + msg);
        assertNull("should be nothing left for consumer1", msg);
        consumerSession1.commit();
        
        // consumer2 should get other message provided consumer1 did not get 2
        msg = consumer2.receive(5000);
        LOG.info("post: from consumer2 received: " + msg);
        if (receivedMessages.size() == 1) {
            assertNotNull("got second message on consumer2", msg);
        }
        consumerSession2.commit();
        
        for (Connection c: connections) {
            c.close();
        }
        
        // ensure no dangling messages with fresh broker etc
        broker.stop();
        broker.waitUntilStopped();
        
        LOG.info("Checking for remaining/hung messages..");
        broker = createBroker(false);
        setPersistenceAdapter(adapter);
        broker.start();
        
        // after restart, ensure no dangling messages
        cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
        connection = cf.createConnection();
        connection.start();
        Session sweeperSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer sweeper = sweeperSession.createConsumer(destination);
        msg = sweeper.receive(1000);
        if (msg == null) {
            msg = sweeper.receive(5000);
        }
        LOG.info("Sweep received: " + msg);
        assertNull("no messges left dangling but got: " + msg, msg);
        connection.close();
    }

    private void produceMessage(final Session producerSession, Queue destination)
            throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);      
        TextMessage message = producerSession.createTextMessage("Test message");
        producer.send(message);
        producer.close();
    }
	
}
