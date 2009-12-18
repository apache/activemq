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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
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
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

// see https://issues.apache.org/activemq/browse/AMQ-2473
public class FailoverTransactionTest {
	
    private static final Log LOG = LogFactory.getLog(FailoverTransactionTest.class);
	private static final String QUEUE_NAME = "test.FailoverTransactionTest";
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

	//@Test
	public void testFailoverProducerCloseBeforeTransaction() throws Exception {
	    startCleanBroker();
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
		MessageProducer producer = session.createProducer(destination);
		
		TextMessage message = session.createTextMessage("Test message");
		producer.send(message);

		// close producer before commit, emulate jmstemplate
		producer.close();
		
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
        doTestFailoverCommitReplyLost(false);
    }  
    
    @Test
    public void testFailoverCommitReplyLostJdbc() throws Exception {
        doTestFailoverCommitReplyLost(true);
    }
    
    public void doTestFailoverCommitReplyLost(boolean useJdbcPersistenceAdapter) throws Exception {
        
        broker = createBroker(true);
        if (useJdbcPersistenceAdapter) {
            broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        }
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
        MessageProducer producer = session.createProducer(destination);
        
        TextMessage message = session.createTextMessage("Test message");
        producer.send(message);
        
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
        if (useJdbcPersistenceAdapter) {
            broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        }
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
        if (useJdbcPersistenceAdapter) {
            broker.setPersistenceAdapter(new JDBCPersistenceAdapter());
        }
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

	@Test
	public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
	    startCleanBroker();        
	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?trackTransactionProducers=false");
	    Connection connection = cf.createConnection();
	    connection.start();
	    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = session.createQueue(QUEUE_NAME);
	    
	    MessageConsumer consumer = session.createConsumer(destination);
	    MessageProducer producer = session.createProducer(destination);
	    
	    TextMessage message = session.createTextMessage("Test message");
	    producer.send(message);
	    
	    // close producer before commit, emulate jmstemplate
	    producer.close();
	    
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
}
