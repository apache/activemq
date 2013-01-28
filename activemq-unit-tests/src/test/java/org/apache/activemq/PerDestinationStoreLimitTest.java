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
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// see: https://issues.apache.org/activemq/browse/AMQ-2668
public class PerDestinationStoreLimitTest extends JmsTestSupport {
    static final Logger LOG = LoggerFactory.getLogger(PerDestinationStoreLimitTest.class);
    final String oneKb = new String(new byte[1024]);
    
    ActiveMQDestination queueDest = new ActiveMQQueue("PerDestinationStoreLimitTest.Queue");
    ActiveMQDestination topicDest = new ActiveMQTopic("PerDestinationStoreLimitTest.Topic");

    protected TransportConnector connector;
    protected ActiveMQConnection connection;
    
    public void testDLQAfterBlockTopic() throws Exception {
        doTestDLQAfterBlock(topicDest);
    }
    
    public void testDLQAfterBlockQueue() throws Exception {
        doTestDLQAfterBlock(queueDest);
    }
    
    public void doTestDLQAfterBlock(ActiveMQDestination destination) throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        // Immediately sent to the DLQ on rollback, no redelivery
        redeliveryPolicy.setMaximumRedeliveries(0);
        factory.setRedeliveryPolicy(redeliveryPolicy);
        
        // Separate connection for consumer so it will not be blocked by filler thread
        // sending when it blocks
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.setClientID("someId");
        connection.start();

        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);      
        MessageConsumer consumer = destination.isQueue() ?
                    consumerSession.createConsumer(destination) :
                    consumerSession.createDurableSubscriber((Topic) destination, "Durable");
        
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        final MessageProducer producer = session.createProducer(destination);
        
        final AtomicBoolean done = new AtomicBoolean(true);
        final AtomicBoolean keepGoing = new AtomicBoolean(true);
        final CountDownLatch fillerStarted = new CountDownLatch(1);

        final AtomicLong sent = new AtomicLong(0);
        Thread thread = new Thread("Filler") {
            int i;
            @Override
            public void run() {
                while (keepGoing.get()) {
                    done.set(false);
                    fillerStarted.countDown();
                    try {
                        producer.send(session.createTextMessage(oneKb + ++i));
                        if (i%10 == 0) {
                            session.commit();
                            sent.getAndAdd(10);
                            LOG.info("committed/sent: " + sent.get());
                        }
                        LOG.info("sent: " + i);
                    } catch (JMSException e) {
                    }
                }
            }
        };
        thread.start();
		
        assertTrue("filler started..", fillerStarted.await(20, TimeUnit.SECONDS));
        waitForBlocked(done);

        // consume and rollback some so message gets to DLQ
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();
        TextMessage msg;
        int received = 0;
        for (;received < sent.get(); ++received) {
        	msg = (TextMessage) consumer.receive(4000);
        	if (msg == null) {
        	    LOG.info("received null on count: " + received);
        	    break;
        	}
        	LOG.info("received: " + received + ", msg: " + msg.getJMSMessageID());
        	if (received%5==0) {
        	    if (received%3==0) {
        	        // force the use of the DLQ which will use some more store
        	        LOG.info("rollback on : " + received);
        	        consumerSession.rollback();
        	    } else {
        	        LOG.info("commit on : " + received);
        	        consumerSession.commit();
        	    }
        	}
        }
        LOG.info("Done:: sent: " + sent.get() + ", received: " + received);
        keepGoing.set(false);
        assertTrue("some were sent:", sent.get() > 0);
        assertEquals("received what was committed", sent.get(), received);	
    }

    protected void waitForBlocked(final AtomicBoolean done)
            throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            // the producer is blocked once the done flag stays true
            if (done.get()) {
                LOG.info("Blocked....");
                break;
            }
            done.set(true);
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setDeleteAllMessagesOnStartup(true);
        
        service.setUseJmx(false);

        service.getSystemUsage().getStoreUsage().setLimit(200*1024);
        
        // allow destination to use 50% of store, leaving 50% for DLQ.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setStoreUsageHighWaterMark(50);
        policyMap.put(queueDest, policy);
        policyMap.put(topicDest, policy);
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
