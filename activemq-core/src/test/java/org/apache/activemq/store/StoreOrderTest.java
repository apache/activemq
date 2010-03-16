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
package org.apache.activemq.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//  https://issues.apache.org/activemq/browse/AMQ-2594
public abstract class StoreOrderTest {

    private static final Log LOG = LogFactory.getLog(StoreOrderTest.class);
    
    protected BrokerService broker;
    private ActiveMQConnection connection;
    public Destination destination = new ActiveMQQueue("StoreOrderTest?consumer.prefetchSize=0");
    
    protected abstract void setPersistentAdapter(BrokerService brokerService) throws Exception;
    protected void dumpMessages() throws Exception {}

    public class TransactedSend implements Runnable {

        private CountDownLatch readyForCommit;
        private CountDownLatch firstDone;
        private boolean first;
        private Session session;
        private MessageProducer producer;

        public TransactedSend(CountDownLatch readyForCommit,
                CountDownLatch firstDone, boolean b) throws Exception {
            this.readyForCommit = readyForCommit;
            this.firstDone = firstDone;
            this.first = b;
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            producer = session.createProducer(destination);
        }

        public void run() {
            try {
                if (!first) {              
                    firstDone.await(30, TimeUnit.SECONDS);
                }
                producer.send(session.createTextMessage(first ? "first" : "second"));
                if (first) {
                    firstDone.countDown();
                }
                readyForCommit.countDown();
            
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected ex on run " + e);
            }
        }
        
        public void commit() throws Exception {
            session.commit();
            session.close();
        }
    }

    @Before
    public void setup() throws Exception {
        broker = createBroker();
        initConnection();
    }
    
    public void initConnection() throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setWatchTopicAdvisories(false);
        connection.start();
    }

    @After
    public void stopBroker() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }
    
    @Test
    public void validateUnorderedTxCommit() throws Exception {
        
        Executor executor = Executors.newCachedThreadPool();
        CountDownLatch readyForCommit = new CountDownLatch(2);
        CountDownLatch firstDone = new CountDownLatch(1);
        
        TransactedSend first = new TransactedSend(readyForCommit, firstDone, true);
        TransactedSend second = new TransactedSend(readyForCommit, firstDone, false);
        executor.execute(first);
        executor.execute(second);
        
        assertTrue("both started", readyForCommit.await(20, TimeUnit.SECONDS));
        
        LOG.info("commit out of order");        
        // send interleaved so sequence id at time of commit could be reversed
        second.commit();
        
        // force usage over the limit before second commit to flush cache
        enqueueOneMessage();
        
        // can get lost in the cursor as it is behind the last sequenceId that was cached
        first.commit();
        
        LOG.info("send/commit done..");
        
        dumpMessages();
        
        String received1, received2, received3 = null;
        if (true) {
            LOG.info("receive and rollback...");
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            received1 = receive(session);
            received2 = receive(session);
            received3 = receive(session);
            
            assertEquals("second", received1);
            assertEquals("middle", received2);
            assertEquals("first", received3);
            
            session.rollback();
            session.close();
        }
        
        
        LOG.info("restart broker");
        stopBroker();
        broker = createRestartedBroker();
        initConnection();
        
        if (true) {
            LOG.info("receive and rollback after restart...");
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            received1 = receive(session);
            received2 = receive(session);
            received3 = receive(session);
            assertEquals("second", received1);
            assertEquals("middle", received2);
            assertEquals("first", received3);
            session.rollback();
            session.close();
        }
        
        LOG.info("receive and ack each message");
        received1 = receiveOne();
        received2 = receiveOne();
        received3 = receiveOne();
        
        assertEquals("second", received1);
        assertEquals("middle", received2);
        assertEquals("first", received3);
    }
    
    private void enqueueOneMessage() throws Exception {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage("middle"));
        session.commit();
        session.close();
    }


    private String receiveOne() throws Exception {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        String received = receive(session);
        session.commit();
        session.close();
        return received;
    }
    
    private String receive(Session session) throws Exception {
        MessageConsumer consumer = session.createConsumer(destination);
        String result = null;
        TextMessage message = (TextMessage) consumer.receive(5000);
        if (message != null) {
            LOG.info("got message: " + message);
            result  = message.getText();
        }
        consumer.close();
        return result;
    }

    protected BrokerService createBroker() throws Exception {
        boolean deleteMessagesOnStartup = true;
        return startBroker(deleteMessagesOnStartup);
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        boolean deleteMessagesOnStartup = false;
        return startBroker(deleteMessagesOnStartup);
    }   

    protected BrokerService startBroker(boolean deleteMessagesOnStartup) throws Exception {
        BrokerService newBroker = new BrokerService();   
        configureBroker(newBroker);
        newBroker.setDeleteAllMessagesOnStartup(deleteMessagesOnStartup);
        newBroker.start();
        return newBroker;
    }
    
    protected void configureBroker(BrokerService brokerService) throws Exception {
        setPersistentAdapter(brokerService);
        brokerService.setAdvisorySupport(false);
        
        PolicyMap map = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setMemoryLimit(1024*3);
        defaultEntry.setCursorMemoryHighWaterMark(68);
        map.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(map);
    }

}