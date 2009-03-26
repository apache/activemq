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

package org.apache.activemq.bugs;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

interface Configurer {
    public void configure(BrokerService broker) throws Exception;
}

public class AMQ2149Test extends TestCase {

    private static final Log LOG = LogFactory.getLog(AMQ2149Test.class);

    private static final long BROKER_STOP_PERIOD = 20 * 1000;

    private static final String BROKER_CONNECTOR = "tcp://localhost:61617";
    private static final String BROKER_URL = "failover:("+ BROKER_CONNECTOR
        +")?maxReconnectDelay=1000&useExponentialBackOff=false";
        
    private final String SEQ_NUM_PROPERTY = "seqNum";

    final int MESSAGE_LENGTH_BYTES = 75000;
    final int MAX_TO_SEND  = 1500;
    final long SLEEP_BETWEEN_SEND_MS = 3;
    final int NUM_SENDERS_AND_RECEIVERS = 10;
    final Object brokerLock = new Object();
    
    BrokerService broker;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    private File dataDirFile;
    
    public void createBroker(Configurer configurer) throws Exception {
        broker = new BrokerService();
        AMQPersistenceAdapterFactory persistenceFactory = new AMQPersistenceAdapterFactory();
        persistenceFactory.setDataDirectory(dataDirFile);
        broker.setPersistenceFactory(persistenceFactory);

        broker.addConnector(BROKER_CONNECTOR);        
        broker.setBrokerName(getName());
        broker.setDataDirectoryFile(dataDirFile);
        if (configurer != null) {
            configurer.configure(broker);
        }
        broker.start();
    }
    
    public void setUp() throws Exception {
        dataDirFile = new File("target/"+ getName());
    }
    
    public void tearDown() throws Exception {
        synchronized(brokerLock) {
            broker.stop();
            broker.waitUntilStopped();
        }
        exceptions.clear();
    }
    
    private String buildLongString() {
        final StringBuilder stringBuilder = new StringBuilder(
                MESSAGE_LENGTH_BYTES);
        for (int i = 0; i < MESSAGE_LENGTH_BYTES; ++i) {
            stringBuilder.append((int) (Math.random() * 10));
        }
        return stringBuilder.toString();
    }

    private class Receiver implements MessageListener {

        private final String queueName;

        private final Connection connection;

        private final Session session;

        private final MessageConsumer messageConsumer;

        private volatile long nextExpectedSeqNum = 0;
        
        private String lastId = null;

        public Receiver(String queueName) throws JMSException {
            this.queueName = queueName;
            connection = new ActiveMQConnectionFactory(BROKER_URL)
                    .createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageConsumer = session.createConsumer(new ActiveMQQueue(
                    queueName));
            messageConsumer.setMessageListener(this);
            connection.start();
        }

        public void close() throws JMSException {
            connection.close();
        }
        
        public long getNextExpectedSeqNo() {
            return nextExpectedSeqNum;
        }
        
        public void onMessage(Message message) {
            try {
                final long seqNum = message.getLongProperty(SEQ_NUM_PROPERTY);
                if ((seqNum % 500) == 0) {
                    LOG.info(queueName + " received " + seqNum);
                }
                if (seqNum != nextExpectedSeqNum) {
                    LOG.warn(queueName + " received " + seqNum
                            + " in msg: " + message.getJMSMessageID()
                            + " expected "
                            + nextExpectedSeqNum
                            + ", lastId: " + lastId 
                            + ", message:" + message);
                    fail(queueName + " received " + seqNum + " expected "
                            + nextExpectedSeqNum);
                }
                ++nextExpectedSeqNum;
                lastId = message.getJMSMessageID();
            } catch (Throwable e) {
                LOG.error(queueName + " onMessage error", e);
                exceptions.add(e);
            }
        }

    }

    private class Sender implements Runnable {

        private final String queueName;

        private final Connection connection;

        private final Session session;

        private final MessageProducer messageProducer;

        private volatile long nextSequenceNumber = 0;

        public Sender(String queueName) throws JMSException {
            this.queueName = queueName;
            connection = new ActiveMQConnectionFactory(BROKER_URL)
                    .createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageProducer = session.createProducer(new ActiveMQQueue(
                    queueName));
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();
        }

        public void run() {
            final String longString = buildLongString();
            while (nextSequenceNumber < MAX_TO_SEND) {
                try {
                    final Message message = session
                            .createTextMessage(longString);
                    message.setLongProperty(SEQ_NUM_PROPERTY,
                            nextSequenceNumber);
                    ++nextSequenceNumber;
                    messageProducer.send(message);
                } catch (Exception e) {
                    LOG.error(queueName + " send error", e);
                    exceptions.add(e);
                }
                if (SLEEP_BETWEEN_SEND_MS > 0) {
                    try {
                        Thread.sleep(SLEEP_BETWEEN_SEND_MS);
                    } catch (InterruptedException e) {
                        LOG.warn(queueName + " sleep interrupted", e);
                    }
                }
            }
            try {
                connection.close();
            } catch (JMSException ignored) {
            }
        }
    }

    // no need to run this unless there are some issues with the others
    public void vanilaVerify_testOrder() throws Exception {
        
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
               broker.deleteAllMessages();            
            }
        });
        
        verifyOrderedMessageReceipt();
        verifyStats(false);
    }

    public void testOrderWithMemeUsageLimit() throws Exception {
        
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                SystemUsage usage = new SystemUsage();
                MemoryUsage memoryUsage = new MemoryUsage();
                memoryUsage.setLimit(MESSAGE_LENGTH_BYTES * 5 * NUM_SENDERS_AND_RECEIVERS);
                usage.setMemoryUsage(memoryUsage);
                broker.setSystemUsage(usage);
                
                broker.deleteAllMessages();            
            }
        });
        
        verifyOrderedMessageReceipt();
        verifyStats(false);
    }

    public void testOrderWithRestartAndVMIndex() throws Exception {
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                AMQPersistenceAdapterFactory persistenceFactory =
                    (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
                persistenceFactory.setPersistentIndex(false);
                broker.deleteAllMessages();     
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                AMQPersistenceAdapterFactory persistenceFactory =
                    (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
                persistenceFactory.setPersistentIndex(false);
            }
        });
        
        try {
            verifyOrderedMessageReceipt();
        } finally {
            timer.cancel();
        }
        verifyStats(true);
    }


    public void testOrderWithRestart() throws Exception {
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.deleteAllMessages();     
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, null);
        
        try {
            verifyOrderedMessageReceipt();
        } finally {
            timer.cancel();
        }
        
        verifyStats(true);
    }
    
    
    public void testOrderWithRestartAndNoCache() throws Exception {
        
        PolicyEntry noCache = new PolicyEntry();
        noCache.setUseCache(false);
        final PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(noCache);

        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.setDestinationPolicy(policyMap);
                broker.deleteAllMessages();
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.setDestinationPolicy(policyMap);
            }
        });
        
        try {
            verifyOrderedMessageReceipt();
        } finally {
            timer.cancel();
        }
        
        verifyStats(true);
    }


    // no need to run this unless there are issues with the other restart tests
  
    public void eaiserToRepoduce_testOrderWithRestartWithForceRecover() throws Exception {
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                AMQPersistenceAdapterFactory persistenceFactory =
                    (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
                persistenceFactory.setForceRecoverReferenceStore(true);
                broker.deleteAllMessages();     
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                AMQPersistenceAdapterFactory persistenceFactory =
                    (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
                persistenceFactory.setForceRecoverReferenceStore(true);
            }
        });
        
        try {
            verifyOrderedMessageReceipt();
        } finally {
            timer.cancel();
        }
        
        verifyStats(true);
    }

    private void verifyStats(boolean brokerRestarts) throws Exception {
        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        
        for (Destination dest : regionBroker.getQueueRegion().getDestinationMap().values()) {
            DestinationStatistics stats = dest.getDestinationStatistics();
            if (brokerRestarts) {
                assertTrue("qneue/dequeue match for: " + dest.getName(),
                        stats.getEnqueues().getCount() <= stats.getDequeues().getCount());
            } else {
                assertEquals("qneue/dequeue match for: " + dest.getName(),
                        stats.getEnqueues().getCount(), stats.getDequeues().getCount());   
            }
        }
    }

    private void schedualRestartTask(final Timer timer, final Configurer configurer) {
        class RestartTask extends TimerTask {
            public void run() {
                synchronized (brokerLock) {
                    LOG.info("stopping broker..");
                    try {
                        broker.stop();
                        broker.waitUntilStopped();
                    } catch (Exception e) {
                        LOG.error("ex on broker stop", e);
                        exceptions.add(e);
                    }
                    LOG.info("restarting broker");
                    try {
                        createBroker(configurer);
                        broker.waitUntilStarted();
                    } catch (Exception e) {
                        LOG.error("ex on broker restart", e);
                        exceptions.add(e);
                    }
                }
                // do it again
                try {
                    timer.schedule(new RestartTask(), BROKER_STOP_PERIOD);
                } catch (IllegalStateException ignore_alreadyCancelled) {   
                }
            } 
        }
        timer.schedule(new RestartTask(), BROKER_STOP_PERIOD);
    }
    
    private void verifyOrderedMessageReceipt() throws Exception {
        
        Vector<Thread> threads = new Vector<Thread>();
        Vector<Receiver> receivers = new Vector<Receiver>();
        
        for (int i = 0; i < NUM_SENDERS_AND_RECEIVERS; ++i) {
            final String queueName = "test.queue." + i;
            receivers.add(new Receiver(queueName));
            Thread thread = new Thread(new Sender(queueName));
            thread.start();
            threads.add(thread);
        }
        
        final long expiry = System.currentTimeMillis() + 1000 * 60 * 20;
        while(!threads.isEmpty() && exceptions.isEmpty() && System.currentTimeMillis() < expiry) {
            Thread sendThread = threads.firstElement();
            sendThread.join(1000*10);
            if (!sendThread.isAlive()) {
                threads.remove(sendThread);
            }
        }
        LOG.info("senders done...");
        
        while(!receivers.isEmpty() && System.currentTimeMillis() < expiry) {
            Receiver receiver = receivers.firstElement();
            if (receiver.getNextExpectedSeqNo() >= MAX_TO_SEND || !exceptions.isEmpty()) {
                receiver.close();
                receivers.remove(receiver);
            }
        }
        assertTrue("No timeout waiting for senders/receivers to complete", System.currentTimeMillis() < expiry);
        assertTrue("No exceptions", exceptions.isEmpty());
    }

}
