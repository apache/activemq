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
import javax.jms.Topic;
import javax.jms.TransactionRolledBackException;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

interface Configurer {
    public void configure(BrokerService broker) throws Exception;
}

public class AMQ2149Test extends AutoFailTestSupport {

    private static final Log LOG = LogFactory.getLog(AMQ2149Test.class);

    private static final String BROKER_CONNECTOR = "tcp://localhost:61617";
    private static final String DEFAULT_BROKER_URL = "failover:("+ BROKER_CONNECTOR
        +")?maxReconnectDelay=1000&useExponentialBackOff=false";
        
    private final String SEQ_NUM_PROPERTY = "seqNum";

    final int MESSAGE_LENGTH_BYTES = 75 * 1024;
    final long SLEEP_BETWEEN_SEND_MS = 25;
    final int NUM_SENDERS_AND_RECEIVERS = 10;
    final Object brokerLock = new Object();
    
    private static final long DEFAULT_BROKER_STOP_PERIOD = 20 * 1000;
    private static final long DEFAULT_NUM_TO_SEND = 1400;
    
    long brokerStopPeriod = DEFAULT_BROKER_STOP_PERIOD;
    long numtoSend = DEFAULT_NUM_TO_SEND;
    long sleepBetweenSend = SLEEP_BETWEEN_SEND_MS;
    String brokerURL = DEFAULT_BROKER_URL;
    
    int numBrokerRestarts = 0;
    final static int MAX_BROKER_RESTARTS = 5;
    BrokerService broker;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    private File dataDirFile;
    final LoggingBrokerPlugin[] plugins = new LoggingBrokerPlugin[]{new LoggingBrokerPlugin()};
    
    
    public void createBroker(Configurer configurer) throws Exception {
        broker = new BrokerService();
        configurePersistenceAdapter(broker);
        
        SystemUsage usage = new SystemUsage();
        MemoryUsage memoryUsage = new MemoryUsage();
        memoryUsage.setLimit(MESSAGE_LENGTH_BYTES * 200 * NUM_SENDERS_AND_RECEIVERS);
        usage.setMemoryUsage(memoryUsage);
        broker.setSystemUsage(usage);
        
        

        broker.addConnector(BROKER_CONNECTOR);        
        broker.setBrokerName(getName());
        broker.setDataDirectoryFile(dataDirFile);
        if (configurer != null) {
            configurer.configure(broker);
        }
        broker.start();
    }
    
    protected void configurePersistenceAdapter(BrokerService brokerService) throws Exception {
        AMQPersistenceAdapterFactory persistenceFactory = new AMQPersistenceAdapterFactory();
        persistenceFactory.setDataDirectory(dataDirFile);
        brokerService.setPersistenceFactory(persistenceFactory);
    }

    public void setUp() throws Exception {
        setMaxTestTime(30*60*1000);
        setAutoFail(true);
        dataDirFile = new File("target/"+ getName());
        numtoSend = DEFAULT_NUM_TO_SEND;
        brokerStopPeriod = DEFAULT_BROKER_STOP_PERIOD;
        sleepBetweenSend = SLEEP_BETWEEN_SEND_MS;
        brokerURL = DEFAULT_BROKER_URL;
    }
    
    public void tearDown() throws Exception {
        synchronized(brokerLock) {
            if (broker!= null) {
                broker.stop();
                broker.waitUntilStopped();
            }
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

        private final javax.jms.Destination dest;

        private final Connection connection;

        private final Session session;

        private final MessageConsumer messageConsumer;

        private volatile long nextExpectedSeqNum = 0;
                
        private final boolean transactional;

        private String lastId = null;

        public Receiver(javax.jms.Destination dest, boolean transactional) throws JMSException {
            this.dest = dest;
            this.transactional = transactional;
            connection = new ActiveMQConnectionFactory(brokerURL)
                    .createConnection();
            connection.setClientID(dest.toString());
            session = connection.createSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
            if (ActiveMQDestination.transform(dest).isTopic()) {
                messageConsumer = session.createDurableSubscriber((Topic) dest, dest.toString());
            } else {
                messageConsumer = session.createConsumer(dest);
            }
            messageConsumer.setMessageListener(this);
            connection.start();
        }

        public void close() throws JMSException {
            connection.close();
        }
        
        public long getNextExpectedSeqNo() {
            return nextExpectedSeqNum;
        }
        
        final int TRANSACITON_BATCH = 500;
        public void onMessage(Message message) {
            try {
                final long seqNum = message.getLongProperty(SEQ_NUM_PROPERTY);
                if ((seqNum % TRANSACITON_BATCH) == 0) {
                    LOG.info(dest + " received " + seqNum);
                    
                    if (transactional) {
                        LOG.info("committing..");
                        session.commit();
                    }
                }
                if (seqNum != nextExpectedSeqNum) {
                    LOG.warn(dest + " received " + seqNum
                            + " in msg: " + message.getJMSMessageID()
                            + " expected "
                            + nextExpectedSeqNum
                            + ", lastId: " + lastId 
                            + ", message:" + message);
                    fail(dest + " received " + seqNum + " expected "
                            + nextExpectedSeqNum);
                }
                ++nextExpectedSeqNum;
                lastId = message.getJMSMessageID();
            } catch (TransactionRolledBackException expectedSometimesOnFailoverRecovery) {
                LOG.info("got rollback: " + expectedSometimesOnFailoverRecovery);
                // batch will be replayed
                nextExpectedSeqNum -= (TRANSACITON_BATCH -1);
            } catch (Throwable e) {
                LOG.error(dest + " onMessage error", e);
                exceptions.add(e);
            }
        }

    }

    private class Sender implements Runnable {

        private final javax.jms.Destination dest;

        private final Connection connection;

        private final Session session;

        private final MessageProducer messageProducer;

        private volatile long nextSequenceNumber = 0;

        public Sender(javax.jms.Destination dest) throws JMSException {
            this.dest = dest;
            connection = new ActiveMQConnectionFactory(brokerURL)
                    .createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageProducer = session.createProducer(dest);
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();
        }

        public void run() {
            final String longString = buildLongString();
            while (nextSequenceNumber < numtoSend) {
                try {
                    final Message message = session
                            .createTextMessage(longString);
                    message.setLongProperty(SEQ_NUM_PROPERTY,
                            nextSequenceNumber);
                    ++nextSequenceNumber;
                    messageProducer.send(message);
                    
                    if ((nextSequenceNumber % 500) == 0) {
                        LOG.info(dest + " sent " + nextSequenceNumber);
                    }
                        
                } catch (Exception e) {
                    LOG.error(dest + " send error", e);
                    exceptions.add(e);
                }
                if (sleepBetweenSend > 0) {
                    try {
                        Thread.sleep(sleepBetweenSend);
                    } catch (InterruptedException e) {
                        LOG.warn(dest + " sleep interrupted", e);
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

    // no need to run this unless there are some issues with the others
    public void noProblem_testOrderWithRestartAndVMIndex() throws Exception {
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
        schedualRestartTask(timer, new Configurer() {
            public void configure(BrokerService broker) throws Exception {    
            }
        });
        
        try {
            verifyOrderedMessageReceipt();
        } finally {
            timer.cancel();
        }
        
        verifyStats(true);
    }
        
    public void testTopicOrderWithRestart() throws Exception {
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.deleteAllMessages();
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, null);
        
        try {
            verifyOrderedMessageReceipt(ActiveMQDestination.TOPIC_TYPE);
        } finally {
            timer.cancel();
        }
        
        verifyStats(true);
    }

    public void testQueueTransactionalOrderWithRestart() throws Exception {
        doTestTransactionalOrderWithRestart(ActiveMQDestination.QUEUE_TYPE);
    }
    
    public void testTopicTransactionalOrderWithRestart() throws Exception {
        doTestTransactionalOrderWithRestart(ActiveMQDestination.TOPIC_TYPE);
    }
    
    public void doTestTransactionalOrderWithRestart(byte destinationType) throws Exception {
        numtoSend = 10000;
        sleepBetweenSend = 3;
        brokerStopPeriod = 30 * 1000;
              
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.deleteAllMessages();
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, null);
        
        try {
            verifyOrderedMessageReceipt(destinationType, 1, true);
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
                broker.setPlugins(plugins);
                broker.deleteAllMessages();     
            }
        });
        
        final Timer timer = new Timer();
        schedualRestartTask(timer, new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                AMQPersistenceAdapterFactory persistenceFactory =
                    (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
                persistenceFactory.setForceRecoverReferenceStore(true);
                broker.setPlugins(plugins);
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
                // all bets are off w.r.t stats as there may be duplicate sends and duplicate
                // dispatches, all of which will be suppressed - either by the reference store
                // not allowing duplicate references or consumers acking duplicates
                LOG.info("with restart: not asserting qneue/dequeue stat match for: " + dest.getName()
                        + " " + stats.getEnqueues().getCount() + " <= " +stats.getDequeues().getCount());
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
                if (++numBrokerRestarts < MAX_BROKER_RESTARTS) {
                    // do it again
                    try {
                        timer.schedule(new RestartTask(), brokerStopPeriod);
                    } catch (IllegalStateException ignore_alreadyCancelled) {   
                    }
                } else {
                    LOG.info("no longer stopping broker on reaching Max restarts: " + MAX_BROKER_RESTARTS);
                }
            } 
        }
        timer.schedule(new RestartTask(), brokerStopPeriod);
    }
    
    private void verifyOrderedMessageReceipt(byte destinationType) throws Exception {
        verifyOrderedMessageReceipt(destinationType, NUM_SENDERS_AND_RECEIVERS, false);
    }
    
    private void verifyOrderedMessageReceipt() throws Exception {
        verifyOrderedMessageReceipt(ActiveMQDestination.QUEUE_TYPE, NUM_SENDERS_AND_RECEIVERS, false);
    }
    
    private void verifyOrderedMessageReceipt(byte destinationType, int concurrentPairs, boolean transactional) throws Exception {
                
        Vector<Thread> threads = new Vector<Thread>();
        Vector<Receiver> receivers = new Vector<Receiver>();
        
        for (int i = 0; i < concurrentPairs; ++i) {
            final javax.jms.Destination destination =
                    ActiveMQDestination.createDestination("test.dest." + i, destinationType);
            receivers.add(new Receiver(destination, transactional));
            Thread thread = new Thread(new Sender(destination));
            thread.start();
            threads.add(thread);
        }
        
        final long expiry = System.currentTimeMillis() + 1000 * 60 * 30;
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
            if (receiver.getNextExpectedSeqNo() >= numtoSend || !exceptions.isEmpty()) {
                receiver.close();
                receivers.remove(receiver);
            }
        }
        assertTrue("No timeout waiting for senders/receivers to complete", System.currentTimeMillis() < expiry);
        if (!exceptions.isEmpty()) {
            exceptions.get(0).printStackTrace();
        }
        assertTrue("No exceptions", exceptions.isEmpty());
    }

}
