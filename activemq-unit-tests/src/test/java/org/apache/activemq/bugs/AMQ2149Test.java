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
import java.lang.IllegalStateException;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.*;

import org.apache.activemq.AutoFailTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

interface Configurer {
    public void configure(BrokerService broker) throws Exception;
}

public class AMQ2149Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2149Test.class);
    @Rule
    public TestName testName = new TestName();

    private static final String BROKER_CONNECTOR = "tcp://localhost:61617";
    private static final String DEFAULT_BROKER_URL = "failover:("+ BROKER_CONNECTOR
        +")?maxReconnectDelay=1000&useExponentialBackOff=false";
        
    private final String SEQ_NUM_PROPERTY = "seqNum";

    final int MESSAGE_LENGTH_BYTES = 75 * 1024;
    final long SLEEP_BETWEEN_SEND_MS = 25;
    final int NUM_SENDERS_AND_RECEIVERS = 10;
    final Object brokerLock = new Object();
    
    private static final long DEFAULT_BROKER_STOP_PERIOD = 10 * 1000;
    private static final long DEFAULT_NUM_TO_SEND = 1400;
    
    long brokerStopPeriod = DEFAULT_BROKER_STOP_PERIOD;
    long numtoSend = DEFAULT_NUM_TO_SEND;
    long sleepBetweenSend = SLEEP_BETWEEN_SEND_MS;
    String brokerURL = DEFAULT_BROKER_URL;
    
    int numBrokerRestarts = 0;
    final static int MAX_BROKER_RESTARTS = 4;
    BrokerService broker;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    protected File dataDirFile;
    final LoggingBrokerPlugin[] plugins = new LoggingBrokerPlugin[]{new LoggingBrokerPlugin()};
    
    
    public void createBroker(Configurer configurer) throws Exception {
        broker = new BrokerService();
        configurePersistenceAdapter(broker);
        
        broker.getSystemUsage().getMemoryUsage().setLimit(MESSAGE_LENGTH_BYTES * 200 * NUM_SENDERS_AND_RECEIVERS);

        broker.addConnector(BROKER_CONNECTOR);        
        broker.setBrokerName(testName.getMethodName());
        broker.setDataDirectoryFile(dataDirFile);
        if (configurer != null) {
            configurer.configure(broker);
        }
        broker.start();
    }
    
    protected void configurePersistenceAdapter(BrokerService brokerService) throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        LOG.debug("Starting test {}", testName.getMethodName());
        dataDirFile = new File("target/"+ testName.getMethodName());
        numtoSend = DEFAULT_NUM_TO_SEND;
        brokerStopPeriod = DEFAULT_BROKER_STOP_PERIOD;
        sleepBetweenSend = SLEEP_BETWEEN_SEND_MS;
        brokerURL = DEFAULT_BROKER_URL;
    }

    @After
    public void tearDown() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> future = executor.submit(new TeardownTask(brokerLock, broker));
        try {
            LOG.debug("Teardown started.");
            long start = System.currentTimeMillis();
            Boolean result =  future.get(30, TimeUnit.SECONDS);
            long finish = System.currentTimeMillis();
            LOG.debug("Result of teardown: {} after {} ms ", result, (finish - start));
        } catch (TimeoutException e) {
            fail("Teardown timed out");
            AutoFailTestSupport.dumpAllThreads(testName.getMethodName());
        }
        executor.shutdownNow();
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

    HashSet<Connection> connections = new HashSet<Connection>();
    private class Receiver implements MessageListener {

        private final javax.jms.Destination dest;

        private final Connection connection;

        private final Session session;

        private final MessageConsumer messageConsumer;

        private volatile long nextExpectedSeqNum = 1;
                
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
            connections.add(connection);
        }

        public void close() throws JMSException {
            connection.close();
        }
        
        public long getNextExpectedSeqNo() {
            return nextExpectedSeqNum;
        }
        
        final int TRANSACITON_BATCH = 500;
        boolean resumeOnNextOrPreviousIsOk = false;
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
                if (resumeOnNextOrPreviousIsOk) {
                    // after an indoubt commit we need to accept what we get
                    // either a batch replay or next batch
                    if (seqNum != nextExpectedSeqNum) {
                        if (seqNum == nextExpectedSeqNum - TRANSACITON_BATCH) {
                            nextExpectedSeqNum -= TRANSACITON_BATCH;
                            LOG.info("In doubt commit failed, getting replay at:" +  nextExpectedSeqNum);
                        }
                    }
                    resumeOnNextOrPreviousIsOk = false;
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
                ++nextExpectedSeqNum;
                LOG.info("got rollback: " + expectedSometimesOnFailoverRecovery);
                if (expectedSometimesOnFailoverRecovery.getMessage().contains("completion in doubt")) {
                    // in doubt - either commit command or reply missing
                    // don't know if we will get a replay
                    resumeOnNextOrPreviousIsOk = true;
                    LOG.info("in doubt transaction completion: ok to get next or previous batch. next:" + nextExpectedSeqNum);
                } else {
                    resumeOnNextOrPreviousIsOk = false;
                    // batch will be replayed
                    nextExpectedSeqNum -= TRANSACITON_BATCH;
                }

            } catch (Throwable e) {
                LOG.error(dest + " onMessage error:" + e);
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
            connections.add(connection);
        }

        public void run() {
            final String longString = buildLongString();
            while (nextSequenceNumber < numtoSend) {
                try {
                    final Message message = session
                            .createTextMessage(longString);
                    message.setLongProperty(SEQ_NUM_PROPERTY,
                            ++nextSequenceNumber);
                    messageProducer.send(message);
                    
                    if ((nextSequenceNumber % 500) == 0) {
                        LOG.info(dest + " sent " + nextSequenceNumber);
                    }

                } catch (javax.jms.IllegalStateException e) {
                    LOG.error(dest + " bailing on send error", e);
                    exceptions.add(e);
                    break;
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

    // attempt to simply replicate leveldb failure. no joy yet
    public void x_testRestartReReceive() throws Exception {
        createBroker(new Configurer() {
            public void configure(BrokerService broker) throws Exception {
                broker.deleteAllMessages();
            }
        });

        final javax.jms.Destination destination =
                ActiveMQDestination.createDestination("test.dest.X", ActiveMQDestination.QUEUE_TYPE);
        Thread thread = new Thread(new Sender(destination));
        thread.start();
        thread.join();

        Connection connection = new ActiveMQConnectionFactory(brokerURL).createConnection();
        connection.setClientID(destination.toString());
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = session.createConsumer(destination);
        connection.start();

        int batch = 200;
        long expectedSeq;

        final TimerTask restartTask = schedualRestartTask(null, new Configurer() {
            public void configure(BrokerService broker) throws Exception {
            }
        });

        expectedSeq = 0;
        for (int s = 0; s < 4; s++) {
            for (int i = 0; i < batch; i++) {
                Message message = messageConsumer.receive(20000);
                assertNotNull("s:" + s + ", i:" + i, message);
                final long seqNum = message.getLongProperty(SEQ_NUM_PROPERTY);
                assertEquals("expected order s:" + s, expectedSeq++, seqNum);

                if (i > 0 && i%600 == 0) {
                    LOG.info("Commit on %5");
                //    session.commit();
                }
            }
            restartTask.run();
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

    @Test(timeout = 10 * 60 * 1000)
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

    @Test(timeout = 10 * 60 * 1000)
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

    @Test(timeout = 10 * 60 * 1000)
    public void testQueueTransactionalOrderWithRestart() throws Exception {
        doTestTransactionalOrderWithRestart(ActiveMQDestination.QUEUE_TYPE);
    }

    @Test(timeout = 10 * 60 * 1000)
    public void testTopicTransactionalOrderWithRestart() throws Exception {
        doTestTransactionalOrderWithRestart(ActiveMQDestination.TOPIC_TYPE);
    }
    
    public void doTestTransactionalOrderWithRestart(byte destinationType) throws Exception {
        numtoSend = 5000;
        sleepBetweenSend = 3;
        brokerStopPeriod = 10 * 1000;
              
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

    private TimerTask schedualRestartTask(final Timer timer, final Configurer configurer) {
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
                if (++numBrokerRestarts < MAX_BROKER_RESTARTS && timer != null) {
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
        RestartTask task = new RestartTask();
        if (timer != null) {
            timer.schedule(task, brokerStopPeriod);
        }
        return task;
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
        
        final long expiry = System.currentTimeMillis() + 1000 * 60 * 10;
        while(!threads.isEmpty() && exceptions.isEmpty() && System.currentTimeMillis() < expiry) {
            Thread sendThread = threads.firstElement();
            sendThread.join(1000*60*10);
            if (!sendThread.isAlive()) {
                threads.remove(sendThread);
            } else {
                AutoFailTestSupport.dumpAllThreads("Send blocked");
            }
        }
        LOG.info("senders done..." + threads);

        while(!receivers.isEmpty() && System.currentTimeMillis() < expiry) {
            Receiver receiver = receivers.firstElement();
            if (receiver.getNextExpectedSeqNo() >= numtoSend || !exceptions.isEmpty()) {
                receiver.close();
                receivers.remove(receiver);
            }
        }

        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Exception ignored) {}
        }
        connections.clear();

        assertTrue("No timeout waiting for senders/receivers to complete", System.currentTimeMillis() < expiry);
        if (!exceptions.isEmpty()) {
            exceptions.get(0).printStackTrace();
        }

        LOG.info("Dangling threads: " + threads);
        for (Thread dangling : threads) {
            dangling.interrupt();
            dangling.join(10*1000);
        }

        assertTrue("No exceptions", exceptions.isEmpty());
    }

}

class TeardownTask implements Callable<Boolean> {
    private Object brokerLock;
    private BrokerService broker;

    public TeardownTask(Object brokerLock, BrokerService broker) {
        this.brokerLock = brokerLock;
        this.broker = broker;
    }

    @Override
    public Boolean call() throws Exception {
        synchronized(brokerLock) {
            if (broker!= null) {
                broker.stop();
                broker.waitUntilStopped();
            }
        }
        return Boolean.TRUE;
    }
}
