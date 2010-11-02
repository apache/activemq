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
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// variation on AMQ2584 where the DLQ consumer works in parallel to producer so
// that some dups are not suppressed as they are already acked by the consumer
// the audit needs to be disabled to allow these dupes to be consumed
public class AMQ2584ConcurrentDlqTest extends org.apache.activemq.TestSupport {

    static final Log LOG = LogFactory.getLog(AMQ2584ConcurrentDlqTest.class);
    BrokerService broker = null;
    ActiveMQTopic topic;

    ActiveMQConnection consumerConnection = null, producerConnection = null, dlqConnection = null;
    Session consumerSession;
    Session producerSession;
    MessageProducer producer;
    Vector<TopicSubscriber> duralbeSubs = new Vector<TopicSubscriber>();
    final int numMessages = 1000;
    final int numDurableSubs = 2;

    String data;
    private long dlqConsumerLastReceivedTimeStamp;
    private AtomicLong dlqReceivedCount = new AtomicLong(0);

    // 2 deliveries of each message, 3 producers
    CountDownLatch redeliveryConsumerLatch = new CountDownLatch(((2 * numMessages) * numDurableSubs) - 1);
    // should get at least numMessages, possibly more
    CountDownLatch dlqConsumerLatch = new CountDownLatch((numMessages - 1));

    public void testSize() throws Exception {
        openConsumer(redeliveryConsumerLatch);
        openDlqConsumer(dlqConsumerLatch);


        assertEquals(0, broker.getAdminView().getStorePercentUsage());

        for (int i = 0; i < numMessages; i++) {
            sendMessage(false);
        }

        final BrokerView brokerView = broker.getAdminView();

        broker.getSystemUsage().getStoreUsage().isFull();
        LOG.info("store percent usage: " + brokerView.getStorePercentUsage());
        assertTrue("redelivery consumer got all it needs, remaining: "
                + redeliveryConsumerLatch.getCount(), redeliveryConsumerLatch.await(60, TimeUnit.SECONDS));
        assertTrue("dql  consumer got all it needs", dlqConsumerLatch.await(60, TimeUnit.SECONDS));
        closeConsumer();

        LOG.info("Giving dlq a chance to clear down once topic consumer is closed");

        // consumer all of the duplicates that arrived after the first ack
        closeDlqConsumer();

        //get broker a chance to clean obsolete messages, wait 2*cleanupInterval
        Thread.sleep(5000);

        FilenameFilter justLogFiles = new FilenameFilter() {
            public boolean accept(File file, String s) {
                return s.endsWith(".log");
            }
        };
        int numFiles = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getDirectory().list(justLogFiles).length;
        if (numFiles > 2) {
            LOG.info(Arrays.toString(((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getDirectory().list(justLogFiles)));
        }
        LOG.info("num files: " + numFiles);
        assertEquals("kahaDB dir should contain 1 db file,is: " + numFiles, 1, numFiles);
    }

    private void openConsumer(final CountDownLatch latch) throws Exception {
        consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.setClientID("cliID");
        consumerConnection.start();
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
                try {
                    consumerSession.recover();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        };

        for (int i = 1; i <= numDurableSubs; i++) {
            TopicSubscriber sub = consumerSession.createDurableSubscriber(topic, "subName" + i);
            sub.setMessageListener(listener);
            duralbeSubs.add(sub);
        }
    }

    private void openDlqConsumer(final CountDownLatch received) throws Exception {

        dlqConnection = (ActiveMQConnection) createConnection();
        Session dlqSession = dlqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer dlqConsumer = dlqSession.createConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));
        dlqConsumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                if (received.getCount() > 0 && received.getCount() % 200 == 0) {
                    LOG.info("remaining on DLQ: " + received.getCount());
                }
                received.countDown();
                dlqConsumerLastReceivedTimeStamp = System.currentTimeMillis();
                dlqReceivedCount.incrementAndGet();
            }
        });
        dlqConnection.start();
    }


    private void closeConsumer() throws JMSException {
        for (TopicSubscriber sub : duralbeSubs) {
            sub.close();
        }
        if (consumerSession != null) {
            for (int i = 1; i <= numDurableSubs; i++) {
                consumerSession.unsubscribe("subName" + i);
            }
        }
        if (consumerConnection != null) {
            consumerConnection.close();
            consumerConnection = null;
        }
    }

    private void closeDlqConsumer() throws JMSException, InterruptedException {
        final long limit = System.currentTimeMillis() + 30 * 1000;
        if (dlqConsumerLastReceivedTimeStamp > 0) {
            while (System.currentTimeMillis() < dlqConsumerLastReceivedTimeStamp + 5000
                    && System.currentTimeMillis() < limit) {
                LOG.info("waiting for DLQ do drain, receivedCount: " + dlqReceivedCount);
                TimeUnit.SECONDS.sleep(1);
            }
        }
        if (dlqConnection != null) {
            dlqConnection.close();
            dlqConnection = null;
        }
    }

    private void sendMessage(boolean filter) throws Exception {
        if (producerConnection == null) {
            producerConnection = (ActiveMQConnection) createConnection();
            producerConnection.start();
            producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = producerSession.createProducer(topic);
        }

        Message message = producerSession.createMessage();
        message.setStringProperty("data", data);
        producer.send(message);
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setBrokerName("testStoreSize");

        PolicyMap map = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setEnableAudit(false);
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);

        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        configurePersistenceAdapter(broker.getPersistenceAdapter());
        broker.getSystemUsage().getStoreUsage().setLimit(200 * 1000 * 1000);
        broker.start();
    }

    private void configurePersistenceAdapter(PersistenceAdapter persistenceAdapter) {
        Properties properties = new Properties();
        String maxFileLengthVal = String.valueOf(2 * 1024 * 1024);
        properties.put("journalMaxFileLength", maxFileLengthVal);
        properties.put("maxFileLength", maxFileLengthVal);
        properties.put("cleanupInterval", "2000");
        properties.put("checkpointInterval", "2000");
        // there are problems with duplicate dispatch in the cursor, which maintain
        // a map of messages. A dup dispatch can be dropped.
        // see: org.apache.activemq.broker.region.cursors.OrderedPendingList
        // Adding duplicate detection to the default DLQ strategy removes the problem
        // which means we can leave the default for concurrent store and dispatch q
        //properties.put("concurrentStoreAndDispatchQueues", "false");

        IntrospectionSupport.setProperties(persistenceAdapter, properties);
    }

    private void stopBroker() throws Exception {
        if (broker != null)
            broker.stop();
        broker = null;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://testStoreSize?jms.watchTopicAdvisories=false&jms.redeliveryPolicy.maximumRedeliveries=1&jms.redeliveryPolicy.initialRedeliveryDelay=0&waitForStart=5000&create=false");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        StringBuilder sb = new StringBuilder(5000);
        for (int i = 0; i < 5000; i++) {
            sb.append('a');
        }
        data = sb.toString();

        startBroker(true);
        topic = (ActiveMQTopic) createDestination();
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }
}
