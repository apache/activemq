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
package org.apache.activemq.broker.region.cursors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullDestinationMemoryMessageExpirationTest {
    private static final Logger LOG = LoggerFactory.getLogger(FullDestinationMemoryMessageExpirationTest.class);

    private static final long DESTINATION_MEMORY_LIMIT = 1024 * 1024; // 1 MB destination memory limit
    private static final long BROKER_MEMORY_LIMIT = 64 * DESTINATION_MEMORY_LIMIT; // Broker memory limit has to be bigger than destination memory limit
    private static final long BROKER_TEMP_USAGE_LIMIT = 64 * DESTINATION_MEMORY_LIMIT;
    private static final String BROKER_DATA_DIRECTORY = "target/test-classes/" +
        FullDestinationMemoryMessageExpirationTest.class.getName().replace('.',  '/') +
        "-activemq-data";
    private static final String BROKER_URL = "vm://" + BrokerService.DEFAULT_BROKER_NAME;
    private static final String QUEUE_NAME = "NON_PERSISTENT_TEST";
    private static final String MESSAGE_ID_PROPERTY_NAME = "MessageId";

    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        // Delete AMQ data directory
        FileUtils.deleteDirectory(new File(BROKER_DATA_DIRECTORY));

        // Configure/Start Broker
        brokerService = new BrokerService();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setProducerFlowControl(false);
        defaultEntry.setMemoryLimit(DESTINATION_MEMORY_LIMIT);
        defaultEntry.setExpireMessagesPeriod(0);  // Disable background message expiration process
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(policyMap);
        brokerService.setUseJmx(false);
        brokerService.setDataDirectory(BROKER_DATA_DIRECTORY);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File(brokerService.getBrokerDataDirectory(),"KahaDB"));
        brokerService.setPersistenceAdapter(persistenceAdapter);
        brokerService.getSystemUsage().getTempUsage().setLimit(BROKER_TEMP_USAGE_LIMIT);
        brokerService.getSystemUsage().getMemoryUsage().setLimit(BROKER_MEMORY_LIMIT);
        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            // Stop Broker
            if(brokerService != null)
                brokerService.stop();
        } finally {
            // Delete AMQ data directory
            FileUtils.deleteDirectory(new File(BROKER_DATA_DIRECTORY));
        }
    }

    @Test
    public void destinationMemoryFullMessageExpirationTest() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

        // Producer
        final long messageTtl = 50L;
        Producer producer = new Producer(connectionFactory, QUEUE_NAME, messageTtl, 512);
        Thread produderThread = new Thread(producer, "Producer");
        produderThread.start();

        // Wait till temporary storage is used
        while(getTempPercentUsage(QUEUE_NAME) <= 0) {
            Thread.sleep(20);
        }

        // Consumer
        Consumer consumer = new Consumer(connectionFactory, QUEUE_NAME);
        Thread consumerThread = new Thread(consumer, "Consumer");
        consumerThread.start();

        // Stop Producer after at least one message is received
        while (consumer.getReceivedMessages().size() <= 0) {
            Thread.sleep(20);
        }
        producer.stop();
        produderThread.join();
        assertNull(producer.getException());
        LOG.info(String.format(
            "Producer: SentMessageCount=%d, ExpirationCount=%d, QueueSize=%d (MemoryPercentageUsage=%d%%, TempPercentUsage=%d%%)",
            producer.getSentMessages(),
            getExpirationCount(QUEUE_NAME),
            getQueueSize(QUEUE_NAME),
            getMemoryPercentUsage(QUEUE_NAME),
            getTempPercentUsage(QUEUE_NAME)
        ));


        // Wait till received messages + expiration messages = sentMessages (or timeout expired)
        final long sentMessagesCount = producer.getSentMessages();
        final long deadlineMs = System.currentTimeMillis() + (60 * 1000);
        long processedMessagesCount;
        do {
            processedMessagesCount = consumer.getReceivedMessages().size() + getExpirationCount(QUEUE_NAME);
        } while(processedMessagesCount < sentMessagesCount && System.currentTimeMillis() < deadlineMs);


        // Stop Consumer
        consumer.stop();
        consumerThread.join();
        assertNull(consumer.getException());
        LOG.info(String.format(
            "Consumer: ReceivedMessageCount=%d, ExpirationCount=%d, QueueSize=%d (SentMessageCount=%d, MemoryPercentageUsage=%d%%, TempPercentUsage=%d%%)",
            consumer.getReceivedMessages().size(),
            getExpirationCount(QUEUE_NAME),
            getQueueSize(QUEUE_NAME),
            producer.getSentMessages(),
            getMemoryPercentUsage(QUEUE_NAME),
            getTempPercentUsage(QUEUE_NAME)
        ));
        assertEquals(0, getQueueSize(QUEUE_NAME));
        assertEquals(sentMessagesCount, processedMessagesCount);
    }

    private long getQueueSize(String queueName) throws Exception {
        org.apache.activemq.broker.region.Destination destination = brokerService.getDestination(
            new ActiveMQQueue(queueName)
        );
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    private long getExpirationCount(String queueName) throws Exception {
        org.apache.activemq.broker.region.Destination destination = brokerService.getDestination(
            new ActiveMQQueue(queueName)
        );
        return destination.getDestinationStatistics().getExpired().getCount();
    }

    private int getMemoryPercentUsage(String queueName) throws Exception {
        org.apache.activemq.broker.region.Destination destination = brokerService.getDestination(
            new ActiveMQQueue(queueName)
        );
        return destination.getMemoryUsage().getPercentUsage();
  }

    private int getTempPercentUsage(String queueName) throws Exception {
        org.apache.activemq.broker.region.Destination destination = brokerService.getDestination(
            new ActiveMQQueue(queueName)
        );
        return destination.getTempUsage().getPercentUsage();
    }

    private class Producer implements Runnable {
        private static final String MESSAGE_DATA = "012346789";

        private final AtomicLong messageId = new AtomicLong();
        private final AtomicReference<Exception> exception = new AtomicReference<>();
        private final String queueName;
        private final long messageTtl;
        private final int messageBodySize;
        private final Connection con;
        private volatile boolean isStopped = false;

        public Producer(
          ConnectionFactory connectionFactory, String queueName, long messageTtl, int messageBodySize
        ) throws JMSException {
            this.queueName = queueName;
            this.messageTtl = messageTtl;
            this.messageBodySize = messageBodySize;
            this.con = connectionFactory.createConnection();
        }

        @Override
        public void run() {
            try {
              while (!isStopped) {
                  Message message = sendMessage(
                      con, queueName, messageId.incrementAndGet(), messageTtl, messageBodySize
                  );
                  logMessageSent(message);
              }
            } catch(Exception e) {
                exception.set(e);
            } finally {
              try {
                  close();
              } catch(Exception e) {
                  exception.set(e);
              }
            }
        }

        public void stop() {
            isStopped = true;
        }

        public long getSentMessages() {
            return messageId.get();
        }

        public Exception getException() {
            return exception.get();
        }

        private Message sendMessage(
          Connection con, String queueName, long messageId, long messageTTL, int messageBodySize
        ) throws Exception {
            TextMessage message;
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                Destination destination = new ActiveMQQueue(queueName);
                MessageProducer producer = session.createProducer(destination);
                try {
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    if (messageTTL > 0 ) {
                        producer.setTimeToLive(messageTTL);
                    }
                    message = session.createTextMessage(createMessageBody(messageBodySize));
                    message.setStringProperty(MESSAGE_ID_PROPERTY_NAME, String.valueOf(messageId));
                    producer.send(message);
                } finally {
                    producer.close();
                }
            } finally {
                session.close();
            }
            return message;
        }

        private String createMessageBody(int size) {
            StringBuilder sb = new StringBuilder(size);
            int messageDataLength = MESSAGE_DATA.length();
            for (int i = 0; i < size; i++) {
                sb.append(MESSAGE_DATA.charAt(i % messageDataLength));
            }
            return sb.toString();
        }

        private void logMessageSent(Message message) throws Exception {
            LOG.debug(String.format(
                "MessageId %s sent (Count=%d), MemoryPercentageUsage=%d%%, TempPercentUsage=%d%%, ExpirationCount=%d, QueueSize=%d",
                message.getStringProperty(MESSAGE_ID_PROPERTY_NAME),
                messageId.get(),
                getMemoryPercentUsage(queueName),
                getTempPercentUsage(queueName),
                getExpirationCount(queueName),
                getQueueSize(queueName)
            ));
        }

        private void close() throws JMSException {
            con.close();
        }

    }

    private class Consumer implements Runnable {
        private final List<Message> receivedMessages = new ArrayList<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();
        private final String queueName;
        private final Connection con;
        private volatile boolean isStopped = false;

        public Consumer(ConnectionFactory connectionFactory, String queueName) throws JMSException {
            this.queueName = queueName;
            this.con = connectionFactory.createConnection();
            this.con.start();
        }

        @Override
        public void run() {
            try {
                while (!isStopped) {
                    Message message = receiveMessage(con, queueName);
                    if (message != null) {
                        addReceivedMessage(message);
                    }
                    logMessageReceived(message);
                }
            } catch(Exception e) {
                exception.set(e);
            } finally {
              try {
                  close();
              } catch(Exception e) {
                  exception.set(e);
              }
            }
        }

        public void stop() {
            isStopped = true;
        }

        public List<Message> getReceivedMessages() {
            synchronized(this) {
                return new ArrayList<>(receivedMessages);
            }
        }

        public Exception getException() {
            return exception.get();
        }

        private Message receiveMessage(Connection con, String queueName) throws Exception {
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                Destination destination = new ActiveMQQueue(queueName);
                MessageConsumer consumer = session.createConsumer(destination);
                try {
                    return consumer.receive(1000);
                } finally {
                    consumer.close();
                }
            } finally {
                session.close();
            }
        }

        private void logMessageReceived(Message message) throws Exception {
            int memoryPercentUsage = getMemoryPercentUsage(queueName);
            int tempPercentUsage = getTempPercentUsage(queueName);
            long expirationCount = getExpirationCount(queueName);
            long queueSize = getQueueSize(queueName);
            if(message != null) {
                LOG.debug(String.format(
                    "MessageId %s received (Count=%d), MemoryPercentageUsage=%d%%, TempPercentUsage=%d%%, ExpirationCount=%d, QueueSize=%d",
                    message.getStringProperty(MESSAGE_ID_PROPERTY_NAME),
                    receivedMessages.size(),
                    memoryPercentUsage,
                    tempPercentUsage,
                    expirationCount,
                    queueSize
                ));
            } else {
                LOG.debug(String.format(
                    "Message wasn't receive, MemoryPercentageUsage=%d%%, TempPercentUsage=%d%%, ExpirationCount=%d, QueueSize=%d",
                    memoryPercentUsage,
                    tempPercentUsage,
                    expirationCount,
                    queueSize
                ));
            }
        }

        private void addReceivedMessage(Message message) {
            synchronized(receivedMessages) {
                receivedMessages.add(message);
            }
        }

        private void close() throws JMSException {
            con.close();
        }

    }

}
