/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases;

import junit.framework.TestCase;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerContainer;
import org.activemq.broker.impl.BrokerContainerImpl;
import org.activemq.store.vm.VMPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * TODO this test case relies on a perfect distribution of messages, dispatched one by one to each consumer.
 * So this test case can only really work with an explicit setting of 'prefetch = 1' or something similar.
 * The default out of the box dispatcher eagerly dispatches messages as quickly as possible.
 * 
 * Ensures that if there is a network of brokers that a message is always dispatched to an available consumer * regardless of which broker it is on.
 *
 * @version $Revision: 1.1 $
 */
public class AvailableConsumerTest extends TestCase {
    private static final transient Log log = LogFactory.getLog(AvailableConsumerTest.class);
    private static final long BROKER_INITIALIZATION_DELAY = 7000;

    public void testOneBroker() throws Throwable {
        String[] urls = new String[]{"tcp://localhost:9000"};
        runSimulation(urls, 2, "QUEUE_NAME");
        //runSimulation(urls, 5, "QUEUE_NAME");
        //runSimulation(urls, 10, "QUEUE_NAME");
    }

    public void testTwoBrokers() throws Throwable {
        String[] urls = new String[]{"tcp://localhost:9000", "tcp://localhost:9001"};
        runSimulation(urls, 2, "QUEUE_NAME");
        runSimulation(urls, 5, "QUEUE_NAME");
        runSimulation(urls, 10, "QUEUE_NAME");
    }

    public void testTenBrokers() throws Throwable {
        String[] urls = new String[]{
            "tcp://localhost:9000", "tcp://localhost:9001", "tcp://localhost:9002", "tcp://localhost:9003", "tcp://localhost:9004",
            "tcp://localhost:9005", "tcp://localhost:9006", "tcp://localhost:9007", "tcp://localhost:9008", "tcp://localhost:9009"
        };
        runSimulation(urls, 2, "QUEUE_NAME");
        runSimulation(urls, 5, "QUEUE_NAME");
        runSimulation(urls, 10, "QUEUE_NAME");
    }

    private void runSimulation(String[] brokerURLs, int numConsumers, String queue) throws Throwable {
        assertTrue("More than one consumer is required", numConsumers > 1);

        BrokerThread[] brokers = null;
        BlockingConsumer[] consumers = null;
        MessagePublisher[] publishers = null;

        try {
            String reliableURL = createReliableURL(brokerURLs);
            brokers = createBrokers(brokerURLs);
            consumers = createConsumers(reliableURL, numConsumers, queue);
            publishers = createPublishers(reliableURL, 1, queue);

            // Now get all of the consumers blocked except for one
            {
                for (int i = 0; i < consumers.length - 1; i++) {
                    publishers[0].sendMessage();
                }
                waitUntilNumBlocked(consumers, consumers.length - 1);
            }

            // Now send one more message which should cause all of the consumers to be blocked
            {
                publishers[0].sendMessage();
                waitUntilNumBlocked(consumers, consumers.length);
            }

            // Unblock a consumer and make sure it is unblocked
            {
                synchronized (consumers[0]) {
                    consumers[0].notifyAll();
                }
                waitUntilNumBlocked(consumers, consumers.length - 1);
            }

            // Send another message and make sure it is blocked again
            {
                publishers[0].sendMessage();
                waitUntilNumBlocked(consumers, consumers.length);
            }

            // Finally queue up a message for each consumer but one, then unblock them all and make sure one is still unblocked
            {
                for (int i = 0; i < consumers.length - 1; i++) {
                    publishers[0].sendMessage();
                }

                for (int i = 0; i < consumers.length; i++) {
                    synchronized (consumers[i]) {
                        consumers[i].notifyAll();
                    }
                }

                waitUntilNumBlocked(consumers, consumers.length - 1);
            }
        }
        finally {
            cleanupSimulation(brokers, publishers, consumers);
        }
    }

    private static void cleanupSimulation(BrokerThread[] brokers, MessagePublisher[] publishers, BlockingConsumer[] consumers) {
        try {
            for (int i = 0; i < publishers.length; i++) {
                publishers[i].done();
            }
        }
        catch (Throwable t) {
            log.warn("Non-fatal error during cleanup", t);
        }

        try {
            for (int i = 0; i < consumers.length; i++) {
                // Unblock it in case it is blocked
                synchronized (consumers[i]) {
                    consumers[i].notifyAll();
                }

                consumers[i].done();
            }
        }
        catch (Throwable t) {
            log.warn("Non-fatal error during cleanup", t);
        }

        try {
            for (int i = 0; i < brokers.length; i++) {
                brokers[i].done();
            }
        }
        catch (Throwable t) {
            log.warn("Non-fatal error during cleanup", t);
        }
    }

    private static BrokerThread[] createBrokers(String[] brokerURLs) throws InterruptedException {
        BrokerThread[] threads = new BrokerThread[brokerURLs.length];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new BrokerThread(Integer.toString(i), brokerURLs[i], brokerURLs);
            threads[i].start();
        }

// Delay so that the brokers have a chance to come up fully and connect to each other
        log.debug("Created " + threads.length + " brokers, giving them time to initialize...");
        Object temp = new Object();
        synchronized (temp) {
            temp.wait(BROKER_INITIALIZATION_DELAY * brokerURLs.length);
        }
        log.debug("Brokers should be initialized now");

        return threads;
    }

    private static BlockingConsumer[] createConsumers(String brokerURL, int numConsumers, String queue) throws JMSException {
        BlockingConsumer[] consumers = new BlockingConsumer[numConsumers];
        for (int i = 0; i < consumers.length; i++) {
            consumers[i] = new BlockingConsumer(brokerURL, queue);
        }

        return consumers;
    }

    private static MessagePublisher[] createPublishers(String brokerURL, int numPublishers, String queue) throws JMSException {
        MessagePublisher[] publishers = new MessagePublisher[numPublishers];
        for (int i = 0; i < publishers.length; i++) {
            publishers[i] = new MessagePublisher(brokerURL, queue);
        }

        return publishers;
    }

    private static String createReliableURL(String[] brokerURLs) {
        StringBuffer sb = new StringBuffer("reliable:");
        for (int i = 0; i < brokerURLs.length; i++) {
            if (i != 0) {
                sb.append(',');
            }

            sb.append(brokerURLs[i]);
        }

        return sb.toString();
    }

    private static void waitUntilNumBlocked(BlockingConsumer[] consumers, int expectedNumBlocked) throws InterruptedException {
        boolean found = false;
        int maxIterations = 50;
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            int numBlocked = 0;
            for (int i = 0; i < consumers.length; i++) {
                numBlocked += consumers[i].isBlocked() ? 1 : 0;
            }

            if (numBlocked == expectedNumBlocked) {
                found = true;
                break;
            }

            log.debug("Waiting for " + expectedNumBlocked + " consumers to block, currently only " + numBlocked + " are blocked.");
            Object temp = new Object();
            synchronized (temp) {
                temp.wait(250);
            }
        }

        assertTrue("Never saw " + expectedNumBlocked + " consumers blocked", found);
    }

    private static final class BrokerThread extends Thread {
        private final String m_id;
        private final String m_myURL;
        private final String[] m_linkedURLs;
        private BrokerContainer m_container;

        public BrokerThread(String id, String myURL, String[] linkedURLs) {
            m_id = id;
            m_myURL = myURL;
            m_linkedURLs = linkedURLs;
        }

        public void run() {
            try {
                m_container = new BrokerContainerImpl(m_id);
                m_container.setPersistenceAdapter(new VMPersistenceAdapter());
                m_container.addConnector(m_myURL);

                for (int i = 0; i < m_linkedURLs.length; i++) {
                    if (!m_myURL.equals(m_linkedURLs[i])) {
                        m_container.addNetworkConnector("reliable:" + m_linkedURLs[i]);
                    }
                }

                m_container.start();
            }
            catch (JMSException e) {
                log.error("Unexpected exception", e);
            }
        }

        public void done() {
            try {
                m_container.stop();
            }
            catch (JMSException e) {
                log.error("Unexpected exception", e);
            }
        }
    }

    private static final class MessagePublisher {
        private final String m_url;
        private final Connection m_connection;
        private final Session m_session;
        private final Queue m_queue;
        private final MessageProducer m_producer;

        public MessagePublisher(String url, String queue) throws JMSException {
            this(url, queue, DeliveryMode.PERSISTENT);
        }

        public MessagePublisher(String url, String queue, int deliveryMode) throws JMSException {
            m_url = url;

            m_connection = new ActiveMQConnectionFactory(m_url).createConnection();
            m_connection.start();

            m_session = m_connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            m_queue = m_session.createQueue(queue);

            m_producer = m_session.createProducer(m_queue);
            m_producer.setDeliveryMode(deliveryMode);
        }

        public void done() {
            try {
                m_producer.close();
            }
            catch (Throwable ignored) {
            }

            try {
                m_session.close();
            }
            catch (Throwable ignored) {
            }

            try {
                m_connection.close();
            }
            catch (Throwable ignored) {
            }
        }

        public void sendMessage() throws JMSException {
            Message message = m_session.createMessage();
            m_producer.send(message);
        }
    }

    private static final class BlockingConsumer implements MessageListener {
        private final String m_url;
        private final Connection m_connection;
        private final Session m_session;
        private final Queue m_queue;
        private final MessageConsumer m_consumer;
        private boolean m_blocked;

        public BlockingConsumer(String url, String queue) throws JMSException {
            m_url = url;

            m_connection = new ActiveMQConnectionFactory(m_url).createConnection();
            m_connection.start();

            m_session = m_connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            m_queue = m_session.createQueue(queue);
            m_consumer = m_session.createConsumer(m_queue);
            m_consumer.setMessageListener(this);
            m_blocked = false;
        }

        public boolean isBlocked() {
            return m_blocked;
        }

        public void done() {
            try {
                m_consumer.setMessageListener(null);
            }
            catch (Throwable ignored) {
            }

            try {
                m_consumer.close();
            }
            catch (Throwable ignored) {
            }

            try {
                m_session.close();
            }
            catch (Throwable ignored) {
            }

            try {
                m_connection.close();
            }
            catch (Throwable ignored) {
            }
        }

        public void onMessage(Message message) {
            m_blocked = true;

            synchronized (this) {
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    log.error("Unexpected InterruptedException during onMessage", e);
                }
            }

            m_blocked = false;
        }
    }
}