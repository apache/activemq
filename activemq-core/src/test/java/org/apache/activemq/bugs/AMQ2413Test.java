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
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;

public class AMQ2413Test extends CombinationTestSupport implements MessageListener {
    BrokerService broker;
    private ActiveMQConnectionFactory factory;

    private static final int HANG_THRESHOLD = 60;
    private static final int SEND_COUNT = 10000;
    private static final int RECEIVER_THINK_TIME = 1;
    private static final int CONSUMER_COUNT = 1;
    private static final int PRODUCER_COUNT = 50;

    public int deliveryMode = DeliveryMode.NON_PERSISTENT;
    public int ackMode = Session.DUPS_OK_ACKNOWLEDGE;
    public boolean useVMCursor = false;
    public boolean useOptimizeAcks = false;

    private ArrayList<Service> services = new ArrayList<Service>(CONSUMER_COUNT + PRODUCER_COUNT);
    AtomicInteger count = new AtomicInteger(0);
    Semaphore receivedMessages;
    AtomicBoolean running = new AtomicBoolean(false);

    public void initCombos() {
        addCombinationValues("deliveryMode", new Object[] { DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT });
        addCombinationValues("ackMode", new Object[] { Session.DUPS_OK_ACKNOWLEDGE, Session.AUTO_ACKNOWLEDGE });
        addCombinationValues("useVMCursor", new Object[] { true, false });
        //addCombinationValues("useOptimizeAcks", new Object[] {true, false});
    }

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "test-data" + File.separator + "AMQ2401Test");
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://0.0.0.0:2401");
        PolicyMap policies = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setMemoryLimit(1024 * 1024);
        entry.setProducerFlowControl(true);
        if (useVMCursor) {
            entry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        }
        entry.setQueue(">");
        policies.setDefaultEntry(entry);
        broker.setDestinationPolicy(policies);
        broker.start();
        broker.waitUntilStarted();

        count.set(0);
        receivedMessages = new Semaphore(0);

        factory = new ActiveMQConnectionFactory("tcp://0.0.0.0:2401");
        //factory = new ActiveMQConnectionFactory("vm://localhost?broker.useJmx=false&broker.persistent=false");
        setAutoFail(true);
        super.setUp();
    }

    protected void tearDown() throws Exception {
        running.set(false);
        for(Service service : services)
        {
            service.close();
        }
        
        broker.stop();
        broker.waitUntilStopped();
        
        super.tearDown();
    }

    public void testReceipt() throws Exception {

        running.set(true);
        TestProducer p = null;
        TestConsumer c = null;
        try {

            for (int i = 0; i < CONSUMER_COUNT; i++) {
                TestConsumer consumer = new TestConsumer();
                consumer.start();
                services.add(consumer);
            }
            for (int i = 0; i < PRODUCER_COUNT; i++) {
                TestProducer producer = new TestProducer(i);
                producer.start();
                services.add(producer);
            }
            waitForMessageReceipt();

        } finally {
            if (p != null) {
                p.close();
            }

            if (c != null) {
                c.close();
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    public void onMessage(Message message) {
        receivedMessages.release();
        if (count.incrementAndGet() % 100 == 0) {
            System.out.println("Received message " + count);
        }
        if (RECEIVER_THINK_TIME > 0) {
            try {
                Thread.currentThread().sleep(RECEIVER_THINK_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }

    /**
     * @throws InterruptedException
     * @throws TimeoutException
     * 
     */
    private void waitForMessageReceipt() throws InterruptedException, TimeoutException {
        try {
            while (count.get() < SEND_COUNT) {
                if (!receivedMessages.tryAcquire(HANG_THRESHOLD, TimeUnit.SECONDS)) {
                    if (count.get() == SEND_COUNT) break;
                    throw new TimeoutException("@count=" + count.get() + " Message not received for more than " + HANG_THRESHOLD + " seconds");
                }
            }
        } finally {
            running.set(false);
        }
    }

    private interface Service {
        public void start() throws Exception;

        public void close();
    }

    private class TestProducer implements Runnable, Service {
        Thread thread;
        BytesMessage message;
        int id;
        Connection connection;
        Session session;
        MessageProducer producer;

        TestProducer(int id) throws Exception {
            this.id = id;
            thread = new Thread(this, "TestProducer-" + id);
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            producer = session.createProducer(session.createQueue("AMQ2401Test"));

        }

        public void start() {
            thread.start();
        }

        public void run() {

            int count = SEND_COUNT / PRODUCER_COUNT;
            for (int i = 1; i <= count; i++) {
                try {

                    if (+i % 100 == 0) {
                        System.out.println(thread.currentThread().getName() + " Sending message " + i);
                    }
                    message = session.createBytesMessage();
                    message.writeBytes(new byte[1024]);
                    producer.setDeliveryMode(deliveryMode);
                    producer.send(message);
                } catch (JMSException jmse) {
                    jmse.printStackTrace();
                    break;
                }
            }
        }

        public void close() {
            try {
                connection.close();
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private class TestConsumer implements Runnable, Service {
        ActiveMQConnection connection;
        Session session;
        MessageConsumer consumer;
        Thread thread;

        TestConsumer() throws Exception {
            factory.setOptimizeAcknowledge(false);
            connection = (ActiveMQConnection) factory.createConnection();
            if (useOptimizeAcks) {
                connection.setOptimizeAcknowledge(true);
            }

            session = connection.createSession(false, ackMode);
            consumer = session.createConsumer(session.createQueue("AMQ2401Test"));

            consumer.setMessageListener(AMQ2413Test.this);
        }

        public void start() throws Exception {
            connection.start();
        }

        public void close() {
            try {
                connection.close();
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            while (running.get()) {
                try {
                    onMessage(consumer.receive());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

    }
    
    public static Test suite() {
       return suite(AMQ2413Test.class);
     }
}