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
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AMQ2314Test extends CombinationTestSupport {

    public boolean consumeAll = false;
    public int deliveryMode = DeliveryMode.NON_PERSISTENT;
    
    private static final Logger LOG = LoggerFactory.getLogger(AMQ2314Test.class);
    private static final int MESSAGES_COUNT = 30000;
    private static byte[]  buf = new byte[1024];
    private BrokerService broker;
    
    protected long messageReceiveTimeout = 500L;

    Destination destination = new ActiveMQTopic("FooTwo");
    
    public void testRemoveSlowSubscriberWhacksTempStore() throws Exception {
        runProducerWithHungConsumer();
    }
    
    public void testMemoryUsageReleasedOnAllConsumed() throws Exception {
        consumeAll = true;
        runProducerWithHungConsumer();
        // do it again to ensure memory limits are decreased
        runProducerWithHungConsumer();
    }
    
    
    public void runProducerWithHungConsumer() throws Exception {
    
        final CountDownLatch consumerContinue = new CountDownLatch(1);
        final CountDownLatch consumerReady = new CountDownLatch(1);
        
        final long origTempUsage = broker.getSystemUsage().getTempUsage().getUsage();
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        factory.setAlwaysSyncSend(true);
        
        // ensure messages are spooled to disk for this consumer
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setTopicPrefetch(500);
        factory.setPrefetchPolicy(prefetch);
        final Connection connection = factory.createConnection();
        connection.start();

        Thread producingThread = new Thread("Producing thread") {
            public void run() {
                try {
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(deliveryMode);
                    for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
                        Message message = session.createTextMessage(new String(buf) + idx);
                        producer.send(message);
                    }
                    producer.close();
                    session.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        
        Thread consumingThread = new Thread("Consuming thread") {
            public void run() {
                try {
                    int count = 0;
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    
                    while (consumer.receive(messageReceiveTimeout) == null) {
                        consumerReady.countDown();
                    }
                    count++;
                    LOG.info("Received one... waiting");  
                    consumerContinue.await();
                    if (consumeAll) {
                        LOG.info("Consuming the rest of the messages...");
                        while (consumer.receive(messageReceiveTimeout) != null) {
                            count++;
                        }
                    }
                    LOG.info("consumer session closing: consumed count: " + count);
                    session.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        consumingThread.start();
        consumerReady.await();
        
        producingThread.start();
        producingThread.join();
        
        final long tempUsageBySubscription = broker.getSystemUsage().getTempUsage().getUsage();
        LOG.info("Orig Usage: " + origTempUsage + ", currentUsage: " + tempUsageBySubscription);
        assertTrue("some temp store has been used", tempUsageBySubscription != origTempUsage);
        consumerContinue.countDown();
        consumingThread.join();
        connection.close();
       
        LOG.info("Subscrition Usage: " + tempUsageBySubscription + ", endUsage: "
                + broker.getSystemUsage().getTempUsage().getUsage());
        
        assertTrue("temp usage decreased with removed sub", Wait.waitFor(new Wait.Condition(){
            public boolean isSatisified() throws Exception {
                return broker.getSystemUsage().getTempUsage().getUsage()  < tempUsageBySubscription;
            }
        }));
    }
    
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        broker.addConnector("tcp://localhost:61616").setName("Default");
        broker.start();
    }
    
    public void tearDown() throws Exception {
        broker.stop();
    }
    
    
    public static Test suite() {
        return suite(AMQ2314Test.class);
    }

}
