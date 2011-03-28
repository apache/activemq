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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2584Test extends org.apache.activemq.TestSupport {

    static final Logger LOG = LoggerFactory.getLogger(AMQ2584Test.class);
    BrokerService broker = null;
    ActiveMQTopic topic;

    ActiveMQConnection consumerConnection = null, producerConnection = null;
    Session producerSession;
    MessageProducer producer;
    final int minPercentUsageForStore = 10;
    String data;

    public static Test suite() {
        return suite(AMQ2584Test.class);
    }

    public void initCombosForTestSize() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{
                        PersistenceAdapterChoice.AMQ,
                        PersistenceAdapterChoice.KahaDB
                });
    }

    public void testSize() throws Exception {
        CountDownLatch redeliveryConsumerLatch = new CountDownLatch(15000 -1);
        openConsumer(redeliveryConsumerLatch);

        assertEquals(0, broker.getAdminView().getStorePercentUsage());

        for (int i = 0; i < 5000; i++) {
            sendMessage(false);
        }

        final BrokerView brokerView = broker.getAdminView();

        broker.getSystemUsage().getStoreUsage().isFull();
        LOG.info("store percent usage: "+brokerView.getStorePercentUsage());
        assertTrue("some store in use", broker.getAdminView().getStorePercentUsage() > minPercentUsageForStore);

        assertTrue("redelivery consumer got all it needs", redeliveryConsumerLatch.await(60, TimeUnit.SECONDS));
        closeConsumer();

        // consume from DLQ
        final CountDownLatch received = new CountDownLatch(5000 -1);
        consumerConnection = (ActiveMQConnection) createConnection();
        Session dlqSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer dlqConsumer = dlqSession.createConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));
        dlqConsumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                if (received.getCount() % 500 == 0) {
                    LOG.info("remaining on DLQ: " + received.getCount());
                }
                received.countDown();
            }
        });
        consumerConnection.start();

        assertTrue("Not all messages reached the DLQ", received.await(60, TimeUnit.SECONDS));

        assertTrue("Store usage exceeds expected usage",
                Wait.waitFor(new Wait.Condition() {
                    public boolean isSatisified() throws Exception {
                        broker.getSystemUsage().getStoreUsage().isFull();
                        LOG.info("store precent usage: "+brokerView.getStorePercentUsage());
                        return broker.getAdminView().getStorePercentUsage() < minPercentUsageForStore;
                    }
                }));

         closeConsumer();

    }

    private void openConsumer(final CountDownLatch latch) throws Exception {
        consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.setClientID("cliID");
        consumerConnection.start();
        final Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
                try {
                    session.recover();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }

            }
        };

        session.createDurableSubscriber(topic, "subName1").setMessageListener(listener);
        session.createDurableSubscriber(topic, "subName2").setMessageListener(listener);
        session.createDurableSubscriber(topic, "subName3").setMessageListener(listener);
    }

    private void closeConsumer() throws JMSException {
        if (consumerConnection != null)
            consumerConnection.close();
        consumerConnection = null;
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

        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        setDefaultPersistenceAdapter(broker);
        configurePersistenceAdapter(broker.getPersistenceAdapter());
        broker.getSystemUsage().getStoreUsage().setLimit(200 * 1000 * 1000);
        broker.start();
    }

    private void configurePersistenceAdapter(PersistenceAdapter persistenceAdapter) {
        Properties properties = new Properties();
        String maxFileLengthVal = String.valueOf(1 * 1024 * 1024);
        properties.put("journalMaxFileLength", maxFileLengthVal);
        properties.put("maxFileLength", maxFileLengthVal);
        properties.put("cleanupInterval", "2000");
        properties.put("checkpointInterval", "2000");

        IntrospectionSupport.setProperties(persistenceAdapter, properties);
    }

    private void stopBroker() throws Exception {
        if (broker != null)
            broker.stop();
        broker = null;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://testStoreSize?jms.watchTopicAdvisories=false&jms.redeliveryPolicy.maximumRedeliveries=0&waitForStart=5000&create=false");
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
