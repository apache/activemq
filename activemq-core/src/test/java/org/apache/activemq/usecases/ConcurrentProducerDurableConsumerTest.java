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
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConcurrentProducerDurableConsumerTest extends TestSupport {
    private static final Log LOG = LogFactory.getLog(ConcurrentProducerDurableConsumerTest.class);
    private int consumerCount = 1;
    BrokerService broker;
    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());
    protected Map<MessageConsumer, MessageIdList> consumers = new HashMap<MessageConsumer, MessageIdList>();
    protected MessageIdList allMessagesList = new MessageIdList();
    private int messageSize = 1024;

    public void testPlaceHolder() throws Exception {
    }

    public void x_initCombosForTestSendRateWithActivatingConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC, PersistenceAdapterChoice.MEM});
    }

    public void x_testSendRateWithActivatingConsumers() throws Exception {
        final Destination destination = createDestination();
        final ConnectionFactory factory = createConnectionFactory();
        startInactiveConsumers(factory, destination);

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = createMessageProducer(session, destination);

        // preload the durable consumers
        double[] inactiveConsumerStats = produceMessages(destination, 200, 100, session, producer, null);
        LOG.info("With inactive consumers: ave: " + inactiveConsumerStats[1]
                + ", max: " + inactiveConsumerStats[0] + ", multiplier: " + (inactiveConsumerStats[0]/inactiveConsumerStats[1]));

        // periodically start a durable sub that is has a backlog
        final int consumersToActivate = 1;
        final Object addConsumerSignal = new Object();
        Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ActivateConsumer" + this);
            }
        }).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    MessageConsumer consumer = null;
                    for (int i = 0; i < consumersToActivate; i++) {
                        LOG.info("Waiting for add signal");
                        synchronized (addConsumerSignal) {
                            addConsumerSignal.wait(30 * 60 * 1000);
                        }
                        consumer = createDurableSubscriber(factory.createConnection(), destination, "consumer" + (i + 1));
                        LOG.info("Created consumer " + consumer);
                        MessageIdList list = new MessageIdList();
                        list.setParent(allMessagesList);
                        consumer.setMessageListener(list);
                        consumers.put(consumer, list);
                    }
                } catch (Exception e) {
                    LOG.error("failed to start consumer", e);
                }
            }
        });


        double[] stats  = produceMessages(destination, 20, 100, session, producer, addConsumerSignal);

        LOG.info(" with concurrent activate, ave: " + stats[1] + ", max: " + stats[0] + ", multiplier: " + (stats[0]/stats[1]));
        assertTrue("max (" + stats[0] + ") within reasonable multiplier of ave (" + stats[1] + ")",
                stats[0] < 5 * stats[1]);

    }


    public void x_initCombosForTestSendWithInactiveAndActiveConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC, PersistenceAdapterChoice.MEM});
    }

    public void x_testSendWithInactiveAndActiveConsumers() throws Exception {
        Destination destination = createDestination();
        ConnectionFactory factory = createConnectionFactory();
        startInactiveConsumers(factory, destination);

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        final int toSend = 100;
        final int numIterations = 5;

        double[] noConsumerStats = produceMessages(destination, toSend, numIterations, session, producer, null);

        startConsumers(factory, destination);
        LOG.info("Activated consumer");

        double[] withConsumerStats = produceMessages(destination, toSend, numIterations, session, producer, null);

        LOG.info("With consumer: " + withConsumerStats[1] + " , with noConsumer: " + noConsumerStats[1]
                + ", multiplier: " + (withConsumerStats[1]/noConsumerStats[1]));
        final int reasonableMultiplier = 4; // not so reasonable, but on slow disks it can be
        assertTrue("max X times as slow with consumer: " + withConsumerStats[1] + ", with no Consumer: "
                + noConsumerStats[1] + ", multiplier: " + (withConsumerStats[1]/noConsumerStats[1]),
                withConsumerStats[1] < noConsumerStats[1] * reasonableMultiplier);

        final int toReceive = toSend * numIterations * consumerCount * 2;
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("count: " + allMessagesList.getMessageCount());
                return toReceive == allMessagesList.getMessageCount();
            }
        }, 60 * 1000);

        assertEquals("got all messages", toReceive, allMessagesList.getMessageCount());
    }


    private MessageProducer createMessageProducer(Session session, Destination destination) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        return producer;
    }


    private void startInactiveConsumers(ConnectionFactory factory, Destination destination) throws Exception {
        // create off line consumers
        startConsumers(factory, destination);
        for (Connection connection: connections) {
            connection.close();
        }
        connections.clear();
        consumers.clear();
    }


    protected void startConsumers(ConnectionFactory factory, Destination dest) throws Exception {
        MessageConsumer consumer;
        for (int i = 0; i < consumerCount; i++) {
            consumer = createDurableSubscriber(factory.createConnection(), dest, "consumer" + (i + 1));
            MessageIdList list = new MessageIdList();
            list.setParent(allMessagesList);
            consumer.setMessageListener(list);
            consumers.put(consumer, list);
        }
    }

    protected TopicSubscriber createDurableSubscriber(Connection conn, Destination dest, String name) throws Exception {
        conn.setClientID(name);
        connections.add(conn);
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TopicSubscriber consumer = sess.createDurableSubscriber((javax.jms.Topic)dest, name);

        return consumer;
    }

    /**
     * @return max and ave send time
     * @throws Exception
     */
    private double[] produceMessages(Destination destination,
                                     int toSend,
                                     int numIterations,
                                     Session session,
                                     MessageProducer producer,
                                     Object addConsumerSignal) throws Exception {
        long start;
        long count = 0;
        double max = 0, sum = 0;
        for (int i=0; i<numIterations; i++) {
            start = System.currentTimeMillis();
            for (int j=0; j < toSend; j++) {
                TextMessage msg = createTextMessage(session, "" + j);
                producer.send(msg);
                if (++count % 300 == 0) {
                    if (addConsumerSignal != null) {
                        synchronized (addConsumerSignal) {
                            addConsumerSignal.notifyAll();
                            LOG.info("Signaled add consumer");
                        }
                    }
                }
                if (count % 5000 == 0) {
                    LOG.info("Sent " + count);
                }

            }
            long duration = System.currentTimeMillis() - start;
            max = Math.max(max, duration);
            sum += duration;
        }

        LOG.info("Sent: " + toSend * numIterations + ", max send time: " + max);
        return new double[]{max, sum/numIterations};
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        TextMessage msg = session.createTextMessage();

        // Pad message text
        if (initText.length() < messageSize) {
            char[] data = new char[messageSize - initText.length()];
            Arrays.fill(data, '*');
            String str = new String(data);
            msg.setText(initText + str);

            // Do not pad message text
        } else {
            msg.setText(initText);
        }

        return msg;
    }

    @Override
    protected void setUp() throws Exception {
        topic = true;
        super.setUp();
        broker = createBroker();
        broker.start();
    }

    @Override
    protected void tearDown() throws Exception {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection conn = iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
        broker.stop();
        allMessagesList.flushMessages();
        consumers.clear();
        super.tearDown();
    }


    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setEnableStatistics(false);
        brokerService.addConnector("tcp://0.0.0.0:61616");
        brokerService.setDeleteAllMessagesOnStartup(true);

        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(true);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        brokerService.setDestinationPolicy(policyMap);

        //setPersistenceAdapter(brokerService, PersistenceAdapterChoice.JDBC);
        setDefaultPersistenceAdapter(brokerService);
        return brokerService;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(1);
        factory.setPrefetchPolicy(prefetchPolicy);
        return factory;
    }

    public static Test suite() {
        return suite(ConcurrentProducerDurableConsumerTest.class);
    }

}
