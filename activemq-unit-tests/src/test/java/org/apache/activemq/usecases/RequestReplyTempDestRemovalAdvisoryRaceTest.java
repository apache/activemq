/**
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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.PriorityDispatchPolicy;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.network.DemandForwardingBridgeSupport;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class RequestReplyTempDestRemovalAdvisoryRaceTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(RequestReplyTempDestRemovalAdvisoryRaceTest.class);

    private static final String BROKER_A = "BrokerA";
    private static final String BROKER_B = "BrokerB";
    private static final String BROKER_C = "BrokerC";

    private static final int NUM_RESPONDENTS = 1;
    private static final int NUM_SENDS = 1;
    private static final int RANDOM_SLEEP_FOR_RESPONDENT_MS = 0;
    private static final int RANDOM_SLEEP_FOR_SENDER_MS = 1;
    private static final String QUEUE_NAME = "foo.queue";
    private static String[] TEST_ITERATIONS = new String[]{QUEUE_NAME+"0", QUEUE_NAME+"1", QUEUE_NAME+"2", QUEUE_NAME+"3"};

    final AtomicLong messageCount = new AtomicLong(0);
    final AtomicLong respondentSendError = new AtomicLong(0);
    final AtomicLong responseReceived = new AtomicLong(0);
    final AtomicLong sendsWithNoConsumers = new AtomicLong(0);
    final AtomicLong forwardFailures = new AtomicLong(0);


    protected final AtomicBoolean shutdown = new AtomicBoolean(false);
    HashSet<NetworkConnector> networkConnectors = new HashSet<NetworkConnector>();
    HashSet<Connection> advisoryConsumerConnections = new HashSet<Connection>();
    Appender slowDownAppender;

    CountDownLatch consumerDemandExists;

    protected boolean useDuplex = false;

    public static Test suite() {
        return suite(RequestReplyTempDestRemovalAdvisoryRaceTest.class);
    }

    /**
     * Notes: to reliably reproduce use debugger... set a "thread" breakpoint at line 679 in DemandForwardingBridgeSupport,
     * and only break on the "2nd" pass (broker C's bridge). Allow debugging to continue shortly after hitting
     * the breakpoint, for this test we use a logging appender to implement the pause,
     * it fails most of the time, hence the combos
     */
    public void initCombos() {
        addCombinationValues("QUEUE_NAME", TEST_ITERATIONS);
    }

    public void testTempDestRaceDuplex() throws Exception {
        // duplex
        useDuplex = true;
        bridgeBrokers(BROKER_A, BROKER_B, false, 3);
        bridgeBrokers(BROKER_B, BROKER_C, false, 3);

        startAllBrokers();

        waitForBridgeFormation(1);

        HashSet<NetworkBridge> bridgesStart = new HashSet<NetworkBridge>();
        for (NetworkConnector networkConnector : networkConnectors) {
            bridgesStart.addAll(networkConnector.activeBridges());
        }
        LOG.info("Bridges start:" + bridgesStart);

        slowDownAdvisoryDispatch();
        noConsumerAdvisory();
        forwardFailureAdvisory();

        // set up respondents
        ExecutorService respondentThreadPool = Executors.newFixedThreadPool(50);
        BrokerItem brokerA = brokers.get(BROKER_A);
        ActiveMQConnectionFactory brokerAFactory = new ActiveMQConnectionFactory(brokerA.broker.getTransportConnectorByScheme("tcp").getName()
                + "?jms.watchTopicAdvisories=false");
        brokerAFactory.setAlwaysSyncSend(true);
        for (int i = 0; i < NUM_RESPONDENTS; i++) {
            respondentThreadPool.execute(new EchoRespondent(brokerAFactory));
        }

        // fire off sends
        ExecutorService senderThreadPool = Executors.newCachedThreadPool();
        BrokerItem brokerC = brokers.get(BROKER_C);
        ActiveMQConnectionFactory brokerCFactory = new ActiveMQConnectionFactory(brokerC.broker.getTransportConnectorByScheme("tcp").getName()
                + "?jms.watchTopicAdvisories=false");
        for (int i = 0; i < NUM_SENDS; i++) {
            senderThreadPool.execute(new MessageSender(brokerCFactory));
        }

        senderThreadPool.shutdown();
        senderThreadPool.awaitTermination(30, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(15);
        LOG.info("shutting down");
        shutdown.compareAndSet(false, true);

        HashSet<NetworkBridge> bridgesEnd = new HashSet<NetworkBridge>();
        for (NetworkConnector networkConnector : networkConnectors) {
            bridgesEnd.addAll(networkConnector.activeBridges());
        }
        LOG.info("Bridges end:" + bridgesEnd);

        assertEquals("no new bridges created", bridgesStart, bridgesEnd);

        // validate success or error of dlq
        LOG.info("received: " + responseReceived.get() + ", respondent error: " + respondentSendError.get()
                + ", noConsumerCount: " + sendsWithNoConsumers.get()
                + ", forwardFailures: " + forwardFailures.get());
        assertEquals("success or error", NUM_SENDS, respondentSendError.get() + forwardFailures.get()
                + responseReceived.get() + sendsWithNoConsumers.get());

    }

    private void forwardFailureAdvisory() throws JMSException {
        for (BrokerItem item : brokers.values()) {
            ActiveMQConnectionFactory brokerAFactory = new ActiveMQConnectionFactory(item.broker.getTransportConnectorByScheme("tcp").getName()
                    + "?jms.watchTopicAdvisories=false");
            Connection connection = brokerAFactory.createConnection();
            connection.start();
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(
                    AdvisorySupport.getNetworkBridgeForwardFailureAdvisoryTopic()).setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    forwardFailures.incrementAndGet();
                }
            });
        }
    }

    private void noConsumerAdvisory() throws JMSException {
        for (BrokerItem item : brokers.values()) {
            ActiveMQConnectionFactory brokerAFactory = new ActiveMQConnectionFactory(item.broker.getTransportConnectorByScheme("tcp").getName()
                    + "?jms.watchTopicAdvisories=false");
            Connection connection = brokerAFactory.createConnection();
            connection.start();
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(
                    AdvisorySupport.getNoTopicConsumersAdvisoryTopic(new ActiveMQTempTopic(">"))).setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    sendsWithNoConsumers.incrementAndGet();
                }
            });
        }
    }


    public void testTempDestRace() throws Exception {
        // non duplex
        bridgeBrokers(BROKER_A, BROKER_B, false, 3);
        bridgeBrokers(BROKER_B, BROKER_A, false, 3);
        bridgeBrokers(BROKER_B, BROKER_C, false, 3);
        bridgeBrokers(BROKER_C, BROKER_B, false, 3);

        startAllBrokers();

        waitForBridgeFormation(1);

        HashSet<NetworkBridge> bridgesStart = new HashSet<NetworkBridge>();
        for (NetworkConnector networkConnector : networkConnectors) {
            bridgesStart.addAll(networkConnector.activeBridges());
        }

        slowDownAdvisoryDispatch();
        noConsumerAdvisory();
        forwardFailureAdvisory();


        // set up respondents
        ExecutorService respondentThreadPool = Executors.newFixedThreadPool(50);
        BrokerItem brokerA = brokers.get(BROKER_A);
        ActiveMQConnectionFactory brokerAFactory = new ActiveMQConnectionFactory(brokerA.broker.getTransportConnectorByScheme("tcp").getName()
                + "?jms.watchTopicAdvisories=false");
        brokerAFactory.setAlwaysSyncSend(true);
        for (int i = 0; i < NUM_RESPONDENTS; i++) {
            respondentThreadPool.execute(new EchoRespondent(brokerAFactory));
        }

        // fire off sends
        ExecutorService senderThreadPool = Executors.newCachedThreadPool();
        BrokerItem brokerC = brokers.get(BROKER_C);
        ActiveMQConnectionFactory brokerCFactory = new ActiveMQConnectionFactory(brokerC.broker.getTransportConnectorByScheme("tcp").getName()
                + "?jms.watchTopicAdvisories=false");
        for (int i = 0; i < NUM_SENDS; i++) {
            senderThreadPool.execute(new MessageSender(brokerCFactory));
        }

        senderThreadPool.shutdown();
        senderThreadPool.awaitTermination(30, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(10);
        LOG.info("shutting down");
        shutdown.compareAndSet(false, true);

        HashSet<NetworkBridge> bridgesEnd = new HashSet<NetworkBridge>();
        for (NetworkConnector networkConnector : networkConnectors) {
            bridgesEnd.addAll(networkConnector.activeBridges());
        }
        assertEquals("no new bridges created", bridgesStart, bridgesEnd);

        // validate success or error or dlq
        LOG.info("received: " + responseReceived.get() + ", respondent error: " + respondentSendError.get()
                + ", noConsumerCount: " + sendsWithNoConsumers.get()
                + ", forwardFailures: " + forwardFailures.get());
        assertEquals("success or error", NUM_SENDS, respondentSendError.get() + forwardFailures.get()
                + responseReceived.get() + sendsWithNoConsumers.get());

    }

    private void slowDownAdvisoryDispatch() throws Exception {

        org.apache.log4j.Logger.getLogger(DemandForwardingBridgeSupport.class).setLevel(Level.DEBUG);

        // instrument a logger to block the processing of a remove sub advisory
        // simulate a slow thread
        slowDownAppender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent loggingEvent) {
                if (Level.DEBUG.equals(loggingEvent.getLevel())) {
                    String message = loggingEvent.getMessage().toString();
                    if (message.startsWith("BrokerB") && message.contains("remove local subscription")) {
                        // sleep for a bit
                        try {
                            consumerDemandExists.countDown();
                            System.err.println("Sleeping on receipt of remove info debug message: " + message);
                            TimeUnit.SECONDS.sleep(2);
                        } catch (Exception ignored) {
                        }
                    }

                }
            }
        };

        org.apache.log4j.Logger.getRootLogger().addAppender(slowDownAppender);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        responseReceived.set(0);
        respondentSendError.set(0);
        forwardFailures.set(0);
        sendsWithNoConsumers.set(0);
        networkConnectors.clear();
        advisoryConsumerConnections.clear();
        consumerDemandExists = new CountDownLatch(1);
        createBroker(new URI("broker:(tcp://localhost:0)/" + BROKER_A + "?persistent=false&useJmx=false")).setDedicatedTaskRunner(false);
        createBroker(new URI("broker:(tcp://localhost:0)/" + BROKER_B + "?persistent=false&useJmx=false")).setDedicatedTaskRunner(false);
        createBroker(new URI("broker:(tcp://localhost:0)/" + BROKER_C + "?persistent=false&useJmx=false")).setDedicatedTaskRunner(false);

        PolicyMap map = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setSendAdvisoryIfNoConsumers(true);
        DeadLetterStrategy deadletterStrategy = new SharedDeadLetterStrategy();
        deadletterStrategy.setProcessNonPersistent(true);
        defaultEntry.setDeadLetterStrategy(deadletterStrategy);
        defaultEntry.setDispatchPolicy(new PriorityDispatchPolicy());
        map.put(new ActiveMQTempTopic(">"), defaultEntry);

        for (BrokerItem item : brokers.values()) {
            item.broker.setDestinationPolicy(map);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        if (slowDownAppender != null) {
            org.apache.log4j.Logger.getRootLogger().removeAppender(slowDownAppender);
        }
        for (Connection connection : advisoryConsumerConnections) {
            connection.close();
        }
        super.tearDown();
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly, int networkTTL) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName, dynamicOnly, networkTTL, true);
        connector.setBridgeTempDestinations(true);
        connector.setAdvisoryForFailedForward(true);
        connector.setDuplex(useDuplex);
        connector.setAlwaysSyncSend(true);
        networkConnectors.add(connector);
        return connector;
    }

    abstract class MessageClient {
        protected Connection connection;
        protected Session session;
        protected MessageConsumer consumer;
        protected MessageProducer producer;
        protected Random random;
        protected int timeToSleep;

        // set up the connection and session
        public MessageClient(ActiveMQConnectionFactory factory, int timeToSleep) throws Exception {
            this.connection = factory.createConnection();
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.timeToSleep = timeToSleep;
            this.random = new Random(System.currentTimeMillis());
            preInit();
            initProducer();
            initConsumer();
            this.connection.start();
        }

        protected void preInit() throws JMSException {

        }

        abstract protected void initProducer() throws JMSException;

        abstract protected void initConsumer() throws JMSException;
    }

    class MessageSender extends MessageClient implements Runnable {


        protected Destination tempDest;

        public MessageSender(ActiveMQConnectionFactory factory) throws Exception {
            super(factory, RANDOM_SLEEP_FOR_SENDER_MS);
        }

        @Override
        public void run() {
            // create a message
            try {
                TextMessage message = session.createTextMessage("request: message #" + messageCount.getAndIncrement());
                message.setJMSReplyTo(tempDest);
                producer.send(message);
                LOG.info("SENDER: Message [" + message.getText() + "] has been sent.");

                Message incomingMessage = consumer.receive(timeToSleep);
                if (incomingMessage instanceof TextMessage) {
                    try {
                        LOG.info("SENDER: Got a response from echo service!" + ((TextMessage) incomingMessage).getText());
                        responseReceived.incrementAndGet();
                    } catch (JMSException e) {
                        LOG.error("SENDER: might want to see why i'm getting non-text messages..." + incomingMessage, e);
                    }
                } else {
                    LOG.info("SENDER: Did not get a response this time");
                }


            } catch (JMSException e) {
                LOG.error("SENDER: Could not complete message sending properly: " + e.getMessage());
            } finally {
                try {
                    producer.close();
                    consumer.close();
                    session.close();
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void preInit() throws JMSException {
            this.tempDest = session.createTemporaryTopic();

        }

        @Override
        protected void initProducer() throws JMSException {
            this.producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME));
        }

        @Override
        protected void initConsumer() throws JMSException {
            this.consumer = session.createConsumer(tempDest);
            LOG.info("consumer for: " + tempDest + ", " + consumer);

        }

    }

    class EchoRespondent extends MessageClient implements Runnable {

        public EchoRespondent(ActiveMQConnectionFactory factory) throws Exception {
            super(factory, RANDOM_SLEEP_FOR_RESPONDENT_MS);
        }

        @Override
        public void run() {
            try {
                LOG.info("RESPONDENT LISTENING");
                while (!shutdown.get()) {
                    Message incomingMessage = consumer.receive(1000);
                    if (incomingMessage instanceof TextMessage) {
                        ActiveMQTextMessage textMessage = (ActiveMQTextMessage) incomingMessage;
                        try {
                            LOG.info("RESPONDENT: Received a message: [" + textMessage.getText() + "]" + Arrays.asList(textMessage.getBrokerPath()));
                            Message message = session.createTextMessage("reply: " + textMessage.getText());
                            Destination replyTo = incomingMessage.getJMSReplyTo();
                            TimeUnit.MILLISECONDS.sleep(timeToSleep);
                            consumerDemandExists.await(5, TimeUnit.SECONDS);
                            try {
                                producer.send(replyTo, message);
                                LOG.info("RESPONDENT: sent reply:" + message.getJMSMessageID() + " back to: " + replyTo);
                            } catch (JMSException e) {
                                LOG.error("RESPONDENT: could not send reply message: " + e.getLocalizedMessage(), e);
                                respondentSendError.incrementAndGet();
                            }
                        } catch (JMSException e) {
                            LOG.error("RESPONDENT: could not create the reply message: " + e.getLocalizedMessage(), e);
                        } catch (InterruptedException e) {
                            LOG.info("RESPONDENT could not generate a random number");
                        }
                    }
                }
            } catch (JMSException e) {
                LOG.info("RESPONDENT: Could not set the message listener on the respondent");
            }
        }

        @Override
        protected void initProducer() throws JMSException {
            this.producer = session.createProducer(null);
            // so that we can get an advisory on sending with no consumers
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        @Override
        protected void initConsumer() throws JMSException {
            this.consumer = session.createConsumer(new ActiveMQQueue(QUEUE_NAME));
        }
    }
}