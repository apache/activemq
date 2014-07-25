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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class ThreeBrokerTempDestDemandSubscriptionCleanupTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ThreeBrokerTempDestDemandSubscriptionCleanupTest.class);

    boolean enableTempDestinationBridging = true;

    private static final String BROKER_A = "BrokerA";
    private static final String BROKER_B = "BrokerB";
    private static final String BROKER_C = "BrokerC";

    private static final String ECHO_QUEUE_NAME = "echo";
    private static final int NUM_ITER = 100;
    private static final long CONSUME_TIMEOUT = 500;


    /**
     * So we network three brokers together, and send a message with request-reply semantics.
     * The message goes to an echo service listening on broker C. We send a message on a queue
     * to broker A which gets demand forwarded to broker C. the echo service will respond to the
     * temp destination listed in the JMSReplyTo header. that will get demand forwarded back to
     * broker A. When the consumer of the temp dest on broker A closes, that subscription should
     * be removed on broker A. advisories firing from broker A to broker B should remove that
     * subscription on broker B. advisories firing from broker B to broker C should remove that
     * subscription on broker C.
     *
     * @throws Exception
     */
    public void testSubscriptionsCleanedUpRace() throws Exception {

        final BrokerItem brokerA = brokers.get(BROKER_A);


        Runnable tester = new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < NUM_ITER; i++) {

                    Connection conn = null;
                    try {
                        conn = brokerA.createConnection();

                        conn.start();

                        final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Destination destination = sess.createQueue(ECHO_QUEUE_NAME);

                        MessageProducer producer = sess.createProducer(destination);

                        LOG.info("Starting iter: " + i);
                        Destination replyTo = sess.createTemporaryQueue();
                        MessageConsumer responseConsumer = sess.createConsumer(replyTo);

                        Message message = sess.createTextMessage("Iteration: " + i);
                        message.setJMSReplyTo(replyTo);

                        producer.send(message);

                        TextMessage response = (TextMessage)responseConsumer.receive(CONSUME_TIMEOUT);
                        assertNotNull("We should have gotten a response, but didn't for iter: " + i, response);
                        assertEquals("We got the wrong response from the echo service", "Iteration: " + i, response.getText());


                        // so we close the consumer so that an actual RemoveInfo command gets propogated through the
                        // network
                        responseConsumer.close();
                        conn.close();

                    } catch (Exception e) {
                        e.printStackTrace();
                        fail();
                    }

                }
            }
        };

        ExecutorService threadService = Executors.newFixedThreadPool(2);
        threadService.submit(tester);
        threadService.submit(tester);

        threadService.shutdown();
        assertTrue("executor done on time", threadService.awaitTermination(30l, TimeUnit.SECONDS));

        // for the real test... we should not have any subscriptions left on broker C for the temp dests
        BrokerItem brokerC = brokers.get(BROKER_C);
        RegionBroker regionBroker = (RegionBroker) brokerC.broker.getRegionBroker();
        final AbstractRegion region = (AbstractRegion) regionBroker.getTempQueueRegion();

        assertTrue("There were no lingering temp-queue destinations", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Lingering temps: " + region.getSubscriptions().size());
                return 0 == region.getSubscriptions().size();
            }
        }));
    }


    /**
     * This test is slightly different from the above. We don't explicitly close the consumer down
     * (which we did in the previous test to force the RemoveInfo to be sent). Here we just close
     * the connection which should still clean up the subscriptions and temp destinations on the
     * networked brokers.
     *
     * @throws Exception
     */
    public void testSubscriptionsCleanedUpAfterConnectionClose() throws Exception {

        final BrokerItem brokerA = brokers.get(BROKER_A);

        for (int i = 0; i < NUM_ITER; i++) {

            Connection conn = null;
            try {
                conn = brokerA.createConnection();

                conn.start();

                final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = sess.createQueue(ECHO_QUEUE_NAME);

                MessageProducer producer = sess.createProducer(destination);

                LOG.info("Starting iter: " + i);
                Destination replyTo = sess.createTemporaryQueue();
                MessageConsumer responseConsumer = sess.createConsumer(replyTo);

                Message message = sess.createTextMessage("Iteration: " + i);
                message.setJMSReplyTo(replyTo);

                producer.send(message);

                TextMessage response = (TextMessage)responseConsumer.receive(CONSUME_TIMEOUT);
                assertNotNull("We should have gotten a response, but didn't for iter: " + i, response);
                assertEquals("We got the wrong response from the echo service", "Iteration: " + i, response.getText());


                // so closing the connection without closing the consumer first will leak subscriptions
                // in a nob?
//              responseConsumer.close();
                conn.close();

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }

        }

        // for the real test... we should not have any subscriptions left on broker C for the temp dests
        BrokerItem brokerC = brokers.get(BROKER_C);
        RegionBroker regionBroker = (RegionBroker) brokerC.broker.getRegionBroker();
        final AbstractRegion region = (AbstractRegion) regionBroker.getTempQueueRegion();

        assertTrue("There were no lingering temp-queue destinations", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Lingering temps: " + region.getSubscriptions().size());
                return 0 == region.getSubscriptions().size();
            }
        }));

    }

    private void installEchoClientOnBrokerC() throws Exception {
        BrokerItem brokerC = brokers.get(BROKER_C);
        Connection conn = brokerC.createConnection();
        conn.start();

        final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = sess.createQueue(ECHO_QUEUE_NAME);
        MessageConsumer consumer = sess.createConsumer(destination);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {

                TextMessage textMessage = (TextMessage) message;

                try {
                    Destination replyTo = message.getJMSReplyTo();

                    MessageProducer producer = sess.createProducer(replyTo);
                    Message response = sess.createTextMessage(textMessage.getText());

                    LOG.info("Replying to this request: "  + textMessage.getText());
                    producer.send(response);
                    producer.close();

                } catch (JMSException e) {
                    e.printStackTrace();
                    fail("Could not respond to an echo request");
                }
            }
        });
    }


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/" + BROKER_A + "?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/" + BROKER_B + "?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/" + BROKER_C + "?persistent=false&useJmx=false"));

        bridgeBrokers("BrokerA", "BrokerB", false, 3);
        bridgeBrokers("BrokerB", "BrokerC", false, 3);

        startAllBrokers();

        // set up a listener on broker C that will demand forward subscriptions over the network
        installEchoClientOnBrokerC();
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly, int networkTTL) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName, dynamicOnly, networkTTL, true);
        connector.setBridgeTempDestinations(enableTempDestinationBridging);
        connector.setDuplex(true);
        return connector;
    }
}