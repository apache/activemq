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
package org.apache.activemq.network;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.DestinationDoesNotExistException;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.activemq.util.Wait;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class NetworkFailoverTest extends TestCase {

    protected static final int MESSAGE_COUNT = 10;
    private static final Logger LOG = LoggerFactory.getLogger(NetworkFailoverTest.class);

    protected AbstractApplicationContext context;
    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected final ActiveMQQueue included = new ActiveMQQueue("include.test.foo");

    // Track unique original message texts across outcomes to avoid double-counting
    // when duplicates are delivered due to failover and network bridge re-forwarding
    private final Set<String> completedMessages = ConcurrentHashMap.newKeySet();
    private final Set<String> nedMessages = ConcurrentHashMap.newKeySet();
    private final Set<String> dlqMessages = ConcurrentHashMap.newKeySet();

    public void testRequestReply() throws Exception {
        final MessageProducer remoteProducer = remoteSession.createProducer(null);
        final MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        remoteConsumer.setMessageListener(msg -> {
            final TextMessage textMsg = (TextMessage) msg;
            // Extract original text before try block so it's accessible in catch
            // (clearBody()/setText() inside try modifies the message body)
            String origText;
            try {
                origText = textMsg.getText();
            } catch (JMSException e) {
                LOG.warn("Failed to read original message text", e);
                origText = "unknown";
            }
            final String originalText = origText;
            try {
                final String payload = "REPLY: " + originalText + ", " + textMsg.getJMSMessageID();
                final Destination replyTo = msg.getJMSReplyTo();
                textMsg.clearBody();
                textMsg.setText(payload);
                LOG.info("*** Sending response: {}", textMsg.getText());
                remoteProducer.send(replyTo, textMsg);
                LOG.info("replied with: " + textMsg.getJMSMessageID());

            } catch (DestinationDoesNotExistException expected) {
                // Temp queue was removed during failover but not yet recreated.
                // Track the unique original message text so duplicates don't double-count.
                try {
                    nedMessages.add(originalText);
                    LOG.info("NED: " + textMsg.getJMSMessageID() + ", text: " + originalText);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                LOG.warn("*** Responder listener caught exception: ", e);
                e.printStackTrace();
            }
        });

        final Queue tempQueue = localSession.createTemporaryQueue();
        final MessageProducer requestProducer = localSession.createProducer(included);
        requestProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        final MessageConsumer requestConsumer = localSession.createConsumer(tempQueue);

        // track remote DLQ for forward failures
        final MessageConsumer remoteDlqConsumer = remoteSession.createConsumer(
                new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        remoteDlqConsumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    dlqMessages.add(((TextMessage) message).getText());
                }
                LOG.info("remote dlq " + message.getJMSMessageID());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        // Use a separate session for the local DLQ consumer since localSession
        // uses synchronous receive() and JMS does not allow mixing synchronous
        // receive with asynchronous MessageListener on the same session
        final Session localDlqSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer localDlqConsumer = localDlqSession.createConsumer(
                new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        localDlqConsumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    dlqMessages.add(((TextMessage) message).getText());
                }
                LOG.info("local dlq " + message.getJMSMessageID());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        // Wait for both network bridges to be fully started
        waitForBridgeFullyStarted(localBroker, "local");
        waitForBridgeFullyStarted(remoteBroker, "remote");

        // Wait for the remote consumer demand to propagate to the local broker
        // so messages sent to 'included' on local broker are forwarded to remote
        assertTrue("Network consumer for included queue should be registered on local broker",
                Wait.waitFor(() -> {
                    try {
                        return localBroker.getDestination(included)
                                .getConsumers().size() >= 1;
                    } catch (Exception e) {
                        return false;
                    }
                }, TimeUnit.SECONDS.toMillis(10), 100));

        final FailoverTransport failoverTransport = ((FailoverTransport) ((TransportFilter) ((TransportFilter)
                ((ActiveMQConnection) localConnection)
                        .getTransport()).getNext()).getNext());

        final long done = System.currentTimeMillis() + (MESSAGE_COUNT * 6000);
        int i = 0;
        while (uniqueAccountedCount() < MESSAGE_COUNT && done > System.currentTimeMillis()) {
            if (i < MESSAGE_COUNT) {
                final String payload = "test msg " + i;
                i++;
                final TextMessage msg = localSession.createTextMessage(payload);
                msg.setJMSReplyTo(tempQueue);
                requestProducer.send(msg);
                LOG.info("Sent: " + msg.getJMSMessageID() + ", Failing over");
                failoverTransport.handleTransportFailure(new IOException("Forcing failover from test"));

                // Wait for the failover transport to reconnect before attempting to receive
                assertTrue("Failover transport should reconnect",
                        Wait.waitFor(failoverTransport::isConnected,
                                TimeUnit.SECONDS.toMillis(10), 100));
            }
            final TextMessage result = (TextMessage) requestConsumer.receive(5000);
            if (result != null) {
                LOG.info("Got reply: " + result.getJMSMessageID() + ", " + result.getText());
                // Extract original message text from reply ("REPLY: test msg N, ID:...")
                final String replyText = result.getText();
                final int startIdx = replyText.indexOf("test msg");
                if (startIdx >= 0) {
                    final int endIdx = replyText.indexOf(',', startIdx);
                    completedMessages.add(replyText.substring(startIdx, endIdx > startIdx ? endIdx : replyText.length()));
                }
            }
        }

        // Wait for async processing (DLQ delivery, NED processing) to complete
        assertTrue("All " + MESSAGE_COUNT + " unique messages should be accounted for",
                Wait.waitFor(() -> {
                    final int total = uniqueAccountedCount();
                    LOG.info("Waiting: completed=" + completedMessages.size()
                            + ", NED=" + nedMessages.size()
                            + ", DLQ=" + dlqMessages.size()
                            + ", uniqueTotal=" + total);
                    return total >= MESSAGE_COUNT;
                }, TimeUnit.SECONDS.toMillis(30), 500));

        LOG.info("Final: completed=" + completedMessages.size()
                + ", NED=" + nedMessages.size()
                + ", DLQ=" + dlqMessages.size()
                + ", uniqueTotal=" + uniqueAccountedCount());
        assertTrue("All " + MESSAGE_COUNT + " unique messages should be accounted for "
                + "(completed=" + completedMessages.size()
                + ", NED=" + nedMessages.size()
                + ", DLQ=" + dlqMessages.size() + ")",
                uniqueAccountedCount() >= MESSAGE_COUNT);
    }

    /**
     * Returns the number of unique messages accounted for across all outcomes.
     * A message is counted once even if it was delivered multiple times due to
     * failover (as round-trip completed, NED, or DLQ).
     */
    private int uniqueAccountedCount() {
        final Set<String> all = ConcurrentHashMap.newKeySet();
        all.addAll(completedMessages);
        all.addAll(nedMessages);
        all.addAll(dlqMessages);
        return all.size();
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        doSetUp(true);
    }

    @Override
    protected void tearDown() throws Exception {
        doTearDown();
        super.tearDown();
    }

    protected void doTearDown() throws Exception {
        try {
            localConnection.close();
            remoteConnection.close();
        } catch (Exception ex) {}

        try {
            localBroker.stop();
        } catch (Exception ex) {}
        try {
            remoteBroker.stop();
        } catch (Exception ex) {}
    }

    protected void doSetUp(final boolean deleteAllMessages) throws Exception {

        remoteBroker = createRemoteBroker();
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        remoteBroker.setCacheTempDestinations(true);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        localBroker = createLocalBroker();
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.setCacheTempDestinations(true);
        localBroker.start();
        localBroker.waitUntilStarted();

        // Get actual assigned ephemeral ports
        final URI localConnectURI = localBroker.getTransportConnectors().get(0).getConnectUri();
        final URI remoteConnectURI = remoteBroker.getTransportConnectors().get(0).getConnectUri();
        final String localURI = localConnectURI.toString();
        final String remoteURI = remoteConnectURI.toString();

        // Add network connectors programmatically using actual ports
        final DiscoveryNetworkConnector localToRemote = new DiscoveryNetworkConnector(
                new URI("static://(" + remoteURI + ")"));
        localToRemote.setName("networkConnector");
        localToRemote.setDynamicOnly(false);
        localToRemote.setConduitSubscriptions(true);
        localToRemote.setDecreaseNetworkConsumerPriority(false);
        localToRemote.setDynamicallyIncludedDestinations(
                java.util.List.of(new ActiveMQQueue("include.test.foo"), new ActiveMQTopic("include.test.bar")));
        localToRemote.setExcludedDestinations(
                java.util.List.of(new ActiveMQQueue("exclude.test.foo"), new ActiveMQTopic("exclude.test.bar")));
        localBroker.addNetworkConnector(localToRemote);
        localBroker.startNetworkConnector(localToRemote, null);

        final DiscoveryNetworkConnector remoteToLocal = new DiscoveryNetworkConnector(
                new URI("static://(" + localURI + ")"));
        remoteToLocal.setName("networkConnector");
        remoteBroker.addNetworkConnector(remoteToLocal);
        remoteBroker.startNetworkConnector(remoteToLocal, null);

        final ActiveMQConnectionFactory localFac = new ActiveMQConnectionFactory(
                "failover:(" + localURI + "," + remoteURI + ")?randomize=false&backup=false&trackMessages=true");
        localConnection = localFac.createConnection();
        localConnection.setClientID("local");
        localConnection.start();

        // The remote connection does not need trackMessages since we only force failover
        // on the local connection. Disable duplicate checking so that messages re-forwarded
        // by the network bridge (due to forced failovers) are delivered to the remote consumer
        // listener instead of being silently poison-acked as duplicates.
        final ActiveMQConnectionFactory remoteFac = new ActiveMQConnectionFactory(
                "failover:(" + remoteURI + "," + localURI + ")?randomize=false&backup=false");
        remoteFac.setWatchTopicAdvisories(false);
        remoteFac.setCheckForDuplicates(false);
        remoteConnection = remoteFac.createConnection();
        remoteConnection.setClientID("remote");
        remoteConnection.start();

        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker-ephemeral.xml";
    }

    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker-ephemeral.xml";
    }

    protected void waitForBridgeFullyStarted(final BrokerService broker, final String label) throws Exception {
        assertTrue(label + " broker bridge should be fully started", Wait.waitFor(() -> {
            if (broker.getNetworkConnectors().isEmpty()
                    || broker.getNetworkConnectors().get(0).activeBridges().isEmpty()) {
                return false;
            }
            final NetworkBridge bridge = broker.getNetworkConnectors().get(0).activeBridges().iterator().next();
            if (bridge instanceof DemandForwardingBridgeSupport) {
                return ((DemandForwardingBridgeSupport) bridge).startedLatch.getCount() == 0;
            }
            return true;
        }, TimeUnit.SECONDS.toMillis(10), 100));
    }

    protected BrokerService createBroker(final String uri) throws Exception {
        final Resource resource = new ClassPathResource(uri);
        final BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        return factory.getBroker();
    }

    protected BrokerService createLocalBroker() throws Exception {
        return createBroker(getLocalBrokerURI());
    }

    protected BrokerService createRemoteBroker() throws Exception {
        return createBroker(getRemoteBrokerURI());
    }
}
