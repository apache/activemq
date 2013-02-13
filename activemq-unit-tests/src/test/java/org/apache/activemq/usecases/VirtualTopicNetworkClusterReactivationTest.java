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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;

import javax.jms.*;
import java.net.URI;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class VirtualTopicNetworkClusterReactivationTest extends JmsMultipleBrokersTestSupport {

    private static final String BROKER_A = "brokerA";
    private static final String BROKER_B = "brokerB";
    private static final String BROKER_A_TRANSPORT_URL = "tcp://localhost:61616";
    private static final String BROKER_B_TRANSPORT_URL = "tcp://localhost:61617";
    private static final long DEFAULT_SLEEP_MS = 1000;

    private ActiveMQTopic topic = new ActiveMQTopic("VirtualTopic.FOO.TEST");
    private ActiveMQQueue queue = new ActiveMQQueue("Consumer.FOO.VirtualTopic.FOO.TEST");


    /**
     * This test shows how to use pub/sub to mimic durable subscribers in a network of brokers.
     *
     * When using durable subscribers in a broker cluster, you can encounter a situation where a
     * subscription gets orphaned on a broker when the client disconnects and reattaches to another
     * broker in the cluster. Since the clientID/durableName only need to be unique within a single
     * broker, it's possible to have a durable sub on multiple brokers in a cluster.
     *
     * FOR EXAMPLE:
     * Broker A and B are networked together in both directions to form a full mesh. If durable
     * subscriber 'foo' subscribes to failover(A,B) and ends up on B, and a producer on A, messages
     * will be demand forwarded from A to B. But if the durable sub 'foo' disconnects from B,
     * then reconnects to failover(A,B) but this time gets connected to A, the subscription on
     * B will still be there are continue to receive messages (and possibly have missed messages
     * sent there while gone)
     *
     * We can avoid all of this mess with virtual topics as seen below:
     *
     *
     * @throws JMSException
     */
    public void testDurableSubReconnectFromAtoB() throws JMSException {
        // create consumer on broker B
        ActiveMQConnectionFactory bConnFactory = new ActiveMQConnectionFactory(BROKER_B_TRANSPORT_URL+ "?jms.prefetchPolicy.queuePrefetch=0");
        Connection bConn = bConnFactory.createConnection();
        bConn.start();
        Session bSession = bConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer bSessionConsumer = bSession.createConsumer(queue);


        // create producer on A
        ActiveMQConnectionFactory aConnFactory = new ActiveMQConnectionFactory(BROKER_A_TRANSPORT_URL);
        Connection aProducerConn = aConnFactory.createConnection();
        aProducerConn.start();

        Session aProducerSession = aProducerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = aProducerSession.createProducer(topic);
        produce(producer, aProducerSession, 5);

        // sleep for a sec to let the messages get bridged over to broker B
        sleep();

        // consumer on B has not consumed any messages, and for some reason goes away:
        bSessionConsumer.close();
        bSession.close();
        bConn.close();

        // let the bridge catch up
        sleep();

        // and now consumer reattaches to A and wants the messages that were sent to B
        Connection aConsumerConn = aConnFactory.createConnection();
        aConsumerConn.start();
        Session aConsumerSession = aConsumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer aSessionConsumer = aConsumerSession.createConsumer(queue);

        sleep();

        // they should all be there
        consume(aSessionConsumer, 5);

    }


    private void consume(MessageConsumer durable, int numMessagesExpected) throws JMSException {
        for (int i = 0; i < numMessagesExpected; i++) {
            Message message = durable.receive(1000);
            assertNotNull(message);
            TextMessage textMessage = (TextMessage) message;
            System.out.println("received: " + textMessage.getText());
            assertEquals("message: " +i, textMessage.getText());
        }
    }

    private void produce(MessageProducer producer, Session sess, int numMessages) throws JMSException {
        for (int i = 0; i < numMessages; i++) {
            producer.send(sess.createTextMessage("message: " + i));
        }
    }

    @Override
    protected void setUp() throws Exception {
        maxSetupTime = 1000;
        super.setAutoFail(true);
        super.setUp();
        final String options = "?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true";

        BrokerService brokerServiceA = createBroker(new URI(String.format("broker:(%s)/%s%s", BROKER_A_TRANSPORT_URL, BROKER_A, options)));
        brokerServiceA.setDestinationPolicy(buildPolicyMap());
        brokerServiceA.setDestinations(new ActiveMQDestination[]{queue});

        BrokerService brokerServiceB = createBroker(new URI(String.format("broker:(%s)/%s%s", BROKER_B_TRANSPORT_URL, BROKER_B, options)));
        brokerServiceB.setDestinationPolicy(buildPolicyMap());
        brokerServiceB.setDestinations(new ActiveMQDestination[]{queue});



        // bridge brokers to each other statically (static: discovery)
        bridgeBrokers(BROKER_A, BROKER_B);
        bridgeBrokers(BROKER_B, BROKER_A);

        startAllBrokers();
    }

    private PolicyMap buildPolicyMap() {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setOptimizedDispatch(true);
        ConditionalNetworkBridgeFilterFactory networkBridgeFilterFactory = new ConditionalNetworkBridgeFilterFactory();
        networkBridgeFilterFactory.setReplayWhenNoConsumers(true);
        policyEntry.setNetworkBridgeFilterFactory(networkBridgeFilterFactory);
        policyEntry.setEnableAudit(false);
        policyMap.put(new ActiveMQQueue("Consumer.*.VirtualTopic.>"), policyEntry);
        return policyMap;
    }

    private void sleep() {
        try {
            Thread.sleep(DEFAULT_SLEEP_MS);
        } catch (InterruptedException igonred) {
        }
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }
    }
}