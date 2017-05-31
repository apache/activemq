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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.commons.lang.ArrayUtils;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class MQTTNetworkOfBrokersFailoverTest extends NetworkTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTNetworkOfBrokersFailoverTest.class);
    private int localBrokerMQTTPort = -1;
    private int remoteBrokerMQTTPort = -1;

    @Override
    protected void setUp() throws Exception {
        useJmx=true;
        super.setUp();

        URI ncUri = new URI("static:(" + connector.getConnectUri().toString() + ")");
        NetworkConnector nc = new DiscoveryNetworkConnector(ncUri);
        nc.setDuplex(true);
        remoteBroker.addNetworkConnector(nc);
        nc.start();

        // mqtt port should have been assigned by now
        assertFalse(localBrokerMQTTPort == -1);
        assertFalse(remoteBrokerMQTTPort == -1);
    }

    @Override
    protected void tearDown() throws Exception {
        if (remoteBroker.isStarted()) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
        if (broker.isStarted()) {
            broker.stop();
            broker.waitUntilStopped();
        }
        super.tearDown();
    }

    @Test
    public void testNoStaleSubscriptionAcrossNetwork() throws Exception {

        // before we get started, we want an async way to be able to know when
        // the durable consumer has been networked so we can assert that it indeed
        // would have a durable subscriber. for example, when we subscribe on remote broker,
        // a network-sub would be created on local broker and we want to listen for when that
        // even happens. we do that with advisory messages and a latch:
        CountDownLatch consumerNetworked = listenForConsumersOn(broker);

        // create a subscription with Clean == 0 (durable sub for QoS==1 && QoS==2)
        // on the remote broker. this sub should still be there after we disconnect
        MQTT remoteMqtt = createMQTTTcpConnection("foo", false, remoteBrokerMQTTPort);
        BlockingConnection remoteConn = remoteMqtt.blockingConnection();
        remoteConn.connect();
        remoteConn.subscribe(new Topic[]{new Topic("foo/bar", QoS.AT_LEAST_ONCE)});

        assertTrue("No destination detected!", consumerNetworked.await(1, TimeUnit.SECONDS));
        assertQueueExistsOn(remoteBroker, "Consumer.foo_AT_LEAST_ONCE.VirtualTopic.foo.bar");
        assertQueueExistsOn(broker, "Consumer.foo_AT_LEAST_ONCE.VirtualTopic.foo.bar");
        remoteConn.disconnect();

        // now we reconnect the same sub on the local broker, again with clean==0
        MQTT localMqtt = createMQTTTcpConnection("foo", false, localBrokerMQTTPort);
        BlockingConnection localConn = localMqtt.blockingConnection();
        localConn.connect();
        localConn.subscribe(new Topic[]{new Topic("foo/bar", QoS.AT_LEAST_ONCE)});

        // now let's connect back up to remote broker and send a message
        remoteConn = remoteMqtt.blockingConnection();
        remoteConn.connect();
        remoteConn.publish("foo/bar", "Hello, World!".getBytes(), QoS.AT_LEAST_ONCE, false);

        // now we should see that message on the local broker because the subscription
        // should have been properly networked... we'll give a sec of grace for the
        // networking and forwarding to have happened properly
        org.fusesource.mqtt.client.Message msg = localConn.receive(100, TimeUnit.SECONDS);
        assertNotNull(msg);
        msg.ack();
        String response = new String(msg.getPayload());
        assertEquals("Hello, World!", response);
        assertEquals("foo/bar", msg.getTopic());

        // Now... we SHOULD NOT see a message on the remote broker because we already
        // consumed it on the local broker... having the same message on the remote broker
        // would effectively give us duplicates in a distributed topic scenario:
        remoteConn.subscribe(new Topic[]{new Topic("foo/bar", QoS.AT_LEAST_ONCE)});
        msg = remoteConn.receive(500, TimeUnit.MILLISECONDS);
        assertNull("We have duplicate messages across the cluster for a distributed topic", msg);
    }

    private CountDownLatch listenForConsumersOn(BrokerService broker) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        URI brokerUri = broker.getVmConnectorURI();

        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri.toASCIIString());
        final Connection connection = cf.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = session.createTopic("ActiveMQ.Advisory.Consumer.Queue.Consumer.foo:AT_LEAST_ONCE.VirtualTopic.foo.bar");
        MessageConsumer consumer = session.createConsumer(dest);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                latch.countDown();
                // shutdown this connection
                Dispatch.getGlobalQueue().execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            session.close();
                            connection.close();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        return latch;
    }

    private void assertQueueExistsOn(BrokerService broker, String queueName) throws Exception {
        BrokerViewMBean brokerView = broker.getAdminView();
        ObjectName[] queueNames = brokerView.getQueues();
        assertEquals(1, queueNames.length);

        assertTrue(queueNames[0].toString().contains(queueName));
    }

    @SuppressWarnings("unused")
    private void assertOneDurableSubOn(BrokerService broker, String subName) throws Exception {
        BrokerViewMBean brokerView = broker.getAdminView();
        ObjectName[] activeDurableSubs = brokerView.getDurableTopicSubscribers();
        ObjectName[] inactiveDurableSubs = brokerView.getInactiveDurableTopicSubscribers();
        ObjectName[] allDurables = (ObjectName[]) ArrayUtils.addAll(activeDurableSubs, inactiveDurableSubs);
        assertEquals(1, allDurables.length);

        // at this point our assertions should prove that we have only on durable sub
        DurableSubscriptionViewMBean durableSubView = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(allDurables[0], DurableSubscriptionViewMBean.class, true);

        assertEquals(subName, durableSubView.getClientId());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker =  super.createBroker();
        broker.setPersistent(true);
        broker.setBrokerName("local");
        broker.setDataDirectory("target/activemq-data");
        broker.setDeleteAllMessagesOnStartup(true);
        TransportConnector tc = broker.addConnector(getDefaultMQTTTransportConnectorUri());
        localBrokerMQTTPort = tc.getConnectUri().getPort();
        return broker;
    }

    @Override
    protected BrokerService createRemoteBroker(PersistenceAdapter persistenceAdapter) throws Exception {
        BrokerService broker = super.createRemoteBroker(persistenceAdapter);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setDataDirectory("target/activemq-data");
        TransportConnector tc = broker.addConnector(getDefaultMQTTTransportConnectorUri());
        remoteBrokerMQTTPort = tc.getConnectUri().getPort();
        return broker;
    }

    private String getDefaultMQTTTransportConnectorUri(){
        return "mqtt://localhost:0?transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions";
    }

    private MQTT createMQTTTcpConnection(String clientId, boolean clean, int port) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setTracer(createTracer());
        if (clientId != null) {
            mqtt.setClientId(clientId);
        }
        mqtt.setCleanSession(clean);
        mqtt.setHost("localhost", port);
        return mqtt;
    }

    protected Tracer createTracer() {
        return new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client Received:\n" + frame);
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client Sent:\n" + frame);
            }

            @Override
            public void debug(String message, Object... args) {
                LOG.info(String.format(message, args));
            }
        };
    }
}
