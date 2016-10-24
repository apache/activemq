/*
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
package org.apache.activemq.transport.amqp.interop;

import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_NAME;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpFrameValidator;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.engine.Receiver;
import org.junit.Test;

/**
 * Tests for broker side support of the Durable Subscription mapping for JMS.
 */
public class AmqpDurableReceiverTest extends AmqpClientTestSupport {

    private final String SELECTOR_STRING = "color = red";

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Test(timeout = 60000)
    public void testCreateDurableReceiver() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        session.createDurableReceiver("topic://" + getTestName(), getTestName());

        final BrokerViewMBean brokerView = getProxyToBroker();

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testDetachedDurableReceiverRemainsActive() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        connection.setReceivedFrameInspector(new AmqpFrameValidator() {

            @Override
            public void inspectDetach(Detach detach, Binary encoded) {
                if (detach.getClosed()) {
                    markAsInvalid("Remote should have detached but closed instead.");
                }
            }
        });

        connection.setSentFrameInspector(new AmqpFrameValidator() {

            @Override
            public void inspectDetach(Detach detach, Binary encoded) {
                if (detach.getClosed()) {
                    markAsInvalid("Client should have detached but closed instead.");
                }
            }
        });

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        final BrokerViewMBean brokerView = getProxyToBroker();

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.getSentFrameInspector().assertValid();
        connection.getReceivedFrameInspector().assertValid();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCloseDurableReceiverRemovesSubscription() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        final BrokerViewMBean brokerView = getProxyToBroker();

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReattachToDurableNode() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReattachToDurableNodeAfterRestart() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        connection.close();

        restartBroker();

        connection = client.createConnection();
        connection.setContainerId(getTestName());
        connection.connect();
        session = connection.createSession();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupExistingSubscription() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.lookupSubscription(getTestName());

        assertNotNull(receiver);

        Receiver protonReceiver = receiver.getReceiver();
        assertNotNull(protonReceiver.getRemoteSource());
        Source remoteSource = (Source) protonReceiver.getRemoteSource();

        if (remoteSource.getFilter() != null) {
            assertFalse(remoteSource.getFilter().containsKey(NO_LOCAL_NAME));
            assertFalse(remoteSource.getFilter().containsKey(JMS_SELECTOR_NAME));
        }

        assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        assertEquals(COPY, remoteSource.getDistributionMode());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupExistingSubscriptionWithSelector() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName(), SELECTOR_STRING);

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.lookupSubscription(getTestName());

        assertNotNull(receiver);

        Receiver protonReceiver = receiver.getReceiver();
        assertNotNull(protonReceiver.getRemoteSource());
        Source remoteSource = (Source) protonReceiver.getRemoteSource();

        assertNotNull(remoteSource.getFilter());
        assertFalse(remoteSource.getFilter().containsKey(NO_LOCAL_NAME));
        assertTrue(remoteSource.getFilter().containsKey(JMS_SELECTOR_NAME));
        assertEquals(SELECTOR_STRING, ((DescribedType) remoteSource.getFilter().get(JMS_SELECTOR_NAME)).getDescribed());

        assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        assertEquals(COPY, remoteSource.getDistributionMode());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupExistingSubscriptionWithNoLocal() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName(), null, true);

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.lookupSubscription(getTestName());

        assertNotNull(receiver);

        Receiver protonReceiver = receiver.getReceiver();
        assertNotNull(protonReceiver.getRemoteSource());
        Source remoteSource = (Source) protonReceiver.getRemoteSource();

        assertNotNull(remoteSource.getFilter());
        assertTrue(remoteSource.getFilter().containsKey(NO_LOCAL_NAME));
        assertFalse(remoteSource.getFilter().containsKey(JMS_SELECTOR_NAME));

        assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        assertEquals(COPY, remoteSource.getDistributionMode());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupExistingSubscriptionWithSelectorAndNoLocal() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName(), SELECTOR_STRING, true);

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver = session.lookupSubscription(getTestName());

        assertNotNull(receiver);

        Receiver protonReceiver = receiver.getReceiver();
        assertNotNull(protonReceiver.getRemoteSource());
        Source remoteSource = (Source) protonReceiver.getRemoteSource();

        assertNotNull(remoteSource.getFilter());
        assertTrue(remoteSource.getFilter().containsKey(NO_LOCAL_NAME));
        assertTrue(remoteSource.getFilter().containsKey(JMS_SELECTOR_NAME));
        assertEquals(SELECTOR_STRING, ((DescribedType) remoteSource.getFilter().get(JMS_SELECTOR_NAME)).getDescribed());

        assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        assertEquals(COPY, remoteSource.getDistributionMode());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupExistingSubscriptionAfterRestartWithSelectorAndNoLocal() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName(), SELECTOR_STRING, true);

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        restartBroker();

        connection = client.createConnection();
        connection.setContainerId(getTestName());
        connection.connect();

        session = connection.createSession();
        receiver = session.lookupSubscription(getTestName());

        assertNotNull(receiver);

        Receiver protonReceiver = receiver.getReceiver();
        assertNotNull(protonReceiver.getRemoteSource());
        Source remoteSource = (Source) protonReceiver.getRemoteSource();

        assertNotNull(remoteSource.getFilter());
        assertTrue(remoteSource.getFilter().containsKey(NO_LOCAL_NAME));
        assertTrue(remoteSource.getFilter().containsKey(JMS_SELECTOR_NAME));
        assertEquals(SELECTOR_STRING, ((DescribedType) remoteSource.getFilter().get(JMS_SELECTOR_NAME)).getDescribed());

        assertEquals(TerminusExpiryPolicy.NEVER, remoteSource.getExpiryPolicy());
        assertEquals(TerminusDurability.UNSETTLED_STATE, remoteSource.getDurable());
        assertEquals(COPY, remoteSource.getDistributionMode());

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.close();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testLookupNonExistingSubscription() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        try {
            session.lookupSubscription(getTestName());
            fail("Should throw an exception since there is not subscription");
        } catch (Exception e) {
            LOG.info("Error on lookup: {}", e.getMessage());
        }

        connection.close();
    }
}