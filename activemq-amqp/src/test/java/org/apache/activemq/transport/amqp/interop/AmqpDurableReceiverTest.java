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
package org.apache.activemq.transport.amqp.interop;

import static org.apache.activemq.transport.amqp.AmqpSupport.COPY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.engine.Receiver;
import org.junit.Test;

/**
 * Tests for broker side support of the Durable Subscription mapping for JMS.
 */
public class AmqpDurableReceiverTest extends AmqpClientTestSupport {

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }

    @Test(timeout = 60000)
    public void testCreateDurableReceiver() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.createConnection();
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
        AmqpConnection connection = client.createConnection();
        connection.setContainerId(getTestName());
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpReceiver receiver = session.createDurableReceiver("topic://" + getTestName(), getTestName());

        final BrokerViewMBean brokerView = getProxyToBroker();

        assertEquals(1, brokerView.getDurableTopicSubscribers().length);
        assertEquals(0, brokerView.getInactiveDurableTopicSubscribers().length);

        receiver.detach();

        assertEquals(0, brokerView.getDurableTopicSubscribers().length);
        assertEquals(1, brokerView.getInactiveDurableTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCloseDurableReceiverRemovesSubscription() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.createConnection();
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
        AmqpConnection connection = client.createConnection();
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
    public void testLookupExistingSubscription() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.createConnection();
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
        AmqpConnection connection = client.createConnection();
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