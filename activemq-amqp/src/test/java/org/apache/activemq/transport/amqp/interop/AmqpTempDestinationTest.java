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

import static org.apache.activemq.transport.amqp.AmqpSupport.LIFETIME_POLICY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_QUEUE_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_TOPIC_CAPABILITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.junit.Test;

/**
 * Tests for JMS temporary destination mappings to AMQP
 */
public class AmqpTempDestinationTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testCannotCreateSenderWithNamedTempQueue() throws Exception {
        doTestCannotCreateSenderWithNamedTempDestination(false);
    }

    @Test(timeout = 60000)
    public void testCannotCreateSenderWithNamedTempTopic() throws Exception {
        doTestCannotCreateSenderWithNamedTempDestination(true);
    }

    protected void doTestCannotCreateSenderWithNamedTempDestination(boolean topic) throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        String address = null;
        if (topic) {
            address = "temp-topic://" + getTestName();
        } else {
            address = "temp-queue://" + getTestName();
        }

        try {
            session.createSender(address);
            fail("Should not be able to create sender to a temp destination that doesn't exist.");
        } catch (Exception ex) {
            LOG.info("Error creating sender: {}", ex.getMessage());
        }
    }

    @Test(timeout = 60000)
    public void testCanntCreateReceverWithNamedTempQueue() throws Exception {
        doTestCannotCreateReceiverWithNamedTempDestination(false);
    }

    @Test(timeout = 60000)
    public void testCannotCreateReceiverWithNamedTempTopic() throws Exception {
        doTestCannotCreateReceiverWithNamedTempDestination(true);
    }

    protected void doTestCannotCreateReceiverWithNamedTempDestination(boolean topic) throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        String address = null;
        if (topic) {
            address = "temp-topic://" + getTestName();
        } else {
            address = "temp-queue://" + getTestName();
        }

        try {
            session.createReceiver(address);
            fail("Should not be able to create sender to a temp destination that doesn't exist.");
        } catch (Exception ex) {
            LOG.info("Error creating sender: {}", ex.getMessage());
        }
    }

    @Test(timeout = 60000)
    public void testCreateDynamicSenderToTopic() throws Exception {
        doTestCreateDynamicSender(true);
    }

    @Test(timeout = 60000)
    public void testCreateDynamicSenderToQueue() throws Exception {
        doTestCreateDynamicSender(false);
    }

    protected void doTestCreateDynamicSender(boolean topic) throws Exception {
        Target target = createDynamicTarget(topic);

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender(target);
        assertNotNull(sender);

        if (topic) {
            assertEquals(1, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(1, brokerView.getTemporaryQueues().length);
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testDynamicSenderLifetimeBoundToLinkTopic() throws Exception {
        doTestDynamicSenderLifetimeBoundToLinkQueue(true);
    }

    @Test(timeout = 60000)
    public void testDynamicSenderLifetimeBoundToLinkQueue() throws Exception {
        doTestDynamicSenderLifetimeBoundToLinkQueue(false);
    }

    protected void doTestDynamicSenderLifetimeBoundToLinkQueue(boolean topic) throws Exception {
        Target target = createDynamicTarget(topic);

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender(target);
        assertNotNull(sender);

        if (topic) {
            assertEquals(1, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(1, brokerView.getTemporaryQueues().length);
        }

        sender.close();

        if (topic) {
            assertEquals(0, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(0, brokerView.getTemporaryQueues().length);
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateDynamicReceiverToTopic() throws Exception {
        doTestCreateDynamicSender(true);
    }

    @Test(timeout = 60000)
    public void testCreateDynamicReceiverToQueue() throws Exception {
        doTestCreateDynamicSender(false);
    }

    protected void doTestCreateDynamicReceiver(boolean topic) throws Exception {
        Source source = createDynamicSource(topic);

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(source);
        assertNotNull(receiver);

        if (topic) {
            assertEquals(1, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(1, brokerView.getTemporaryQueues().length);
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testDynamicReceiverLifetimeBoundToLinkTopic() throws Exception {
        doTestDynamicReceiverLifetimeBoundToLinkQueue(true);
    }

    @Test(timeout = 60000)
    public void testDynamicReceiverLifetimeBoundToLinkQueue() throws Exception {
        doTestDynamicReceiverLifetimeBoundToLinkQueue(false);
    }

    protected void doTestDynamicReceiverLifetimeBoundToLinkQueue(boolean topic) throws Exception {
        Source source = createDynamicSource(topic);

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(source);
        assertNotNull(receiver);

        if (topic) {
            assertEquals(1, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(1, brokerView.getTemporaryQueues().length);
        }

        receiver.close();

        if (topic) {
            assertEquals(0, brokerView.getTemporaryTopics().length);
        } else {
            assertEquals(0, brokerView.getTemporaryQueues().length);
        }

        connection.close();
    }

    protected Source createDynamicSource(boolean topic) {

        Source source = new Source();
        source.setDynamic(true);
        source.setDurable(TerminusDurability.NONE);
        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        // Set the dynamic node lifetime-policy
        Map<Symbol, Object> dynamicNodeProperties = new HashMap<Symbol, Object>();
        dynamicNodeProperties.put(LIFETIME_POLICY, DeleteOnClose.getInstance());
        source.setDynamicNodeProperties(dynamicNodeProperties);

        // Set the capability to indicate the node type being created
        if (!topic) {
            source.setCapabilities(TEMP_QUEUE_CAPABILITY);
        } else {
            source.setCapabilities(TEMP_TOPIC_CAPABILITY);
        }

        return source;
    }

    protected Target createDynamicTarget(boolean topic) {

        Target target = new Target();
        target.setDynamic(true);
        target.setDurable(TerminusDurability.NONE);
        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        // Set the dynamic node lifetime-policy
        Map<Symbol, Object> dynamicNodeProperties = new HashMap<Symbol, Object>();
        dynamicNodeProperties.put(LIFETIME_POLICY, DeleteOnClose.getInstance());
        target.setDynamicNodeProperties(dynamicNodeProperties);

        // Set the capability to indicate the node type being created
        if (!topic) {
            target.setCapabilities(TEMP_QUEUE_CAPABILITY);
        } else {
            target.setCapabilities(TEMP_TOPIC_CAPABILITY);
        }

        return target;
    }
}
