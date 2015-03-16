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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.util.Wait;
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

        assertTrue("Should be a durable sub", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getDurableTopicSubscribers().length == 1;
            }

        }, TimeUnit.SECONDS.toMillis(5000), TimeUnit.MILLISECONDS.toMillis(10)));

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

        assertTrue("Should be a durable sub", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getDurableTopicSubscribers().length == 1;
            }

        }, TimeUnit.SECONDS.toMillis(5000), TimeUnit.MILLISECONDS.toMillis(10)));

        receiver.detach();

        assertTrue("Should be an inactive durable sub", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getInactiveDurableTopicSubscribers().length == 1;
            }

        }, TimeUnit.SECONDS.toMillis(5000), TimeUnit.MILLISECONDS.toMillis(10)));

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

        assertTrue("Should be a durable sub", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getDurableTopicSubscribers().length == 1;
            }

        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(10)));

        receiver.close();

        assertTrue("Should be an inactive durable sub", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getDurableTopicSubscribers().length == 0 &&
                       brokerView.getInactiveDurableTopicSubscribers().length == 0;
            }

        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(10)));

        connection.close();
    }
}