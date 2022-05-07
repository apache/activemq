/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SessionInfo;

import java.util.Collection;

public class BrokerFacadeSupportTest extends BrokerTestSupport {
    public void testGetQueueConsumerWorksWithSimpleQueue() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        setupConsumer(destination);
        BrokerFacadeSupport brokerFacadeSupport = new LocalBrokerFacade(broker);
        Collection<SubscriptionViewMBean> subscriptionViewMBeans = brokerFacadeSupport.getQueueConsumers("TEST");
        assertEquals(1, subscriptionViewMBeans.size());
    }

    public void testGetQueueConsumerWorksWithQueueWithColon() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST:TEST");
        setupConsumer(destination);
        BrokerFacadeSupport brokerFacadeSupport = new LocalBrokerFacade(broker);
        Collection<SubscriptionViewMBean> subscriptionViewMBeans = brokerFacadeSupport.getQueueConsumers("TEST:TEST");
        assertEquals(1, subscriptionViewMBeans.size());
    }

    private void setupConsumer(ActiveMQDestination destination) throws Exception {
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.request(consumerInfo1);
    }
}
