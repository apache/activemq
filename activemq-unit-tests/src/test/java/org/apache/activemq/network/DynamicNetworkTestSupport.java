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

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.util.Wait;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public abstract class DynamicNetworkTestSupport {

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String testTopicName = "include.test.bar";
    protected String excludeTopicName = "exclude.test.bar";
    protected String clientId = "clientId";
    protected String subName = "subId";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    protected RemoveSubscriptionInfo getRemoveSubscriptionInfo(final ConnectionContext context,
            final BrokerService brokerService) throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubcriptionName(subName);
        context.setBroker(brokerService.getBroker());
        context.setClientId(clientId);
        return info;
    }

    protected void waitForConsumerCount(final DestinationStatistics destinationStatistics, final int count) throws Exception {
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                //should only be 1 for the composite destination creation
                return count == destinationStatistics.getConsumers().getCount();
            }
        });
    }

    protected void waitForDispatchFromLocalBroker(final DestinationStatistics destinationStatistics, final int count) throws Exception {
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == destinationStatistics.getDequeues().getCount() &&
                       count == destinationStatistics.getDispatched().getCount() &&
                       count == destinationStatistics.getForwards().getCount();
            }
        });
    }

    protected void assertLocalBrokerStatistics(final DestinationStatistics localStatistics, final int count) {
        assertEquals("local broker dest stat dispatched", count, localStatistics.getDispatched().getCount());
        assertEquals("local broker dest stat dequeues", count, localStatistics.getDequeues().getCount());
        assertEquals("local broker dest stat forwards", count, localStatistics.getForwards().getCount());
    }

    protected interface ConsumerCreator {
        MessageConsumer createConsumer() throws JMSException;
    }

}
