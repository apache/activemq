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
package org.apache.activemq.usecases;

import java.net.URI;
import javax.jms.MessageConsumer;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvisoryViaNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AdvisoryViaNetworkTest.class);


    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
        brokers.put(brokerName, new BrokerItem(broker));

        return broker;
    }

    public void testAdvisoryForwarding() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Producer.Topic.FOO");

        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);
        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);


        MessageConsumer consumerA = createConsumer("A", advisoryTopic);
        MessageConsumer consumerB = createConsumer("B", advisoryTopic);

        this.sendMessages("A", new ActiveMQTopic("FOO"), 1);

        MessageIdList messagesA = getConsumerMessages("A", consumerA);
        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(2);
        messagesB.assertMessagesReceived(2);
    }


    public void testBridgeRelevantAdvisoryNotAvailable() throws Exception {
        ActiveMQTopic advisoryTopic = new ActiveMQTopic("ActiveMQ.Advisory.Consumer.Topic.FOO");
        createBroker("A");
        createBroker("B");
        NetworkConnector networkBridge = bridgeBrokers("A", "B");
        networkBridge.addStaticallyIncludedDestination(advisoryTopic);

        startAllBrokers();
        verifyPeerBrokerInfo(brokers.get("A"), 1);


        MessageConsumer consumerA = createConsumer("A", advisoryTopic);
        MessageConsumer consumerB = createConsumer("B", advisoryTopic);

        createConsumer("A", new ActiveMQTopic("FOO"));

        MessageIdList messagesA = getConsumerMessages("A", consumerA);
        MessageIdList messagesB = getConsumerMessages("B", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(1);
        messagesB.assertMessagesReceived(0);
    }

    private void verifyPeerBrokerInfo(BrokerItem brokerItem, final int max) throws Exception {
        final BrokerService broker = brokerItem.broker;
        final RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
                return max == regionBroker.getPeerBrokerInfos().length;
            }
         }, 120 * 1000);
        LOG.info("verify infos " + broker.getBrokerName() + ", len: " + regionBroker.getPeerBrokerInfos().length);
        for (BrokerInfo info : regionBroker.getPeerBrokerInfos()) {
            LOG.info(info.getBrokerName());
        }
        assertEquals(broker.getBrokerName(), max, regionBroker.getPeerBrokerInfos().length);
    }


    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

}
