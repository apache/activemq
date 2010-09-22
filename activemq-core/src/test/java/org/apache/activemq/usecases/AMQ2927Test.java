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

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import javax.jms.MessageConsumer;
import java.net.URI;


public class AMQ2927Test extends JmsMultipleBrokersTestSupport {

    private static final Log LOG = LogFactory.getLog(AMQ2927Test.class);

    ActiveMQQueue queue = new ActiveMQQueue("TEST");

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        BrokerService brokerA = createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true"));
        brokerA.setBrokerId("BrokerA");
        BrokerService brokerB = createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true"));
        brokerB.setBrokerId("BrokerB");
        NetworkConnector aTOb = bridgeBrokers(brokers.get("BrokerA").broker, brokers.get("BrokerB").broker, false, 2, true, true);
        aTOb.addStaticallyIncludedDestination(queue);
        NetworkConnector bTOa = bridgeBrokers(brokers.get("BrokerB").broker, brokers.get("BrokerA").broker, false, 2, true, true);
        bTOa.addStaticallyIncludedDestination(queue);

        startAllBrokers();
        waitForBridgeFormation();
        
    }

    public void testRestartSend() throws Exception {

        Thread.sleep(1000);

        LOG.info("restarting broker");

        restartBroker("BrokerA");

        Thread.sleep(5000);

        LOG.info("sending message");

        sendMessages("BrokerA", queue, 1);

        Thread.sleep(3000);

        LOG.info("consuming message");

        MessageConsumer consumerA = createConsumer("BrokerA", queue);
        MessageConsumer consumerB = createConsumer("BrokerB", queue);

        Thread.sleep(1000);

        MessageIdList messagesA = getConsumerMessages("BrokerA", consumerA);
        MessageIdList messagesB = getConsumerMessages("BrokerB", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(0);
        messagesB.assertMessagesReceived(1);

    }


    public void testSendRestart() throws Exception {

        Thread.sleep(1000);

        LOG.info("sending message");

        sendMessages("BrokerA", queue, 1);

        Thread.sleep(3000);

        LOG.info("restarting broker");

        restartBroker("BrokerA");

        Thread.sleep(5000);

        LOG.info("consuming message");

        MessageConsumer consumerA = createConsumer("BrokerA", queue);
        MessageConsumer consumerB = createConsumer("BrokerB", queue);

        Thread.sleep(1000);

        MessageIdList messagesA = getConsumerMessages("BrokerA", consumerA);
        MessageIdList messagesB = getConsumerMessages("BrokerB", consumerB);

        LOG.info("consumerA = " + messagesA);
        LOG.info("consumerB = " + messagesB);

        messagesA.assertMessagesReceived(0);
        messagesB.assertMessagesReceived(1);
    }

    protected void restartBroker(String brokerName) throws Exception {
        destroyBroker("BrokerA");
        BrokerService broker = createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=true&useJmx=false"));
        broker.setBrokerId("BrokerA");
        NetworkConnector aTOb = bridgeBrokers(brokers.get("BrokerA").broker, brokers.get("BrokerB").broker, false, 2, true, true);
        aTOb.addStaticallyIncludedDestination(queue);
        broker.start();
        broker.waitUntilStarted();
        waitForBridgeFormation();
    }

}