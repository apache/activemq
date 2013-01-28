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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;

import javax.jms.MessageConsumer;
import java.net.URI;

public class StaticNetworkTest extends JmsMultipleBrokersTestSupport {

    public void testStaticNetwork() throws Exception {
        // Setup destination
        ActiveMQDestination dest = createDestination("TEST", false);
        ActiveMQDestination dest1 = createDestination("TEST1", false);

        NetworkConnector bridgeAB =bridgeBrokers("BrokerA", "BrokerB", true);
        bridgeAB.addStaticallyIncludedDestination(dest);
        bridgeAB.setStaticBridge(true);

        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer consumer1 = createConsumer("BrokerB", dest);
        MessageConsumer consumer2 = createConsumer("BrokerB", dest1);


        Thread.sleep(1000);


        sendMessages("BrokerA", dest,  1);
        sendMessages("BrokerA", dest1, 1);

        MessageIdList msgs1 = getConsumerMessages("BrokerB", consumer1);
        MessageIdList msgs2 = getConsumerMessages("BrokerB", consumer2);

        msgs1.waitForMessagesToArrive(1);

        Thread.sleep(1000);

        assertEquals(1, msgs1.getMessageCount());
        assertEquals(0, msgs2.getMessageCount());

    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
    }

}
