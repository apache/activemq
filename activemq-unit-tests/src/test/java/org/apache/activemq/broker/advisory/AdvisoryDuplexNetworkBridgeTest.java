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
package org.apache.activemq.broker.advisory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;

public class AdvisoryDuplexNetworkBridgeTest extends AdvisoryNetworkBridgeTest {

    @Override
    public void createBroker1() throws Exception {
        broker1 = new BrokerService();
        broker1.setBrokerName("broker1");
        broker1.addConnector("tcp://localhost:0");
        broker1.setUseJmx(false);
        broker1.setPersistent(false);
        broker1.start();
        broker1.waitUntilStarted();
    }

    @Override
    public void createBroker2() throws Exception {
        // Programmatic equivalent of duplexLocalBroker.xml with ephemeral port
        broker2 = new BrokerService();
        broker2.setBrokerName("localBroker");
        broker2.setPersistent(true);
        broker2.setUseShutdownHook(false);
        broker2.setUseJmx(false);
        broker2.addConnector("tcp://localhost:0");

        final String broker1Uri = broker1.getTransportConnectors().get(0).getConnectUri().toString();
        final NetworkConnector nc = broker2.addNetworkConnector("static:(" + broker1Uri + ")");
        nc.setDuplex(true);
        nc.setDynamicOnly(false);
        nc.setConduitSubscriptions(true);
        nc.setDecreaseNetworkConsumerPriority(false);
        nc.getExcludedDestinations().add(new ActiveMQQueue("exclude.test.foo"));
        nc.getExcludedDestinations().add(new ActiveMQTopic("exclude.test.bar"));

        broker2.start();
        broker2.waitUntilStarted();
    }

    public void assertCreatedByDuplex(final boolean createdByDuplex) {
        assertTrue(createdByDuplex);
    }
}
