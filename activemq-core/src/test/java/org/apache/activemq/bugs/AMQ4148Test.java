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

package org.apache.activemq.bugs;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DemandForwardingBridgeSupport;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.junit.Assert;

/**
 * This test demonstrates a bug in {@link DemandForwardingBridgeSupport} whereby
 * a static subscription from broker1 to broker2 is forwarded to broker3 even
 * though the network TTL is 1. This results in duplicate subscriptions on
 * broker3.
 */
public class AMQ4148Test extends JmsMultipleBrokersTestSupport {

    public void test() throws Exception {
        // Create a hub-and-spoke network where each hub-spoke pair share
        // messages on a test queue.
        BrokerService hub = createBroker(new URI("broker:(vm://hub)/hub?persistent=false"));

        final BrokerService[] spokes = new BrokerService[4];
        for (int i = 0; i < spokes.length; i++) {
            spokes[i] = createBroker(new URI("broker:(vm://spoke" + i + ")/spoke" + i + "?persistent=false"));

        }
        startAllBrokers();

        ActiveMQDestination testQueue = createDestination(AMQ4148Test.class.getSimpleName() + ".queue", false);

        NetworkConnector[] ncs = new NetworkConnector[spokes.length];
        for (int i = 0; i < spokes.length; i++) {
            NetworkConnector nc = bridgeBrokers("hub", "spoke" + i);
            nc.setNetworkTTL(1);
            nc.setDuplex(true);
            nc.setConduitSubscriptions(false);
            nc.setStaticallyIncludedDestinations(Arrays.asList(testQueue));
            nc.start();

            ncs[i] = nc;
        }

        waitForBridgeFormation();

        // Pause to allow subscriptions to be created.
        TimeUnit.SECONDS.sleep(5);

        // Verify that the hub has a subscription from each spoke, but that each
        // spoke has a single subscription from the hub (since the network TTL is 1).
        final Destination hubTestQueue = hub.getDestination(testQueue);
        assertTrue("Expecting {" + spokes.length + "} consumer but was {" + hubTestQueue.getConsumers().size() + "}",
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return spokes.length == hubTestQueue.getConsumers().size();
                }
            })
        );

        // Now check each spoke has exactly one consumer on the Queue.
        for (int i = 0; i < 4; i++) {
            Destination spokeTestQueue = spokes[i].getDestination(testQueue);
            Assert.assertEquals(1, spokeTestQueue.getConsumers().size());
        }

        for (NetworkConnector nc : ncs) {
            nc.stop();
        }
    }
}
