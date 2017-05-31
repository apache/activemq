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
import static org.junit.Assert.assertTrue;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.Test;

public class NetworkLoopBackTest {
    @Test
    public void testLoopbackOnDifferentUrlScheme() throws Exception {
        final BrokerService brokerServce = new BrokerService();
        brokerServce.setPersistent(false);

        TransportConnector transportConnector = brokerServce.addConnector("nio://0.0.0.0:0");
        // connection filter is bypassed when scheme is different
        final NetworkConnector networkConnector = brokerServce.addNetworkConnector("static:(tcp://"
                + transportConnector.getConnectUri().getHost() + ":" +  transportConnector.getConnectUri().getPort() + ")");

        brokerServce.start();
        brokerServce.waitUntilStarted();

        try {
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 1 == networkConnector.bridges.size();
                }
            });

            final DemandForwardingBridgeSupport loopbackBridge = (DemandForwardingBridgeSupport) networkConnector.bridges.values().iterator().next();
            assertTrue("nc started", networkConnector.isStarted());

            assertTrue("It should get disposed", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return loopbackBridge.getRemoteBroker().isDisposed();
                }
            }));

            assertEquals("No peer brokers", 0, brokerServce.getBroker().getPeerBrokerInfos().length);

        } finally {
            brokerServce.stop();
        }
    }
}