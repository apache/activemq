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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.activemq.ActiveMQSslConnectionFactoryTest.getKeyManager;
import static org.apache.activemq.ActiveMQSslConnectionFactoryTest.getTrustManager;
import static org.junit.Assert.assertTrue;

public class NetworkReconnectSslNioTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkReconnectSslNioTest.class);

    @Test
    public void testForceReconnect() throws Exception {

        final SslContext sslContext = new SslContext(getKeyManager(), getTrustManager(), null);

        BrokerService remote = new BrokerService();
        remote.setBrokerName("R");
        remote.setSslContext(sslContext);
        remote.setUseJmx(false);
        remote.setPersistent(false);
        final TransportConnector transportConnector = remote.addConnector("nio+ssl://0.0.0.0:0");
        remote.start();

        BrokerService local = new BrokerService();
        local.setSslContext(sslContext);
        local.setUseJmx(false);
        local.setPersistent(false);
        final NetworkConnector networkConnector = local.addNetworkConnector("static:(" + remote.getTransportConnectorByScheme("nio+ssl").getPublishableConnectString().replace("nio+ssl", "ssl") + ")?useExponentialBackOff=false&initialReconnectDelay=10");
        local.start();

        assertTrue("Bridge created", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !networkConnector.activeBridges().isEmpty() && (networkConnector.activeBridges().toArray(new DurableConduitBridge[]{})[0].getRemoteBrokerName() != null);
            }
        }));

        final AtomicReference<DurableConduitBridge> bridge = new AtomicReference<>((DurableConduitBridge) networkConnector.activeBridges().iterator().next());
        assertTrue("Connected to R", bridge.get().getRemoteBrokerName().equals("R"));

        for (int i=0; i<200;  i++) {
            LOG.info("Forcing error on NC via remote exception, iteration:" + i + ",  bridge: " + bridge);

            TransportConnection connection = transportConnector.getConnections().iterator().next();
            connection.dispatchAsync(new ConnectionError());

            assertTrue("bridge failed", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return bridge.get().bridgeFailed.get();
                }
            }, 10*1000, 10));

            bridge.set(null);
            assertTrue("Bridge recreated: " + i, Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    if (!networkConnector.activeBridges().isEmpty()) {
                        try {
                            DurableConduitBridge durableConduitBridge = (DurableConduitBridge) networkConnector.activeBridges().iterator().next();
                            if ("R".equals(durableConduitBridge.getRemoteBrokerName())) {
                                bridge.set(durableConduitBridge);
                            }
                        } catch (NoSuchElementException expectedContention) {}
                    }
                    return bridge.get() != null;
                }
            }, 10*1000, 10));
        }
        local.stop();
        remote.stop();
    }
}
