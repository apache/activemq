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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jms.MessageProducer;
import javax.jms.TemporaryQueue;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplexNetworkTest extends SimpleNetworkTest {
    private static final Logger LOG = LoggerFactory.getLogger(DuplexNetworkTest.class);

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/duplexLocalBroker.xml";
    }

    @Override
    protected BrokerService createRemoteBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("remoteBroker");
        broker.addConnector("tcp://localhost:61617?transport.connectAttemptTimeout=2000");
        return broker;
    }

    @Test
    public void testTempQueues() throws Exception {
        TemporaryQueue temp = localSession.createTemporaryQueue();
        MessageProducer producer = localSession.createProducer(temp);
        producer.send(localSession.createTextMessage("test"));
        Thread.sleep(100);
        assertEquals("Destination not created", 1, remoteBroker.getAdminView().getTemporaryQueues().length);
        temp.delete();

        assertTrue("Destination not deleted", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == remoteBroker.getAdminView().getTemporaryQueues().length;
            }
        }));
    }

    @Test
    public void testStaysUp() throws Exception {
        int bridgeIdentity = getBridgeId();
        LOG.info("Bridges: " + bridgeIdentity);
        TimeUnit.SECONDS.sleep(5);
        assertEquals("Same bridges", bridgeIdentity, getBridgeId());
    }

    private int getBridgeId() {
        int id = 0;
        while (id == 0) {
            try {
                id = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next().hashCode();
            } catch (Throwable tryAgainInABit) {
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return id;
    }

    @Override
    protected void assertNetworkBridgeStatistics(final long expectedLocalSent, final long expectedRemoteSent) throws Exception {

        final NetworkBridge localBridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return expectedLocalSent == localBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                        expectedRemoteSent == localBridge.getNetworkBridgeStatistics().getReceivedCount().getCount();
            }
        }));

    }

}
