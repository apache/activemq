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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DemandForwardingBridgeSupport;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class TwoBrokerQueueSendReceiveTest  extends TwoBrokerTopicSendReceiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(TwoBrokerQueueSendReceiveTest.class);

    protected void setUp() throws Exception {
        topic = false;
        super.setUp();
    }

    public void testReceiveOnXConsumersNoLeak() throws Exception {
        consumer.close();
        sendMessages();
        for (int i=0; i<data.length; i++) {
            consumer = createConsumer();
            onMessage(consumer.receive(10000));
            consumer.close();
        }
        waitForMessagesToBeDelivered();
        assertEquals("Got all messages", data.length, messages.size());

        BrokerService broker = (BrokerService) brokers.get("receiver");
        final DemandForwardingBridgeSupport bridge = (DemandForwardingBridgeSupport) broker.getNetworkConnectors().get(0).activeBridges().toArray()[0];
        assertTrue("No extra, size:" + bridge.getLocalSubscriptionMap().size(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("local subs map size = " + bridge.getLocalSubscriptionMap().size());
                return 0 == bridge.getLocalSubscriptionMap().size();
            }
        }));

    }
    
}
