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

package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompPrefetchTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompPrefetchTest.class);

    @Override
    protected void applyBrokerPolicies() throws Exception {

        PolicyEntry policy = new PolicyEntry();
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        policy.setDurableTopicPrefetch(10);
        policy.setQueueBrowserPrefetch(10);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        brokerService.setDestinationPolicy(pMap);
        brokerService.setAdvisorySupport(true);
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
    }

    @Test(timeout = 60000)
    public void testTopicSubPrefetch() throws Exception {
        stompConnect();
        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/T", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        verifyPrefetch(10, new ActiveMQTopic("T"));
    }

    @Test(timeout = 60000)
    public void testDurableSubPrefetch() throws Exception {
        stompConnect();
        stompConnection.connect("system", "manager");
        HashMap<String,String> headers = new HashMap<String, String>();
        headers.put("id", "durablesub");
        stompConnection.subscribe("/topic/T", Stomp.Headers.Subscribe.AckModeValues.AUTO, headers);

        verifyPrefetch(10, new ActiveMQTopic("T"));
    }

    @Test(timeout = 60000)
    public void testQBrowserSubPrefetch() throws Exception {
        HashMap<String,String> headers = new HashMap<String, String>();
        headers.put("login","system");
        headers.put("passcode","manager");
        headers.put("id", "aBrowser");
        headers.put("browser", "true");
        headers.put("accept-version","1.1");

        stompConnect();
        stompConnection.connect(headers);
        stompConnection.subscribe("/queue/Q", Stomp.Headers.Subscribe.AckModeValues.AUTO, headers);

        verifyPrefetch(10, new ActiveMQQueue("Q"));
    }

    @Test(timeout = 60000)
    public void testQueueSubPrefetch() throws Exception {
        stompConnect();
        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/queue/Q", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        verifyPrefetch(10, new ActiveMQQueue("Q"));
    }

    private void verifyPrefetch(final int val, final Destination dest) throws Exception {
        assertTrue("success in time", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    Subscription sub =
                            brokerService.getRegionBroker().getDestinationMap().get(ActiveMQDestination.transform(dest)).getConsumers().get(0);
                    LOG.info("sub prefetch: " + sub.getConsumerInfo().getPrefetchSize());
                    return val == sub.getConsumerInfo().getPrefetchSize();
                } catch (Exception ignored) {
                }
                return false;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));
    }
}
