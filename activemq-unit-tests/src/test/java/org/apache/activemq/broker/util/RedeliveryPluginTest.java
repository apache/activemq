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
package org.apache.activemq.broker.util;

import junit.framework.TestCase;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ErrorBroker;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedeliveryPluginTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(RedeliveryPluginTest.class);
    RedeliveryPlugin underTest = new RedeliveryPlugin();

    public void testInstallPluginValidation() throws Exception {
        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        RedeliveryPolicy defaultEntry = new RedeliveryPolicy();
        defaultEntry.setInitialRedeliveryDelay(500);
        redeliveryPolicyMap.setDefaultEntry(defaultEntry);
        underTest.setRedeliveryPolicyMap(redeliveryPolicyMap);

        final BrokerService brokerService = new BrokerService();
        brokerService.setSchedulerSupport(false);
        Broker broker = new ErrorBroker("hi") {
            @Override
            public BrokerService getBrokerService() {
                return brokerService;
            }
        };

        try {
            underTest.installPlugin(broker);
            fail("expect exception on no scheduler support");
        } catch (Exception expected) {
            LOG.info("expected: " + expected);
        }

        brokerService.setSchedulerSupport(true);
        try {
            underTest.installPlugin(broker);
            fail("expect exception on small initial delay");
        } catch (Exception expected) {
            LOG.info("expected: " + expected);
        }

        defaultEntry.setInitialRedeliveryDelay(5000);
        defaultEntry.setRedeliveryDelay(500);
        brokerService.setSchedulerSupport(true);
        try {
            underTest.installPlugin(broker);
            fail("expect exception on small redelivery delay");
        } catch (Exception expected) {
            LOG.info("expected: " + expected);
        }
    }
}
