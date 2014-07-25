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
package org.apache.activemq;

import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

/**
 * User: gtully
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class ExpiryHogTest extends JmsMultipleClientsTestSupport {
    boolean sleep = false;

    int numMessages = 4;

    @Test(timeout = 2 * 60 * 1000)
    public void testImmediateDispatchWhenCacheDisabled() throws Exception {
        ConnectionFactory f = createConnectionFactory();
        destination = createDestination();
        startConsumers(f, destination);
        sleep = true;
        this.startProducers(f, destination, numMessages);
        allMessagesList.assertMessagesReceived(numMessages);
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService bs = new BrokerService();
        bs.setDeleteAllMessagesOnStartup(true);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(5000);
        defaultEntry.setUseCache(false);
        policyMap.setDefaultEntry(defaultEntry);
        bs.setDestinationPolicy(policyMap);

        KahaDBPersistenceAdapter ad = (KahaDBPersistenceAdapter) bs.getPersistenceAdapter();
        ad.setConcurrentStoreAndDispatchQueues(true);
        return bs;
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        if (sleep) {
            TimeUnit.SECONDS.sleep(10);
        }
        TextMessage msg = super.createTextMessage(session, initText);
        msg.setJMSExpiration(4000);
        return msg;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        autoFail = false;
        persistent = true;
        super.setUp();
    }
}
