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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3841Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ3841Test.class);
    private final static int maxFileLength = 1024*1024*32;
    private final static String destinationName = "TEST.QUEUE";
    BrokerService broker;

    @Before
    public void setUp() throws Exception {
        prepareBrokerWithMultiStore(true);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    @Test
    public void testRestartAfterQueueDelete() throws Exception {

        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));


        broker.getAdminView().addQueue(destinationName);

        assertNotNull(broker.getDestination(new ActiveMQQueue(destinationName)));

        broker.getAdminView().removeQueue(destinationName);

        broker.stop();
        broker.waitUntilStopped();

        prepareBrokerWithMultiStore(false);
        broker.start();

        broker.getAdminView().addQueue(destinationName);
        assertNotNull(broker.getDestination(new ActiveMQQueue(destinationName)));

    }

    protected KahaDBPersistenceAdapter createStore(boolean delete) throws IOException {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setJournalMaxFileLength(maxFileLength);
        kaha.setCleanupInterval(5000);
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    public void prepareBrokerWithMultiStore(boolean deleteAllMessages) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        if (deleteAllMessages) {
            multiKahaDBPersistenceAdapter.deleteAllMessages();
        }
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
        template.setPersistenceAdapter(createStore(deleteAllMessages));
        template.setPerDestination(true);
        adapters.add(template);

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        broker = createBroker(multiKahaDBPersistenceAdapter);
    }
}
