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
package org.apache.activemq.store;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.File;

public class MKahaDBConfigTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MKahaDBConfigTest.class);

    /*
     * This tests configuring the different broker properties using
     * xbeans-spring
     */
    public void testBrokerConfig() throws Exception {
        BrokerService broker;

        broker = createBroker("org/apache/activemq/store/mKahaDB.xml");
        LOG.info("Success");

        try {
            assertEquals("Broker Config Error (brokerName)", "brokerConfigTest", broker.getBrokerName());
            assertEquals("Broker Config Error (populateJMSXUserID)", false, broker.isPopulateJMSXUserID());
            assertEquals("Broker Config Error (persistent)", true, broker.isPersistent());
            LOG.info("Success");

            SystemUsage systemUsage = broker.getSystemUsage();
            assertTrue("Should have a SystemUsage", systemUsage != null);
            assertEquals("SystemUsage Config Error (StoreUsage.limit)", 1 * 1024 * 1024 * 1024, systemUsage.getStoreUsage().getLimit());
            assertEquals("SystemUsage Config Error (StoreUsage.name)", "foo", systemUsage.getStoreUsage().getName());

            assertNotNull(systemUsage.getStoreUsage().getStore());
            assertTrue(systemUsage.getStoreUsage().getStore() instanceof MultiKahaDBPersistenceAdapter);

            LOG.info("Success");

            broker.getAdminView().addQueue("A.B");

            BaseDestination queue = (BaseDestination) broker.getRegionBroker().getDestinationMap().get(new ActiveMQQueue("A.B"));
            assertTrue(queue.getSystemUsage().getStoreUsage().getStore() instanceof KahaDBPersistenceAdapter);
            assertEquals(50*1024*1024, queue.getSystemUsage().getStoreUsage().getLimit());

        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    protected static void recursiveDelete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        file.delete();
    }

    protected BrokerService createBroker(String resource) throws Exception {
        return createBroker(new ClassPathResource(resource));
    }

    protected BrokerService createBroker(Resource resource) throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();

        BrokerService broker = factory.getBroker();

        assertTrue("Should have a broker!", broker != null);

        return broker;
    }
}
