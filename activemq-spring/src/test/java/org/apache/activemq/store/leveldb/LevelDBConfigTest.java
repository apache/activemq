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
package org.apache.activemq.store.leveldb;

import java.io.File;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 *
 */
public class LevelDBConfigTest extends TestCase {

    protected static final String CONF_ROOT = "src/test/resources/org/apache/activemq/store/leveldb/";
    private static final Logger LOG = LoggerFactory.getLogger(LevelDBConfigTest.class);

    /*
     * This tests configuring the different broker properties using
     * xbeans-spring
     */
    public void testBrokerConfig() throws Exception {
        BrokerService broker;

        // Create broker from resource
        // System.out.print("Creating broker... ");
        broker = createBroker("org/apache/activemq/store/leveldb/leveldb.xml");
        LOG.info("Success");

        try {
            // Check broker configuration
            // System.out.print("Checking broker configurations... ");
            assertEquals("Broker Config Error (brokerName)", "brokerConfigTest", broker.getBrokerName());
            assertEquals("Broker Config Error (populateJMSXUserID)", false, broker.isPopulateJMSXUserID());
            assertEquals("Broker Config Error (useLoggingForShutdownErrors)", true, broker.isUseLoggingForShutdownErrors());
            assertEquals("Broker Config Error (useJmx)", true, broker.isUseJmx());
            assertEquals("Broker Config Error (persistent)", true, broker.isPersistent());
            assertEquals("Broker Config Error (useShutdownHook)", false, broker.isUseShutdownHook());
            assertEquals("Broker Config Error (deleteAllMessagesOnStartup)", true, broker.isDeleteAllMessagesOnStartup());
            LOG.info("Success");

            // Check specific vm transport
            // System.out.print("Checking vm connector... ");
            assertEquals("Should have a specific VM Connector", "vm://javacoola", broker.getVmConnectorURI().toString());
            LOG.info("Success");


            // Check usage manager
            // System.out.print("Checking memory manager configurations... ");
            SystemUsage systemUsage = broker.getSystemUsage();
            assertTrue("Should have a SystemUsage", systemUsage != null);
            assertEquals("SystemUsage Config Error (MemoryUsage.limit)", 1024 * 1024 * 10, systemUsage.getMemoryUsage().getLimit());
            assertEquals("SystemUsage Config Error (MemoryUsage.percentUsageMinDelta)", 20, systemUsage.getMemoryUsage().getPercentUsageMinDelta());
            assertEquals("SystemUsage Config Error (TempUsage.limit)", 1024 * 1024 * 100, systemUsage.getTempUsage().getLimit());
            assertEquals("SystemUsage Config Error (StoreUsage.limit)", 1024 * 1024 * 1024, systemUsage.getStoreUsage().getLimit());
            assertEquals("SystemUsage Config Error (StoreUsage.name)", "foo", systemUsage.getStoreUsage().getName());

            assertNotNull(systemUsage.getStoreUsage().getStore());
            assertTrue(systemUsage.getStoreUsage().getStore() instanceof LevelDBPersistenceAdapter);

            LOG.info("Success");

        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * TODO: Create additional tests for forwarding bridges
     */

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

        // Broker is already started by default when using the XML file
        // broker.start();

        return broker;
    }
}
