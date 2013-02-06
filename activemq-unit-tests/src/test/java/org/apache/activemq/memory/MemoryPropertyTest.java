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
package org.apache.activemq.memory;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPropertyTest extends TestCase {

    private static final transient Logger LOG = LoggerFactory.getLogger(MemoryPropertyTest.class);
    BrokerService broker;


    /**
     * Sets up a test where the producer and consumer have their own connection.
     *
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        // Create broker from resource
        LOG.info("Creating broker... ");
        broker = createBroker("xbean:org/apache/activemq/memory/activemq.xml");
        LOG.info("Success");
        super.setUp();
    }

    protected BrokerService createBroker(String resource) throws Exception {
        return BrokerFactory.createBroker(resource);
    }

    /*
     * Stops the Broker
     *
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        LOG.info("Closing Broker");
        if (broker != null) {
            broker.stop();
        }
        LOG.info("Broker closed...");
    }

    public void testBrokerInitialized() {
        assertTrue("We should have a broker", broker != null);

        assertEquals("test-broker", broker.getBrokerName());
        assertEquals(1024, broker.getSystemUsage().getMemoryUsage().getLimit());
        assertEquals(34, broker.getSystemUsage().getMemoryUsage().getPercentUsageMinDelta());

        assertNotNull(broker.getSystemUsage().getStoreUsage().getStore());
        // non persistent broker so no temp storage
        assertNull(broker.getSystemUsage().getTempUsage().getStore());
    }
}
