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


import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class DestinationsPluginTest {

    BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void shutdown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService createBroker() {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setPlugins(new BrokerPlugin[]{new DestinationsPlugin()});
        broker.setDataDirectory("target/test");
        return broker;
    }

    @Test
    public void testDestinationSave() throws Exception {

        BrokerView brokerView = broker.getAdminView();
        brokerView.addQueue("test-queue");

        broker.stop();
        broker.waitUntilStopped();

        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();


        ActiveMQDestination[] destinations = broker.getRegionBroker().getDestinations();
        for (ActiveMQDestination destination : destinations) {
            if (destination.isQueue()) {
                assertEquals("test-queue", destination.getPhysicalName());
            }
        }

    }

}
